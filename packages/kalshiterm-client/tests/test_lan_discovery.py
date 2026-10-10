import socket
import uuid
from pathlib import Path

import pytest
from fake_server import FakeServer, make_certificate
from kalshiterm_client import discovery
from kalshiterm_client.cli import app
from kalshiterm_client.discovery import SERVICE_TYPE, Found, from_info, matching
from kalshiterm_client.pinning import fingerprint_of_pem
from kalshiterm_client.profiles import ProfileStore, pin_path
from typer.testing import CliRunner
from zeroconf import ServiceInfo, Zeroconf

FP = ":".join(["AB"] * 32)
TOKEN = "kt_7_" + "Zy9" * 14
runner = CliRunner()


def info(**properties: bytes | None) -> ServiceInfo:
    return ServiceInfo(
        SERVICE_TYPE,
        f"KalshiTerm mac-studio.{SERVICE_TYPE}",
        addresses=[socket.inet_aton("192.168.1.20")],
        port=8700,
        properties=properties,
        server="mac-studio.local.",
    )


def test_an_advertisement_is_read_into_a_server_description() -> None:
    found = from_info(info(v=b"1", fp=b"ab" * 32))
    assert found == Found("KalshiTerm mac-studio", "mac-studio.local", ("192.168.1.20",), 8700, FP)
    assert found.url == "https://mac-studio.local:8700"


@pytest.mark.parametrize("bad", [None, b"", b"not-hex", b"ab" * 31, b"zz" * 32])
def test_a_missing_or_malformed_fingerprint_is_ignored_not_trusted(bad: bytes | None) -> None:
    assert from_info(info(fp=bad)).fingerprint is None


def test_an_address_is_matched_by_name_short_name_or_ip_and_port() -> None:
    server = from_info(info(fp=b"ab" * 32))
    for url in (
        "https://mac-studio.local:8700",
        "https://MAC-STUDIO:8700",
        "https://192.168.1.20:8700",
    ):
        assert matching(url, [server]) == server
    for url in ("https://mac-studio:8701", "https://other:8700", "https://192.168.1.21:8700"):
        assert matching(url, [server]) is None
    assert matching("https://mac-studio:8700", []) is None


def test_the_service_name_agrees_with_what_the_host_script_publishes() -> None:
    script = Path(__file__).parents[3] / "deploy" / "host" / "advertise.sh"
    assert SERVICE_TYPE.removesuffix(".local.") in script.read_text()


def test_real_multicast_round_trip() -> None:
    """Publish with zeroconf on this machine and find it again; skipped where multicast does
    not work (some CI runners), because that says nothing about the code."""
    publisher = Zeroconf(interfaces=["127.0.0.1"])
    advertised = ServiceInfo(
        SERVICE_TYPE,
        f"KalshiTerm test-{uuid.uuid4().hex[:8]}.{SERVICE_TYPE}",
        addresses=[socket.inet_aton("127.0.0.1")],
        port=8700,
        properties={b"v": b"1", b"fp": b"cd" * 32},
        server="kterm-test.local.",
    )
    try:
        publisher.register_service(advertised)
        found = [s for s in discovery.browse(3.0, ["127.0.0.1"]) if s.host == "kterm-test.local"]
    finally:
        publisher.unregister_all_services()
        publisher.close()
    if not found:
        pytest.skip("multicast is not available here")
    assert found[0].fingerprint == ":".join(["CD"] * 32) and found[0].port == 8700


def _found(fp: str | None, server: FakeServer) -> Found:
    return Found("KalshiTerm test", "127.0.0.1", ("127.0.0.1",), server.port, fp)


@pytest.fixture
def server() -> object:
    s = FakeServer(cert=make_certificate()).start()
    yield s
    s.stop()


def setup_profile(server: FakeServer) -> str:
    result = runner.invoke(
        app, ["config", "add", "lab", "--url", server.url, "--token-stdin"], input=TOKEN + "\n"
    )
    assert result.exit_code == 0, result.output
    assert server.cert
    return fingerprint_of_pem(server.cert[0])


def test_discover_lists_servers_with_a_hint_and_a_caveat(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        discovery,
        "browse",
        lambda timeout=3.0: [Found("KalshiTerm studio", "studio.local", ("10.0.0.5",), 8700, FP)],
    )
    result = runner.invoke(app, ["server", "discover"])
    assert result.exit_code == 0
    out = result.output
    assert "KalshiTerm studio" in out and "https://studio.local:8700" in out and "10.0.0.5" in out
    assert FP[:23] in out and "kterm config add NAME --url https://studio.local:8700" in out
    assert "anyone on the network" in out


def test_discover_with_nothing_found_says_how_to_add_by_hand(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(discovery, "browse", lambda timeout=3.0: [])
    result = runner.invoke(app, ["server", "discover"])
    assert result.exit_code == 0 and "add one by hand" in result.output


def test_discover_reports_a_network_that_cannot_multicast(monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(timeout: float = 3.0) -> list[Found]:
        raise OSError("no route")

    monkeypatch.setattr(discovery, "browse", broken)
    result = runner.invoke(app, ["server", "discover"])
    assert result.exit_code == 1 and "cannot listen" in result.output


def test_trust_notes_when_the_advertisement_agrees(
    server: FakeServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    fp = setup_profile(server)
    monkeypatch.setattr(discovery, "browse", lambda timeout=3.0: [_found(fp, server)])
    result = runner.invoke(app, ["server", "trust"], input="y\n")
    assert result.exit_code == 0 and "advertisement carries the same fingerprint" in result.output
    assert ProfileStore().get("lab").fingerprint == fp


def test_trust_refuses_when_the_advertisement_differs_and_nothing_is_pinned(
    server: FakeServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup_profile(server)
    monkeypatch.setattr(discovery, "browse", lambda timeout=3.0: [_found(FP, server)])
    result = runner.invoke(app, ["server", "trust"], input="y\n")  # even a yes is not asked for
    assert result.exit_code == 1 and "ADVERTISES" in result.output and FP in result.output
    assert ProfileStore().get("lab").fingerprint is None and not pin_path("lab").exists()


def test_an_explicit_matching_fingerprint_overrides_a_stale_advertisement(
    server: FakeServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    fp = setup_profile(server)
    monkeypatch.setattr(discovery, "browse", lambda timeout=3.0: [_found(FP, server)])
    result = runner.invoke(app, ["server", "trust", "--fingerprint", fp])
    assert result.exit_code == 0 and "WARNING" in result.output
    assert ProfileStore().get("lab").fingerprint == fp


def test_no_advertisement_or_a_broken_multicast_never_blocks_pinning(
    server: FakeServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    fp = setup_profile(server)

    def broken(timeout: float = 3.0) -> list[Found]:
        raise OSError("no multicast")

    monkeypatch.setattr(discovery, "browse", broken)
    assert runner.invoke(app, ["server", "trust"], input="y\n").exit_code == 0
    assert ProfileStore().get("lab").fingerprint == fp


def test_no_mdns_skips_the_lookup_entirely(
    server: FakeServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup_profile(server)
    calls: list[float] = []

    def spy(timeout: float = 3.0) -> list[Found]:
        calls.append(timeout)
        return []

    monkeypatch.setattr(discovery, "browse", spy)
    assert runner.invoke(app, ["server", "trust", "--no-mdns"], input="y\n").exit_code == 0
    assert calls == []
