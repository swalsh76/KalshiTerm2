from collections.abc import Iterator

import pytest
from fake_server import FakeServer, make_certificate
from kalshiterm_client import secrets
from kalshiterm_client.cli import app
from kalshiterm_client.pinning import (
    describe,
    fingerprint_of_pem,
    normalise_fingerprint,
)
from kalshiterm_client.profiles import ProfileError, ProfileStore, pin_path
from typer.testing import CliRunner

TOKEN = "kt_7_" + "Zy9" * 14
runner = CliRunner()


def run(*args: str, input: str | None = None) -> tuple[int, str]:
    result = runner.invoke(app, list(args), input=input)
    return result.exit_code, result.output


@pytest.fixture
def servers() -> Iterator[list[FakeServer]]:
    started: list[FakeServer] = []
    yield started
    for server in started:
        server.stop()


def serve(servers: list[FakeServer], **kw: object) -> FakeServer:
    server = FakeServer(**kw).start()  # type: ignore[arg-type]
    servers.append(server)
    return server


def add_profile(server: FakeServer, name: str = "lab", token: bool = True) -> None:
    code, out = run(
        "config", "add", name, "--url", server.url,
        *(["--token-stdin"] if token else ["--no-token"]),
        input=(TOKEN + "\n") if token else None,
    )  # fmt: skip
    assert code == 0, out


def fingerprint(server: FakeServer) -> str:
    assert server.cert
    return fingerprint_of_pem(server.cert[0])


def trusted(servers: list[FakeServer], **kw: object) -> FakeServer:
    server = serve(servers, cert=make_certificate(), **kw)
    add_profile(server)
    code, out = run("server", "trust", "--fingerprint", fingerprint(server))
    assert code == 0, out
    return server


def test_fingerprints_are_accepted_in_any_spelling() -> None:
    plain = "ab" * 32
    spelled = ":".join(["AB"] * 32)
    assert normalise_fingerprint(plain) == normalise_fingerprint(spelled) == spelled
    for bad in ("", "abc", "zz" * 32, "ab" * 31):
        with pytest.raises(ProfileError):
            normalise_fingerprint(bad)


def test_trusting_with_the_right_fingerprint_pins_the_certificate_and_status_works(
    servers: list[FakeServer],
) -> None:
    server = trusted(servers)
    profile = ProfileStore().get("lab")
    assert profile.fingerprint == fingerprint(server)
    assert pin_path("lab").read_bytes() == server.cert[0]  # type: ignore[index]
    code, out = run("server", "status")
    assert code == 0 and "All checks passed." in out
    assert server.requests == [("/v1/status", f"Bearer {TOKEN}")]


def test_a_wrong_fingerprint_pins_nothing(servers: list[FakeServer]) -> None:
    server = serve(servers, cert=make_certificate())
    add_profile(server)
    code, out = run("server", "trust", "--fingerprint", "00" * 32)
    assert code == 1 and "not 00:00" in out and "Nothing pinned" in out
    assert ProfileStore().get("lab").fingerprint is None and not pin_path("lab").exists()


def test_interactive_trust_shows_the_certificate_and_defaults_to_no(
    servers: list[FakeServer],
) -> None:
    server = serve(servers, cert=make_certificate())
    add_profile(server)
    code, out = run("server", "trust", input="\n")  # just Enter
    assert code == 1 and "not trusted" in out
    assert fingerprint(server) in out and "127.0.0.1" in out and "localhost" in out
    assert ProfileStore().get("lab").fingerprint is None
    code, out = run("server", "trust", input="y\n")
    assert code == 0 and ProfileStore().get("lab").fingerprint == fingerprint(server)


def test_trusting_again_when_nothing_changed_just_says_so(servers: list[FakeServer]) -> None:
    trusted(servers)
    code, out = run("server", "trust")
    assert code == 0 and "already trusts this certificate" in out


def test_an_untrusted_https_server_is_never_sent_the_token(servers: list[FakeServer]) -> None:
    server = serve(servers, cert=make_certificate())
    add_profile(server)
    code, out = run("server", "status")
    assert code == 1 and "no trusted certificate yet" in out and "server trust" in out
    assert server.requests == []


def test_a_changed_certificate_is_refused_before_anything_is_sent_and_needs_a_deliberate_yes(
    servers: list[FakeServer],
) -> None:
    old = trusted(servers)
    old.stop()
    new = serve(servers, port=old.port, cert=make_certificate())  # same address, new key
    code, out = run("server", "status")
    assert code == 1 and "HAS CHANGED" in out and "Nothing was sent" in out
    assert fingerprint(old) in out and fingerprint(new) in out
    assert "impersonating" in out
    assert new.requests == []  # the token never reached it

    code, out = run("server", "trust", input="\n")  # the default answer is No
    assert code == 1 and "WARNING" in out and fingerprint(old) in out
    assert ProfileStore().get("lab").fingerprint == fingerprint(old)

    assert run("server", "trust", input="y\n")[0] == 0
    code, out = run("server", "status")
    assert code == 0 and new.requests == [("/v1/status", f"Bearer {TOKEN}")]


def test_a_tampered_or_missing_pin_file_is_refused(servers: list[FakeServer]) -> None:
    server = trusted(servers)
    other, _ = make_certificate()
    pin_path("lab").write_bytes(other)
    code, out = run("server", "status")
    assert code == 1 and "altered" in out
    pin_path("lab").unlink()
    code, out = run("server", "status")
    assert code == 1 and "missing" in out
    assert server.requests == []


def test_an_expired_certificate_is_reported_as_expired_not_as_changed(
    servers: list[FakeServer],
) -> None:
    server = serve(servers, cert=make_certificate(expired=True))
    add_profile(server)
    assert run("server", "trust", "--fingerprint", fingerprint(server))[0] == 0
    code, out = run("server", "status")
    assert code == 1 and "no longer acceptable" in out and "expired" in out
    assert "CHANGED" not in out and server.requests == []


def test_a_name_the_certificate_does_not_cover_is_a_name_problem_not_a_change(
    servers: list[FakeServer],
) -> None:
    server = serve(servers, cert=make_certificate(ips=("10.9.9.9",), dns=("other.example",)))
    add_profile(server)
    assert run("server", "trust", "--fingerprint", fingerprint(server))[0] == 0
    code, out = run("server", "status")
    assert code == 1 and "no longer acceptable" in out and "CHANGED" not in out
    assert server.requests == []


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (401, "rejected the token"),
        (403, "needs an admin token"),
        (429, "retry in 42 s"),
        (500, "answered 500"),
    ],
)
def test_server_refusals_are_explained(
    servers: list[FakeServer], status: int, expected: str
) -> None:
    server = trusted(servers)
    server.status = status
    code, out = run("server", "status")
    assert code == 1 and expected in out and TOKEN not in out


def test_a_missing_token_is_explained_and_nothing_is_sent(servers: list[FakeServer]) -> None:
    server = serve(servers, cert=make_certificate())
    add_profile(server, token=False)
    run("server", "trust", "--fingerprint", fingerprint(server))
    code, out = run("server", "status")
    assert code == 1 and "no token for lab" in out and server.requests == []


def test_an_unreachable_server_is_reported(servers: list[FakeServer]) -> None:
    server = trusted(servers)
    server.stop()
    code, out = run("server", "status")
    assert code == 1 and "cannot reach" in out


def test_plain_http_to_this_machine_needs_no_pin_and_has_no_certificate_to_trust(
    servers: list[FakeServer],
) -> None:
    server = serve(servers)  # plain http on loopback
    add_profile(server)
    code, out = run("server", "status")
    assert code == 0 and server.requests == [("/v1/status", f"Bearer {TOKEN}")]
    code, out = run("server", "trust")
    assert code == 1 and "no certificate to trust" in out


def test_problems_make_status_exit_nonzero_and_json_is_raw(servers: list[FakeServer]) -> None:
    server = trusted(servers)
    server.body = {
        "time": "t",
        "problems": ["storage at 91% of budget"],
        "storage": {"used_bytes": 91 * 1024**3, "budget_bytes": 100 * 1024**3, "percent": 91,
                    "mode": "shedding"},
        "streams": {"trades": {"age_seconds": 3, "per_second_5min": 12.5}},
        "backup": {"configured": True, "last_ok": {"age_seconds": 7200, "size_bytes": 5 * 1024**2}},
    }  # fmt: skip
    code, out = run("server", "status")
    assert code == 1 and "NEEDS ATTENTION (1)" in out and "storage at 91% of budget" in out
    assert "91.0 GB of 100.0 GB" in out and "shedding" in out
    assert "trades" in out and "12.5/s" in out and "last good 2h ago, 5.0 MB" in out
    code, out = run("server", "status", "--json")
    assert '"percent": 91' in out


def test_forgetting_a_server_removes_its_pin_and_changing_its_address_drops_it(
    servers: list[FakeServer],
) -> None:
    server = trusted(servers)
    assert pin_path("lab").exists()
    same = run("config", "add", "lab", "--url", server.url, "--no-token", "--replace")
    assert same[0] == 0 and pin_path("lab").exists()  # same address keeps the decision
    assert ProfileStore().get("lab").fingerprint == fingerprint(server)
    run("config", "add", "lab", "--url", "https://elsewhere:8700", "--no-token", "--replace")
    assert not pin_path("lab").exists() and ProfileStore().get("lab").fingerprint is None
    run("config", "remove", "lab")
    assert secrets.get_token("lab") is None


def test_describe_lists_names_and_validity() -> None:
    pem, _ = make_certificate(ips=("10.0.0.5",), dns=("studio", "studio.local"))
    details = describe(pem)
    assert details.dns_names == ["studio", "studio.local"] and details.ip_addresses == ["10.0.0.5"]
    assert details.not_after > details.not_before
    assert details.fingerprint == fingerprint_of_pem(pem) and details.fingerprint.count(":") == 31


def test_show_reports_the_pin_or_that_there_is_none(servers: list[FakeServer]) -> None:
    server = serve(servers, cert=make_certificate())
    add_profile(server)
    assert "pin:   NOT YET" in run("config", "show", "lab")[1]
    run("server", "trust", "--fingerprint", fingerprint(server))
    assert f"pin:   {fingerprint(server)}" in run("config", "show", "lab")[1]
