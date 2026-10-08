import os
import socket
import ssl
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest
from kalshiterm_server import tls

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="subprocess signals")

BOGUS_URL = "postgresql+asyncpg://nobody:x@127.0.0.1:9/none"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@contextmanager
def serve(directory: Path) -> Iterator[int]:
    """The real `kterm-server api`, serving HTTPS with the certificate in ``directory``."""
    port = free_port()
    process = subprocess.Popen(
        [
            "kterm-server",
            "api",
            "--port",
            str(port),
            "--tls-cert",
            str(directory / tls.CERT_FILE),
            "--tls-key",
            str(directory / tls.KEY_FILE),
        ],  # fmt: skip
        env={**os.environ, "KTERM_DB_URL": BOGUS_URL},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        trust = ssl.create_default_context(cafile=str(directory / tls.CERT_FILE))
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                if httpx.get(
                    f"https://127.0.0.1:{port}/healthz", verify=trust, timeout=1
                ).is_success:
                    break
            except httpx.TransportError:
                time.sleep(0.2)
        else:
            process.kill()
            pytest.fail(
                "the API did not start: " + (process.stdout.read() if process.stdout else "")
            )
        yield port
    finally:
        process.terminate()
        process.wait(timeout=10)


@pytest.fixture
def issued(tmp_path: Path) -> Path:
    tls.write_certificate(tmp_path, ["mac-studio"], [])
    return tmp_path


def trusting(directory: Path) -> ssl.SSLContext:
    """A client that trusts exactly one certificate and nothing else (certificate pinning)."""
    return ssl.create_default_context(cafile=str(directory / tls.CERT_FILE))


def test_a_normal_client_refuses_a_self_signed_certificate(issued: Path) -> None:
    with (
        serve(issued) as port,
        pytest.raises(httpx.ConnectError, match="CERTIFICATE_VERIFY_FAILED"),
    ):
        httpx.get(f"https://127.0.0.1:{port}/healthz")


def test_a_client_that_pins_the_certificate_connects_and_gets_the_api(issued: Path) -> None:
    with serve(issued) as port:
        pinned = httpx.get(f"https://127.0.0.1:{port}/healthz", verify=trusting(issued))
        assert pinned.status_code == 200 and pinned.json() == {"status": "ok"}
        assert "server" not in pinned.headers  # no software banner over TLS either
        unauth = httpx.get(f"https://127.0.0.1:{port}/v1/me", verify=trusting(issued))
        assert unauth.status_code == 401  # the application is behind the same socket


def test_the_fingerprint_a_client_sees_is_the_one_the_operator_was_shown(issued: Path) -> None:
    expected = tls.describe((issued / tls.CERT_FILE).read_bytes()).fingerprint
    with serve(issued) as port:
        presented = ssl.get_server_certificate(("127.0.0.1", port))
    assert tls.describe(presented.encode()).fingerprint == expected


def test_a_certificate_for_other_names_is_rejected_even_if_it_is_trusted(issued: Path) -> None:
    with (
        serve(issued) as port,
        socket.create_connection(("127.0.0.1", port)) as raw,
        pytest.raises(ssl.SSLCertVerificationError, match="Hostname mismatch|hostname"),
    ):
        trusting(issued).wrap_socket(raw, server_hostname="evil.example")


def test_plain_http_to_the_tls_port_gets_nothing_useful(issued: Path) -> None:
    with serve(issued) as port, pytest.raises(httpx.HTTPError):
        httpx.get(f"http://127.0.0.1:{port}/healthz", timeout=3)


@pytest.mark.filterwarnings("ignore::DeprecationWarning")  # the old constants are the point
def test_obsolete_tls_versions_are_refused(issued: Path) -> None:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    try:
        context.minimum_version = ssl.TLSVersion.TLSv1
        context.maximum_version = ssl.TLSVersion.TLSv1_1
    except (ValueError, ssl.SSLError):
        pytest.skip("this OpenSSL build cannot even offer TLS 1.1")
    with (
        serve(issued) as port,
        socket.create_connection(("127.0.0.1", port)) as raw,
        pytest.raises(ssl.SSLError),
    ):
        context.wrap_socket(raw, server_hostname="127.0.0.1")


def test_after_rotation_the_old_pin_fails_and_the_new_one_works(tmp_path: Path) -> None:
    old_dir, new_dir = tmp_path / "old", tmp_path / "new"
    old_dir.mkdir()
    new_dir.mkdir()
    old = tls.write_certificate(old_dir, ["mac-studio"], [])
    new = tls.write_certificate(new_dir, ["mac-studio"], [])
    assert old.fingerprint != new.fingerprint
    with serve(new_dir) as port:  # the server now presents the new certificate
        with pytest.raises(httpx.ConnectError, match="CERTIFICATE_VERIFY_FAILED"):
            httpx.get(f"https://127.0.0.1:{port}/healthz", verify=trusting(old_dir))  # old pin
        assert httpx.get(f"https://127.0.0.1:{port}/healthz", verify=trusting(new_dir)).is_success
