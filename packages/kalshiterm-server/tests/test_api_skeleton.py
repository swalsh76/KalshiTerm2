import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient
from kalshiterm_server.api.app import create_app
from kalshiterm_server.api.serve import UnsafeBind, check_bind, is_loopback
from kalshiterm_server.cli import app as cli
from kalshiterm_server.config import ServerSettings
from typer.testing import CliRunner

BOGUS_URL = "postgresql+asyncpg://nobody:secret-pw@127.0.0.1:9/none"


def settings(url: str = BOGUS_URL, **extra: object) -> ServerSettings:
    return ServerSettings(db_url=url, **extra)  # type: ignore[arg-type]


# ---------------------------------------------------------------- the plaintext guard


@pytest.mark.parametrize(
    ("host", "loopback"),
    [
        ("127.0.0.1", True),
        ("127.5.5.5", True),
        ("::1", True),
        ("localhost", True),
        ("0.0.0.0", False),
        ("::", False),
        ("192.168.1.20", False),
        ("mac-studio.local", False),
        ("*", False),
        ("", False),
    ],
)
def test_only_addresses_that_cannot_leave_the_machine_count_as_loopback(
    host: str, loopback: bool
) -> None:
    assert is_loopback(host) is loopback


def test_plaintext_is_allowed_on_loopback_only(tmp_path: Path) -> None:
    check_bind("127.0.0.1", None, None)
    for host in ("0.0.0.0", "192.168.1.20", "mac-studio.local"):
        with pytest.raises(UnsafeBind, match="plaintext"):
            check_bind(host, None, None)
    cert, key = tmp_path / "c.pem", tmp_path / "k.pem"
    cert.write_text("x")
    key.write_text("x")
    check_bind("0.0.0.0", cert, key)  # TLS makes a network bind acceptable


def test_a_half_configured_or_missing_certificate_is_rejected(tmp_path: Path) -> None:
    some = tmp_path / "c.pem"
    some.write_text("x")
    with pytest.raises(UnsafeBind, match="both"):
        check_bind("127.0.0.1", some, None)
    with pytest.raises(UnsafeBind, match="does not exist"):
        check_bind("0.0.0.0", some, tmp_path / "missing.pem")


def test_the_api_command_refuses_a_network_bind_without_tls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KTERM_DB_URL", BOGUS_URL)
    result = CliRunner().invoke(cli, ["api", "--host", "0.0.0.0"])
    assert result.exit_code == 1
    assert "refusing to serve plaintext HTTP" in result.output


# ---------------------------------------------------------------- the open endpoints


def test_healthz_says_only_that_the_process_is_up() -> None:
    with TestClient(create_app(settings())) as client:  # no database is contacted
        response = client.get("/healthz")
    assert response.status_code == 200 and response.json() == {"status": "ok"}


def test_readyz_is_unavailable_without_a_database_and_leaks_nothing() -> None:
    with TestClient(create_app(settings())) as client:
        response = client.get("/readyz")
    assert response.status_code == 503
    assert response.json() == {"ready": False, "reason": "database unreachable"}
    assert "secret-pw" not in response.text and "127.0.0.1" not in response.text


def test_the_api_documents_nothing_unless_asked() -> None:
    with TestClient(create_app(settings())) as client:
        for path in ("/docs", "/redoc", "/openapi.json"):
            assert client.get(path).status_code == 404, path
    with TestClient(create_app(settings(api_docs=True))) as client:
        assert client.get("/openapi.json").status_code == 200


def test_unknown_routes_and_wrong_methods_are_plain_errors() -> None:
    with TestClient(create_app(settings())) as client:
        assert client.get("/nope").status_code == 404
        assert client.post("/healthz").status_code == 405


@pytest.mark.db
def test_readyz_is_ok_on_a_migrated_database(migrated_db_url: str) -> None:
    with TestClient(create_app(settings(migrated_db_url))) as client:
        response = client.get("/readyz")
    assert response.status_code == 200 and response.json() == {"ready": True}


@pytest.mark.db
def test_readyz_is_not_ready_before_any_migration(fresh_db_url: str) -> None:
    with TestClient(create_app(settings(fresh_db_url))) as client:
        response = client.get("/readyz")
    assert response.status_code == 503
    assert response.json() == {"ready": False, "reason": "database not migrated"}


@pytest.mark.db
def test_readyz_notices_a_database_that_is_behind_the_code(migrated_db_url: str) -> None:
    import asyncio

    from kalshiterm_server import db
    from sqlalchemy import text

    async def downgrade_marker() -> None:
        engine = db.make_engine(migrated_db_url)
        async with engine.begin() as conn:
            await conn.execute(text("update alembic_version set version_num = '0001'"))
        await engine.dispose()

    asyncio.run(downgrade_marker())
    with TestClient(create_app(settings(migrated_db_url))) as client:
        response = client.get("/readyz")
    assert response.status_code == 503 and response.json()["reason"] == "database not migrated"


# ---------------------------------------------------------------- the real server


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture
def running_server() -> Iterator[tuple[int, subprocess.Popen[str]]]:
    port = free_port()
    env = {**os.environ, "KTERM_DB_URL": BOGUS_URL}
    process = subprocess.Popen(
        ["kterm-server", "api", "--port", str(port)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                if httpx.get(f"http://127.0.0.1:{port}/healthz", timeout=1).status_code == 200:
                    break
            except httpx.TransportError:
                time.sleep(0.2)
        else:
            process.kill()
            pytest.fail(
                "the API did not start: " + (process.stdout.read() if process.stdout else "")
            )
        yield port, process
    finally:
        if process.poll() is None:
            process.kill()


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")
def test_the_real_server_starts_answers_and_stops_cleanly_on_sigterm(
    running_server: tuple[int, subprocess.Popen[str]],
) -> None:
    port, process = running_server
    assert httpx.get(f"http://127.0.0.1:{port}/healthz").json() == {"status": "ok"}
    assert "server" not in httpx.get(f"http://127.0.0.1:{port}/healthz").headers
    process.terminate()
    process.wait(timeout=10)  # uvicorn finishes its shutdown, then re-raises the signal
    output = process.stdout.read() if process.stdout else ""
    assert "Shutting down" in output and "Finished server process" in output
