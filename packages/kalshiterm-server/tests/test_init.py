import stat
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from dotenv import dotenv_values
from kalshi_core.config import Environment, KalshiSettings
from kalshiterm_server.cli import app
from kalshiterm_server.config import ServerSettings
from kalshiterm_server.ingest.watchlist import load_config
from typer.testing import CliRunner

KEY_ID = "11111111-2222-3333-4444-555555555555"
posix_only = pytest.mark.skipif(
    __import__("sys").platform == "win32", reason="POSIX file permissions"
)


def pem(key: Ed25519PrivateKey | rsa.RSAPrivateKey) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


@pytest.fixture
def key_file(tmp_path: Path) -> Path:
    path = tmp_path / "input" / "server.pem"
    path.parent.mkdir()
    path.write_bytes(pem(Ed25519PrivateKey.generate()))
    return path


@pytest.fixture
def deploy(tmp_path: Path) -> Path:
    out = tmp_path / "deploy"
    out.mkdir()
    return out


def init(deploy: Path, key_file: Path, *extra: str, names: bool = True) -> object:
    args = ["init", "--out", str(deploy), "--key-id", KEY_ID, "--key-file", str(key_file), *extra]
    if names:
        args += ["--host", "mac-studio", "--ip", "192.168.1.20"]
    return CliRunner().invoke(app, args)


def mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


@posix_only
def test_init_writes_private_files_and_a_starter_watchlist(deploy: Path, key_file: Path) -> None:
    result = init(deploy, key_file)
    assert result.exit_code == 0, result.output  # type: ignore[attr-defined]
    assert mode(deploy / ".env") == 0o600
    assert mode(deploy / "secrets" / "kalshi_key.pem") == 0o600
    assert mode(deploy / "secrets") == 0o700
    assert (deploy / "secrets" / "kalshi_key.pem").read_bytes() == key_file.read_bytes()
    assert (deploy / "state").is_dir() and (deploy / "config" / "watchlist.toml").is_file()
    assert load_config(deploy / "config" / "watchlist.toml").auto_top_n == 20


def test_the_written_env_configures_both_the_server_and_the_kalshi_client(
    deploy: Path, key_file: Path
) -> None:
    init(deploy, key_file, "--budget-gb", "450")
    env = deploy / ".env"
    values = dotenv_values(env)
    password = values["POSTGRES_PASSWORD"]
    assert password and len(password) >= 40
    assert values["KTERM_DB_URL"] == f"postgresql+asyncpg://kterm:{password}@db:5432/kterm"

    server = ServerSettings(_env_file=env)  # type: ignore[call-arg]
    assert server.db_url == values["KTERM_DB_URL"] and server.storage_budget_gb == 450
    assert server.host_state_file == "/hoststate/host.json"
    kalshi = KalshiSettings(_env_file=env)  # type: ignore[call-arg]
    assert kalshi.env is Environment.PRODUCTION  # a server deployment reads real data
    assert kalshi.key_id == KEY_ID
    assert kalshi.private_key_path is not None
    assert (
        kalshi.private_key_path.as_posix() == "/run/secrets/kalshi_key.pem"
    )  # a path in the container


def test_init_never_prints_a_secret(deploy: Path, key_file: Path) -> None:
    result = init(deploy, key_file)
    password = dotenv_values(deploy / ".env")["POSTGRES_PASSWORD"]
    body = key_file.read_bytes().decode()
    output = result.output  # type: ignore[attr-defined]
    assert password and password not in output
    assert "PRIVATE KEY" not in output and body.splitlines()[1] not in output
    assert "wrote" in output and "not shown" in output


def test_a_second_run_refuses_to_replace_the_database_password(
    deploy: Path, key_file: Path
) -> None:
    init(deploy, key_file)
    before = (deploy / ".env").read_text()
    result = init(deploy, key_file)
    assert result.exit_code == 1  # type: ignore[attr-defined]
    assert "already exists" in result.output and "lock the server out" in result.output  # type: ignore[attr-defined]
    assert (deploy / ".env").read_text() == before


def test_force_starts_over_with_a_new_password_but_keeps_the_operators_watchlist(
    deploy: Path, key_file: Path
) -> None:
    init(deploy, key_file)
    first = dotenv_values(deploy / ".env")["POSTGRES_PASSWORD"]
    watchlist = deploy / "config" / "watchlist.toml"
    watchlist.write_text('[watchlist]\nmarkets = ["KXA-E1-X"]\n')

    result = init(deploy, key_file, "--force")
    assert result.exit_code == 0  # type: ignore[attr-defined]
    assert dotenv_values(deploy / ".env")["POSTGRES_PASSWORD"] != first
    assert watchlist.read_text() == '[watchlist]\nmarkets = ["KXA-E1-X"]\n'
    assert "kept" in result.output  # type: ignore[attr-defined]


def test_an_unusable_key_is_rejected_before_anything_is_written(
    deploy: Path, key_file: Path, tmp_path: Path
) -> None:
    rsa_key = tmp_path / "rsa.pem"
    rsa_key.write_bytes(pem(rsa.generate_private_key(public_exponent=65537, key_size=2048)))
    garbage = tmp_path / "garbage.pem"
    garbage.write_text("not a key")
    for bad in (rsa_key, garbage):
        result = init(deploy, bad)
        assert result.exit_code == 1, bad  # type: ignore[attr-defined]
        assert "error:" in result.output  # type: ignore[attr-defined]
    assert list(deploy.iterdir()) == []  # no .env, no secrets directory


def test_bad_arguments_are_reported_not_crashed(
    deploy: Path, key_file: Path, tmp_path: Path
) -> None:
    runner = CliRunner()
    base = ["init", "--key-file", str(key_file)]
    assert (
        runner.invoke(app, [*base, "--out", str(tmp_path / "nope"), "--key-id", KEY_ID]).exit_code
        == 1
    )
    assert runner.invoke(app, [*base, "--out", str(deploy), "--key-id", "  "]).exit_code == 1
    result = runner.invoke(
        app, [*base, "--out", str(deploy), "--key-id", KEY_ID, "--budget-gb", "0"]
    )
    assert result.exit_code == 1
    assert (
        runner.invoke(
            app, ["init", "--key-id", KEY_ID, "--key-file", str(tmp_path / "missing")]
        ).exit_code
        == 2
    )


def test_init_needs_to_know_the_names_clients_will_use_and_writes_nothing_without_them(
    deploy: Path, key_file: Path
) -> None:
    result = init(deploy, key_file, names=False)
    assert result.exit_code == 1 and "--host" in result.output  # type: ignore[attr-defined]
    assert list(deploy.iterdir()) == []


@pytest.mark.parametrize(
    "extra", [["--host", "bad name"], ["--ip", "999.1.1.1"], ["--ip", "0.0.0.0"]]
)
def test_a_bad_certificate_name_stops_init_before_anything_is_written(
    deploy: Path, key_file: Path, extra: list[str]
) -> None:
    result = init(deploy, key_file, *extra)
    assert result.exit_code == 1 and "error:" in result.output  # type: ignore[attr-defined]
    assert list(deploy.iterdir()) == []


@posix_only
def test_init_issues_a_certificate_for_those_names_and_prints_its_fingerprint(
    deploy: Path, key_file: Path
) -> None:
    from kalshiterm_server import tls

    result = init(deploy, key_file)
    info = tls.describe((deploy / "secrets" / "tls_cert.pem").read_bytes())
    assert f"fingerprint (SHA-256): {info.fingerprint}" in result.output  # type: ignore[attr-defined]
    assert {"mac-studio", "mac-studio.local", "localhost"} <= set(info.dns_names)
    assert {"192.168.1.20", "127.0.0.1"} <= set(info.ip_addresses)
    assert mode(deploy / "secrets" / "tls_key.pem") == 0o600  # the key is private
    assert mode(deploy / "secrets" / "tls_cert.pem") == 0o644  # the certificate is not secret


@posix_only
def test_init_can_switch_on_nightly_backups(deploy: Path, key_file: Path) -> None:
    result = init(deploy, key_file, "--backup-dir", "/Volumes/nas/kalshiterm")
    assert result.exit_code == 0, result.output  # type: ignore[attr-defined]
    env = dotenv_values(deploy / ".env")
    assert env["COMPOSE_PROFILES"] == "backup"
    assert env["KTERM_BACKUP_DIR"] == "/Volumes/nas/kalshiterm"
    assert env["KTERM_BACKUP_TARGET"] == "/backup"


@posix_only
def test_without_a_backup_dir_the_backup_service_stays_off(deploy: Path, key_file: Path) -> None:
    init(deploy, key_file)
    env = dotenv_values(deploy / ".env")
    assert "COMPOSE_PROFILES" not in env and "KTERM_BACKUP_TARGET" not in env


@pytest.mark.parametrize("bad", ["relative/dir", "/nas/$HOME", '/nas/"x"'])
def test_a_backup_dir_must_be_a_plain_absolute_host_path(
    deploy: Path, key_file: Path, bad: str
) -> None:
    result = init(deploy, key_file, "--backup-dir", bad)
    assert result.exit_code == 1 and "--backup-dir" in result.output  # type: ignore[attr-defined]
    assert list(deploy.iterdir()) == []
