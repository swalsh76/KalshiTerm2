"""``kterm-server init``: first-time configuration for a production deployment.

Writes, into the deploy directory: ``.env`` (generated database password, settings),
``secrets/kalshi_key.pem`` (the server's own read-only Kalshi key) and a starter
``config/watchlist.toml``. Secrets are written with owner-only permissions and are never
printed. Re-running does **not** regenerate anything unless ``force`` is set: a new database
password would lock the server out of an existing database volume.
"""

import os
import secrets
from dataclasses import dataclass, field
from pathlib import Path

from kalshi_core.auth import load_private_key

from kalshiterm_server import tls

CONTAINER_KEY_PATH = "/run/secrets/kalshi_key.pem"
HOST_STATE_PATH = "/hoststate/host.json"

WATCHLIST_TEMPLATE = """\
# Which markets get their full orderbook stored. Re-read every 5 minutes; no restart needed.
#
# markets      = markets you always want (never removed automatically)
# auto_top_n   = also watch the N busiest markets of the last hour (each stays at least 12 h)
#
# Cost warning (measured 2026-10-07): the fast crypto 15-minute books alone produce over half
# of all orderbook deltas (one BTC book ~600 per second). A smaller auto_top_n is much cheaper.
[watchlist]
markets = []
auto_top_n = 20
auto_window_minutes = 60
"""


class InitError(Exception):
    """Something the operator must fix before initialisation can continue."""


@dataclass
class InitResult:
    written: list[Path] = field(default_factory=list)
    kept: list[Path] = field(default_factory=list)
    certificate: tls.CertInfo | None = None


def _write_private(path: Path, content: bytes) -> None:
    """Create ``path`` readable by its owner only, replacing any existing file atomically."""
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, content)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


def run_init(
    out: Path,
    *,
    key_id: str,
    key_file: Path,
    hosts: list[str] | None = None,
    ips: list[str] | None = None,
    budget_gb: float = 500.0,
    backup_dir: Path | None = None,
    force: bool = False,
) -> InitResult:
    if not out.is_dir():
        raise InitError(f"{out} is not a directory")
    if not key_id.strip():
        raise InitError("the Kalshi key id must not be empty")
    if budget_gb <= 0:
        raise InitError("the storage budget must be positive")
    if backup_dir is not None and (
        not backup_dir.is_absolute() or any(c in str(backup_dir) for c in "\r\n\"'$")
    ):
        raise InitError(
            "--backup-dir must be an absolute path on the HOST (the mounted NAS directory), "
            "without quotes or $"
        )
    if not hosts and not ips:
        raise InitError(
            "give --host and/or --ip: the TLS certificate is only valid for the names clients "
            "will use to reach this server (e.g. --host mac-studio --ip 192.168.1.20)"
        )
    try:
        tls.normalise_names(hosts or [], ips or [])  # fail on a bad name before writing anything
    except tls.TlsError as exc:
        raise InitError(str(exc)) from exc
    env_path = out / ".env"
    if env_path.exists() and not force:
        raise InitError(
            f"{env_path} already exists: this deployment is initialised. Re-running would "
            "generate a new database password and lock the server out of its existing data. "
            "Use --force only when starting over with an empty database volume."
        )
    load_private_key(key_file)  # raises AuthError for a missing, encrypted or non-Ed25519 key

    result = InitResult()
    password = secrets.token_urlsafe(32)
    env = f"""\
# Written by `kterm-server init`. Contains secrets: keep it private, never commit it.
COMPOSE_PROJECT_NAME=kalshiterm
POSTGRES_USER=kterm
POSTGRES_DB=kterm
POSTGRES_PASSWORD={password}
KTERM_DB_URL=postgresql+asyncpg://kterm:{password}@db:5432/kterm
KTERM_STORAGE_BUDGET_GB={budget_gb:g}
KTERM_HOST_STATE_FILE={HOST_STATE_PATH}
KALSHI_ENV=production
KALSHI_KEY_ID={key_id.strip()}
KALSHI_PRIVATE_KEY_PATH={CONTAINER_KEY_PATH}
"""
    if backup_dir is not None:
        env += f"""\
# Nightly backups to a directory on another machine (the `backup` service is in this profile)
COMPOSE_PROFILES=backup
KTERM_BACKUP_DIR={backup_dir}
KTERM_BACKUP_TARGET=/backup
"""
    secrets_dir = out / "secrets"
    secrets_dir.mkdir(exist_ok=True)
    secrets_dir.chmod(0o700)
    key_path = secrets_dir / "kalshi_key.pem"
    _write_private(key_path, key_file.expanduser().read_bytes())
    result.written.append(key_path)
    result.certificate = tls.write_certificate(secrets_dir, hosts or [], ips or [])
    result.written += [secrets_dir / tls.CERT_FILE, secrets_dir / tls.KEY_FILE]
    _write_private(env_path, env.encode())
    result.written.append(env_path)

    config_dir = out / "config"
    config_dir.mkdir(exist_ok=True)
    watchlist = config_dir / "watchlist.toml"
    if watchlist.exists():
        result.kept.append(watchlist)  # the operator's edits are never overwritten
    else:
        watchlist.write_text(WATCHLIST_TEMPLATE, encoding="utf-8")
        result.written.append(watchlist)
    (out / "state").mkdir(exist_ok=True)  # the host-side drive check writes here
    return result
