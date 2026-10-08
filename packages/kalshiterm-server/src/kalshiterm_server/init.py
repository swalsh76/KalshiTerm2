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
    budget_gb: float = 500.0,
    force: bool = False,
) -> InitResult:
    if not out.is_dir():
        raise InitError(f"{out} is not a directory")
    if not key_id.strip():
        raise InitError("the Kalshi key id must not be empty")
    if budget_gb <= 0:
        raise InitError("the storage budget must be positive")
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
    secrets_dir = out / "secrets"
    secrets_dir.mkdir(exist_ok=True)
    secrets_dir.chmod(0o700)
    key_path = secrets_dir / "kalshi_key.pem"
    _write_private(key_path, key_file.expanduser().read_bytes())
    result.written.append(key_path)
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
