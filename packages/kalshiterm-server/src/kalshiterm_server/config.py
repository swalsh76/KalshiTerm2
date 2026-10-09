"""Server settings from ``KTERM_*`` environment variables."""

import tempfile

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ServerSettings(BaseSettings):
    """The database URL has no default on purpose: no address is ever hard-coded."""

    model_config = SettingsConfigDict(env_prefix="KTERM_", env_file=".env", extra="ignore")

    db_url: str  # e.g. postgresql+asyncpg://user:password@host:5432/dbname

    # Storage governor (PLAN §9.3). The budget is for Postgres data plus WAL; the production
    # value (500) is set at deployment, the default here is deliberately small.
    storage_budget_gb: float = 100.0
    # Where to measure free space and prove the disk is writable: inside the server container
    # this is the Docker VM's disk, i.e. the same disk Postgres lives on.
    disk_check_path: str = Field(default_factory=tempfile.gettempdir)
    # JSON file written by the host-side drive check (deploy/host/check-data-drive.sh); the
    # container cannot see whether the host mounted the external SSD, so it reads this instead.
    host_state_file: str | None = None

    # The API (PLAN §5.4). It binds to loopback unless a TLS certificate is given.
    api_host: str = "127.0.0.1"
    api_port: int = 8700
    api_docs: bool = False  # interactive docs and the OpenAPI document
    # Per-user watchlists: each watched market costs orderbook storage, so both are capped.
    watchlist_max_per_user: int = 50
    watchlist_max_total: int = 200  # distinct markets requested by all users together
    # Live push (/v1/stream)
    stream_max_markets: int = 100  # per subscription
    stream_max_connections_per_user: int = 5
    stream_heartbeat_seconds: float = 15.0
    stream_auth_timeout: float = 5.0  # for clients that cannot send an Authorization header
    stream_page: int = 1000  # rows read per table per pull
    # a client further behind than this many sequence numbers (~3 hours at today's rates) is
    # told to refetch over REST instead of replaying
    stream_catchup_horizon: int = 20_000_000
    auth_failure_limit: int = 10  # failed logins from one address ...
    auth_failure_window_seconds: float = 60.0  # ... within this window lock it out for a while
