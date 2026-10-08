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
