"""Server settings from ``KTERM_*`` environment variables."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class ServerSettings(BaseSettings):
    """The database URL has no default on purpose: no address is ever hard-coded."""

    model_config = SettingsConfigDict(env_prefix="KTERM_", env_file=".env", extra="ignore")

    db_url: str  # e.g. postgresql+asyncpg://user:password@host:5432/dbname
