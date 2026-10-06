"""Environment configuration. The only place Kalshi URLs are defined."""

from enum import StrEnum
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(StrEnum):
    DEMO = "demo"
    PRODUCTION = "production"


REST_URLS: dict[Environment, str] = {
    Environment.DEMO: "https://external-api.demo.kalshi.co/trade-api/v2",
    Environment.PRODUCTION: "https://external-api.kalshi.com/trade-api/v2",
}

WS_URLS: dict[Environment, str] = {
    Environment.DEMO: "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2",
    Environment.PRODUCTION: "wss://external-api-ws.kalshi.com/trade-api/ws/v2",
}


class KalshiSettings(BaseSettings):
    """Settings read from ``KALSHI_*`` environment variables and ``.env``.

    Demo is the default; production requires setting ``KALSHI_ENV=production``.
    """

    model_config = SettingsConfigDict(env_prefix="KALSHI_", env_file=".env", extra="ignore")

    env: Environment = Environment.DEMO
    key_id: str | None = None
    private_key_path: Path | None = Field(default=None)

    @property
    def is_production(self) -> bool:
        return self.env is Environment.PRODUCTION

    @property
    def rest_url(self) -> str:
        return REST_URLS[self.env]

    @property
    def ws_url(self) -> str:
        return WS_URLS[self.env]
