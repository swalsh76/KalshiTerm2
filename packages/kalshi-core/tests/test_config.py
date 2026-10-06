from pathlib import Path

import pytest
from kalshi_core.config import Environment, KalshiSettings
from pydantic import ValidationError


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)  # no .env here, so the repo's real .env is never read
    for name in ("KALSHI_ENV", "KALSHI_KEY_ID", "KALSHI_PRIVATE_KEY_PATH"):
        monkeypatch.delenv(name, raising=False)


def make() -> KalshiSettings:
    return KalshiSettings()


def test_defaults_to_demo() -> None:
    s = make()
    assert s.env is Environment.DEMO
    assert not s.is_production
    assert "demo" in s.rest_url
    assert "demo" in s.ws_url
    assert s.key_id is None


def test_production_requires_explicit_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KALSHI_ENV", "production")
    s = make()
    assert s.is_production
    assert s.rest_url == "https://external-api.kalshi.com/trade-api/v2"
    assert s.ws_url == "wss://external-api-ws.kalshi.com/trade-api/ws/v2"


def test_rejects_unknown_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KALSHI_ENV", "staging")
    with pytest.raises(ValidationError):
        make()


def test_credentials_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KALSHI_KEY_ID", "abc")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PATH", "/tmp/k.pem")
    s = make()
    assert s.key_id == "abc"
    assert s.private_key_path == Path("/tmp/k.pem")
