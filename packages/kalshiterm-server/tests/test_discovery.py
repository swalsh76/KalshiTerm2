from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import respx
from kalshi_core.config import KalshiSettings
from kalshi_core.rest import KalshiAPIError, KalshiRestClient
from kalshiterm_server import db
from kalshiterm_server.ingest.discovery import FULL_KEY, UPDATED_KEY, discover
from kalshiterm_server.storage import reference
from sqlalchemy import text

pytestmark = pytest.mark.db

BASE = "https://external-api.demo.kalshi.co/trade-api/v2"
T0 = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)


def epoch(moment: datetime) -> str:
    return str(int(moment.timestamp()))


def mkt(ticker: str, status: str = "active", **extra: Any) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "event_ticker": ticker.rsplit("-", 1)[0],
        "market_type": "binary",
        "status": status,
        **extra,
    }


def evt(ticker: str) -> dict[str, Any]:
    return {
        "event_ticker": ticker,
        "series_ticker": ticker.split("-")[0],
        "title": f"Event {ticker}",
        "sub_title": "",
        "mutually_exclusive": False,
    }


class FakeKalshi:
    """Serves /series, /events and /markets with 2-item pages and records every request.

    A request with ``min_updated_ts`` and no ``status`` is an incremental poll and is answered
    from ``updated_*``; a request with ``status`` is answered from ``events`` / ``markets``.
    """

    def __init__(self) -> None:
        self.series: list[dict[str, Any]] = [
            {"ticker": "KXA", "title": "A", "frequency": "daily", "category": "x"},
            {"ticker": "KXB", "title": "B", "frequency": "weekly", "category": "y"},
        ]
        self.series[0]["fee_multiplier"] = 1
        self.series[1]["fee_multiplier"] = 0.07
        self.events: dict[str, list[dict[str, Any]]] = {
            "unopened": [evt("KXA-E0")],
            "open": [evt("KXA-E1"), evt("KXA-E2"), evt("KXB-E3")],  # spans two pages
            "closed": [],
        }
        settled = mkt("KXA-E1-Z", "finalized", result="yes", settlement_value_dollars="1.0000")
        self.markets: dict[str, list[dict[str, Any]]] = {
            "unopened": [mkt("KXA-E0-X", "initialized")],
            "open": [mkt("KXA-E1-X"), mkt("KXA-E2-X"), mkt("KXB-E3-X")],
            "closed": [mkt("KXA-E1-Y", "closed")],
            "settled": [settled],
        }
        self.updated_events: list[dict[str, Any]] = []
        self.updated_markets: list[dict[str, Any]] = []
        self.requests: list[dict[str, str]] = []
        self.fail_incremental = False

    @staticmethod
    def _page(items: list[dict[str, Any]], key: str, params: httpx.QueryParams) -> httpx.Response:
        start = int(params.get("cursor") or 0)
        nxt = start + 2
        cursor = str(nxt) if nxt < len(items) else ""
        return httpx.Response(200, json={key: items[start:nxt], "cursor": cursor})

    def _record(self, request: httpx.Request) -> httpx.QueryParams:
        self.requests.append({"path": request.url.path, **dict(request.url.params)})
        return request.url.params

    def _series(self, request: httpx.Request) -> httpx.Response:
        self._record(request)
        return httpx.Response(200, json={"series": self.series})

    def _events(self, request: httpx.Request) -> httpx.Response:
        params = self._record(request)
        if "status" not in params:
            return self._respond_incremental(self.updated_events, "events", params)
        return self._page(self.events[params["status"]], "events", params)

    def _markets(self, request: httpx.Request) -> httpx.Response:
        params = self._record(request)
        if "status" not in params:
            return self._respond_incremental(self.updated_markets, "markets", params)
        return self._page(self.markets[params["status"]], "markets", params)

    def _respond_incremental(
        self, items: list[dict[str, Any]], key: str, params: httpx.QueryParams
    ) -> httpx.Response:
        assert "min_updated_ts" in params
        if self.fail_incremental:
            return httpx.Response(500, text="boom")
        return self._page(items, key, params)

    def install(self, router: respx.MockRouter) -> None:
        router.get(f"{BASE}/series").mock(side_effect=self._series)
        router.get(f"{BASE}/events").mock(side_effect=self._events)
        router.get(f"{BASE}/markets").mock(side_effect=self._markets)

    def calls(self, suffix: str) -> list[dict[str, str]]:
        return [r for r in self.requests if r["path"].endswith(suffix)]


def rest_client() -> KalshiRestClient:
    return KalshiRestClient(KalshiSettings(), max_retries=0)


async def counts(url: str) -> dict[str, int]:
    engine = db.make_engine(url)
    try:
        async with engine.connect() as conn:
            return {
                t: (await conn.execute(text(f"select count(*) from {t}"))).scalar_one()
                for t in ("series", "events", "markets")
            }
    finally:
        await engine.dispose()


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    monkeypatch.chdir(tmp_path)
    for name in ("KALSHI_ENV", "KALSHI_KEY_ID", "KALSHI_PRIVATE_KEY_PATH"):
        monkeypatch.delenv(name, raising=False)


@respx.mock
async def test_first_cycle_is_a_full_refresh_and_sets_both_bookmarks(migrated_db_url: str) -> None:
    kalshi = FakeKalshi()
    kalshi.install(respx.mock)
    engine = db.make_engine(migrated_db_url)
    try:
        async with rest_client() as rest:
            report = await discover(rest, engine, now=lambda: T0)
        updated = await reference.get_state(engine, UPDATED_KEY)
        full = await reference.get_state(engine, FULL_KEY)
    finally:
        await engine.dispose()
    assert report.mode == "full"
    assert (report.series.inserted, report.events.inserted, report.markets.inserted) == (2, 4, 6)
    assert report.markets.rejected == 0
    assert await counts(migrated_db_url) == {"series": 2, "events": 4, "markets": 6}
    settled = next(r for r in kalshi.requests if r.get("status") == "settled")
    assert settled["min_settled_ts"] == epoch(T0 - timedelta(days=7))
    market_calls = kalshi.calls("/markets")
    assert market_calls and all(r["mve_filter"] == "exclude" for r in market_calls)
    assert updated == epoch(T0 - timedelta(minutes=2)) and full == epoch(T0)


@respx.mock
async def test_later_cycles_are_incremental_and_read_only_what_changed(
    migrated_db_url: str,
) -> None:
    kalshi = FakeKalshi()
    kalshi.install(respx.mock)
    engine = db.make_engine(migrated_db_url)
    try:
        async with rest_client() as rest:
            await discover(rest, engine, now=lambda: T0)
            kalshi.requests.clear()
            changed = mkt("KXA-E1-X", "closed")  # an existing market changes status
            fresh = mkt("KXA-E9-X")  # and a brand-new one appears
            kalshi.updated_markets = [changed, fresh]
            kalshi.updated_events = [evt("KXA-E9")]
            report = await discover(rest, engine, now=lambda: T0 + timedelta(minutes=10))
        updated = await reference.get_state(engine, UPDATED_KEY)
    finally:
        await engine.dispose()
    assert report.mode == "incremental"
    assert (report.markets.inserted, report.markets.updated, report.markets.unchanged) == (1, 1, 0)
    assert (report.events.inserted, report.events.unchanged) == (1, 0)
    assert await counts(migrated_db_url) == {"series": 2, "events": 5, "markets": 7}
    expected = epoch(T0 - timedelta(minutes=2))  # the bookmark left by the first cycle
    assert [r["min_updated_ts"] for r in kalshi.calls("/markets")] == [expected]
    assert [r["min_updated_ts"] for r in kalshi.calls("/events")] == [expected]
    assert all("status" not in r for r in kalshi.calls("/markets") + kalshi.calls("/events"))
    assert updated == epoch(T0 + timedelta(minutes=8))  # advanced


@respx.mock
async def test_a_full_refresh_is_forced_after_a_day_or_on_request(migrated_db_url: str) -> None:
    kalshi = FakeKalshi()
    kalshi.install(respx.mock)
    engine = db.make_engine(migrated_db_url)
    try:
        async with rest_client() as rest:
            await discover(rest, engine, now=lambda: T0)
            soon = await discover(rest, engine, now=lambda: T0 + timedelta(hours=23))
            later = await discover(rest, engine, now=lambda: T0 + timedelta(hours=25))
            forced = await discover(rest, engine, now=lambda: T0 + timedelta(hours=26), full=True)
            after_forced = await discover(rest, engine, now=lambda: T0 + timedelta(hours=27))
    finally:
        await engine.dispose()
    assert [r.mode for r in (soon, later, forced, after_forced)] == [
        "incremental",
        "full",
        "full",
        "incremental",  # the forced refresh reset the daily clock
    ]


@respx.mock
async def test_an_unrepresentable_row_is_rejected_loudly_without_blocking_the_rest(
    migrated_db_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    kalshi = FakeKalshi()
    kalshi.markets["open"][1]["settlement_value_dollars"] = "0.1234567"  # finer than 1e-6
    kalshi.install(respx.mock)
    engine = db.make_engine(migrated_db_url)
    try:
        async with rest_client() as rest:
            report = await discover(rest, engine, now=lambda: T0)
    finally:
        await engine.dispose()
    assert report.markets.rejected == 1 and report.markets.inserted == 5
    assert "rejected markets row" in caplog.text
    assert (await counts(migrated_db_url))["markets"] == 5


@respx.mock
async def test_the_bookmark_does_not_advance_when_a_cycle_fails(migrated_db_url: str) -> None:
    kalshi = FakeKalshi()
    kalshi.install(respx.mock)
    engine = db.make_engine(migrated_db_url)
    try:
        async with rest_client() as rest:
            await discover(rest, engine, now=lambda: T0)
            first = await reference.get_state(engine, UPDATED_KEY)
            kalshi.fail_incremental = True
            with pytest.raises(KalshiAPIError):
                await discover(rest, engine, now=lambda: T0 + timedelta(hours=1))
            after = await reference.get_state(engine, UPDATED_KEY)
    finally:
        await engine.dispose()
    assert first is not None and after == first  # the next cycle re-reads the missed window
