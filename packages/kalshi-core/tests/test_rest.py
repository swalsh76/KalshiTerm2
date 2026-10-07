import base64
import logging
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import respx
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from kalshi_core.auth import HEADER_KEY, HEADER_SIGNATURE, HEADER_TIMESTAMP, KalshiSigner
from kalshi_core.config import KalshiSettings
from kalshi_core.ratelimit import TokenBucket
from kalshi_core.rest import KalshiAPIError, KalshiRestClient, ReadOnlyViolation

BASE = "https://external-api.demo.kalshi.co/trade-api/v2"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    for name in ("KALSHI_ENV", "KALSHI_KEY_ID", "KALSHI_PRIVATE_KEY_PATH"):
        monkeypatch.delenv(name, raising=False)


class Recorder:
    def __init__(self) -> None:
        self.sleeps: list[float] = []

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)


def client(rec: Recorder | None = None, **kw: object) -> KalshiRestClient:
    rec = rec or Recorder()
    return KalshiRestClient(KalshiSettings(), sleep=rec.sleep, **kw)  # type: ignore[arg-type]


def market(ticker: str) -> dict[str, object]:
    return {
        "ticker": ticker,
        "event_ticker": "EV-1",
        "market_type": "binary",
        "status": "active",
        "yes_bid_dollars": "0.5600",
        "volume_fp": "10.00",
        "result": "",
        "unknown_future_field": 1,
    }


@respx.mock
async def test_exchange_status_parses() -> None:
    respx.get(f"{BASE}/exchange/status").respond(
        json={"exchange_active": True, "trading_active": False}
    )
    async with client() as c:
        status = await c.exchange_status()
    assert status.exchange_active and not status.trading_active


@respx.mock
async def test_requests_are_signed_over_full_path_without_query() -> None:
    route = respx.get(f"{BASE}/markets").respond(json={"markets": [], "cursor": ""})
    key = Ed25519PrivateKey.generate()
    async with client(signer=KalshiSigner("kid", key)) as c:
        await c.markets_page(limit=5)
    req = route.calls.last.request
    assert req.headers[HEADER_KEY] == "kid"
    ts = req.headers[HEADER_TIMESTAMP]
    key.public_key().verify(
        base64.b64decode(req.headers[HEADER_SIGNATURE]),
        f"{ts}GET/trade-api/v2/markets".encode(),
    )


@respx.mock
async def test_markets_pagination_follows_cursor() -> None:
    route = respx.get(f"{BASE}/markets").mock(
        side_effect=[
            httpx.Response(200, json={"markets": [market("A"), market("B")], "cursor": "c1"}),
            httpx.Response(200, json={"markets": [market("C")], "cursor": ""}),
        ]
    )
    async with client() as c:
        tickers = [m.ticker async for m in c.iter_markets(status="open", limit=2)]
    assert tickers == ["A", "B", "C"]
    first, second = (call.request.url.params for call in route.calls)
    assert first["status"] == "open" and "cursor" not in first
    assert second["cursor"] == "c1" and second["status"] == "open"


@respx.mock
async def test_market_fields_are_decimal_and_extras_ignored() -> None:
    respx.get(f"{BASE}/markets").respond(json={"markets": [market("A")], "cursor": ""})
    async with client() as c:
        page = await c.markets_page()
    m = page.markets[0]
    assert m.yes_bid_dollars == Decimal("0.5600")
    assert m.volume_fp == Decimal("10.00")


@respx.mock
async def test_events_and_series() -> None:
    respx.get(f"{BASE}/events").respond(
        json={
            "events": [
                {
                    "event_ticker": "EV-1",
                    "series_ticker": "S",
                    "title": "t",
                    "mutually_exclusive": True,
                }
            ],
            "cursor": "",
        }
    )
    respx.get(f"{BASE}/series").respond(
        json={"series": [{"ticker": "S", "title": "t", "frequency": "daily", "category": "x"}]}
    )
    async with client() as c:
        events = [e async for e in c.iter_events()]
        series = await c.series_list()
    assert events[0].mutually_exclusive and series[0].ticker == "S"


@respx.mock
async def test_orderbook_parses_levels() -> None:
    respx.get(f"{BASE}/markets/T-1/orderbook").respond(
        json={"orderbook_fp": {"yes_dollars": [["0.1500", "100.00"]], "no_dollars": []}}
    )
    async with client() as c:
        book = await c.orderbook("T-1", depth=5)
    assert book.yes[0].price == Decimal("0.1500")
    assert book.yes[0].quantity == Decimal("100.00")
    assert book.no == []


@respx.mock
async def test_retries_429_with_backoff_then_succeeds() -> None:
    respx.get(f"{BASE}/exchange/status").mock(
        side_effect=[
            httpx.Response(429, json={"error": "too many requests"}),
            httpx.Response(429, json={"error": "too many requests"}),
            httpx.Response(200, json={"exchange_active": True, "trading_active": True}),
        ]
    )
    rec = Recorder()
    async with client(rec) as c:
        await c.exchange_status()
    assert len(rec.sleeps) == 2
    assert rec.sleeps[1] > rec.sleeps[0] * 0.9  # grows (within jitter)


@respx.mock
async def test_gives_up_after_max_retries() -> None:
    route = respx.get(f"{BASE}/exchange/status").respond(503, text="down")
    rec = Recorder()
    async with client(rec, max_retries=2) as c:
        with pytest.raises(KalshiAPIError) as exc:
            await c.exchange_status()
    assert exc.value.status == 503
    assert route.call_count == 3


@respx.mock
async def test_client_errors_are_not_retried() -> None:
    route = respx.get(f"{BASE}/markets").respond(400, json={"error": "bad"})
    rec = Recorder()
    async with client(rec) as c:
        with pytest.raises(KalshiAPIError) as exc:
            await c.markets_page()
    assert exc.value.status == 400
    assert route.call_count == 1 and rec.sleeps == []


@respx.mock
async def test_transport_errors_are_retried() -> None:
    respx.get(f"{BASE}/exchange/status").mock(
        side_effect=[
            httpx.ConnectError("boom"),
            httpx.Response(200, json={"exchange_active": True, "trading_active": True}),
        ]
    )
    rec = Recorder()
    async with client(rec) as c:
        await c.exchange_status()
    assert len(rec.sleeps) == 1


@respx.mock
@pytest.mark.parametrize("method", ["POST", "PUT", "DELETE", "PATCH"])
async def test_non_get_requests_are_blocked(method: str) -> None:
    route = respx.route().respond(200, json={})
    async with client() as c:
        with pytest.raises(ReadOnlyViolation):
            await c._http.request(method, f"{BASE}/portfolio/orders")
    assert route.call_count == 0  # blocked before anything is sent


@respx.mock
async def test_each_attempt_acquires_rate_limit_tokens() -> None:
    respx.get(f"{BASE}/exchange/status").mock(
        side_effect=[
            httpx.Response(500),
            httpx.Response(200, json={"exchange_active": True, "trading_active": True}),
        ]
    )

    class CountingBucket(TokenBucket):
        acquired = 0

        async def acquire(self, cost: float = 10) -> None:
            type(self).acquired += 1

    async with client(bucket=CountingBucket(1, 1)) as c:
        await c.exchange_status()
    assert CountingBucket.acquired == 2


def test_production_logs_banner(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("KALSHI_ENV", "production")
    with caplog.at_level(logging.WARNING, logger="kalshi_core"):
        KalshiRestClient(KalshiSettings())
    assert "PRODUCTION" in caplog.text


def test_demo_logs_no_banner(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="kalshi_core"):
        KalshiRestClient(KalshiSettings())
    assert "PRODUCTION" not in caplog.text


def trade(trade_id: str, ticker: str = "A") -> dict[str, object]:
    return {
        "trade_id": trade_id,
        "ticker": ticker,
        "yes_price_dollars": "0.5900",
        "no_price_dollars": "0.4100",
        "count_fp": "149.83",
        "taker_side": "yes",
        "taker_book_side": "bid",  # fields we do not model are ignored
        "is_block_trade": False,
        "created_time": "2026-10-07T21:22:01.978691Z",
    }


@respx.mock
async def test_trades_follow_the_cursor_and_keep_exact_values() -> None:
    route = respx.get(f"{BASE}/markets/trades").mock(
        side_effect=[
            httpx.Response(200, json={"trades": [trade("t1"), trade("t2")], "cursor": "c1"}),
            httpx.Response(200, json={"trades": [trade("t3", "B")], "cursor": ""}),
        ]
    )
    async with client() as c:
        found = [t async for t in c.iter_trades(min_ts=100, max_ts=200, limit=2)]
    assert [t.trade_id for t in found] == ["t1", "t2", "t3"]
    assert found[0].count_fp == Decimal("149.83") and found[0].yes_price_dollars == Decimal("0.59")
    assert found[0].created_time.microsecond == 978691  # microseconds survive
    first, second = (call.request.url.params for call in route.calls)
    assert first["min_ts"] == "100" and first["max_ts"] == "200" and "cursor" not in first
    assert second["cursor"] == "c1" and second["min_ts"] == "100"
