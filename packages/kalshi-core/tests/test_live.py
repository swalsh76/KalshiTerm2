"""Opt-in integration tests against the real Kalshi API.

Run with:  KALSHI_INTEGRATION=1 uv run pytest packages/kalshi-core/tests/test_live.py -s

They use the credentials in ``.env`` (read-only production key) and only read: GET requests,
public WebSocket channels. The whole module makes roughly ten REST requests and a handful of
WebSocket connections. Skipped by default and in CI.
"""

import asyncio
import collections
import contextlib
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from kalshi_core.auth import KalshiSigner
from kalshi_core.config import KalshiSettings
from kalshi_core.models import Market
from kalshi_core.orderbook import DELTA, GAP, SNAPSHOT, LocalOrderBook, OrderBookFeed
from kalshi_core.rest import KalshiRestClient
from kalshi_core.ws import RECONNECTED, KalshiWebSocket

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("KALSHI_INTEGRATION") != "1",
        reason="set KALSHI_INTEGRATION=1 to run tests against the real Kalshi API",
    ),
]


@pytest.fixture
def settings() -> KalshiSettings:
    s = KalshiSettings()
    if not s.key_id or s.private_key_path is None:
        pytest.skip("KALSHI_KEY_ID / KALSHI_PRIVATE_KEY_PATH not configured")
    return s


@pytest.fixture
def signer(settings: KalshiSettings) -> KalshiSigner:
    return KalshiSigner.from_settings(settings)


@asynccontextmanager
async def clients(
    settings: KalshiSettings, signer: KalshiSigner
) -> AsyncIterator[tuple[KalshiRestClient, KalshiWebSocket]]:
    async with (
        KalshiRestClient(settings, signer=signer) as rest,
        KalshiWebSocket(settings, signer) as ws,
    ):
        yield rest, ws


def describe(book: LocalOrderBook) -> str:
    levels = f"{len(book.yes)}/{len(book.no)} levels"
    return f"YES bid {book.best_yes_bid} / ask {book.best_yes_ask}, {levels}"


def frozen(book: LocalOrderBook) -> tuple[frozenset[Any], frozenset[Any]]:
    return frozenset(book.yes.items()), frozenset(book.no.items())


async def busiest_market(rest: KalshiRestClient) -> Market:
    page = await rest.markets_page(limit=1000, status="open", mve_filter="exclude")
    ranked = sorted(page.markets, key=lambda m: m.volume_24h_fp or 0, reverse=True)
    assert ranked, "no open markets returned"
    return ranked[0]


class Reader:
    """Reads from an async stream in time-boxed slices without cancelling the generator.

    ``asyncio.wait_for`` cancels a pending ``anext`` on timeout, which kills an async
    generator; keeping the pending read alive between slices avoids that.
    """

    def __init__(self, stream: AsyncIterator[Any]) -> None:
        self._stream = stream
        self._pending: asyncio.Future[Any] | None = None

    async def read_for(self, seconds: float) -> list[Any]:
        out: list[Any] = []
        loop = asyncio.get_running_loop()
        end = loop.time() + seconds
        while (remaining := end - loop.time()) > 0:
            if self._pending is None:
                self._pending = asyncio.ensure_future(anext(self._stream))
            done, _ = await asyncio.wait({self._pending}, timeout=remaining)
            if not done:
                break
            task, self._pending = self._pending, None
            try:
                out.append(task.result())
            except StopAsyncIteration:
                break
        return out

    async def close(self) -> None:
        if self._pending is not None:
            self._pending.cancel()
            with contextlib.suppress(asyncio.CancelledError, StopAsyncIteration):
                await self._pending


@asynccontextmanager
async def reading(stream: AsyncIterator[Any]) -> AsyncIterator[Reader]:
    reader = Reader(stream)
    try:
        yield reader
    finally:
        await reader.close()


async def test_signed_rest_read(settings: KalshiSettings, signer: KalshiSigner) -> None:
    async with KalshiRestClient(settings, signer=signer) as rest:
        status = await rest.exchange_status()
    print(f"\n  exchange_active={status.exchange_active} trading_active={status.trading_active}")
    assert status.exchange_active


async def test_market_models_validate_on_a_large_real_sample(
    settings: KalshiSettings, signer: KalshiSigner
) -> None:
    async with KalshiRestClient(settings, signer=signer) as rest:
        ordinary = await rest.markets_page(limit=1000, status="open", mve_filter="exclude")
        combo = await rest.markets_page(limit=1000, status="open", mve_filter="only")
        events = await rest.multivariate_events_page(limit=5)
        series = await rest.series_list()
    print(f"\n  ordinary markets parsed: {len(ordinary.markets)}")
    print(f"  multivariate markets parsed: {len(combo.markets)}")
    print(f"  series: {len(series)}; combo events: {[e.title for e in events.events[:3]]}")
    for m in ordinary.markets[:3]:
        print(f"    {m.ticker}: {m.status} bid={m.yes_bid_dollars} ask={m.yes_ask_dollars}")
    assert len(ordinary.markets) > 100
    assert not any(m.is_multivariate for m in ordinary.markets)
    assert all(m.is_multivariate and m.mve_selected_legs for m in combo.markets)
    assert events.events and series


async def test_ws_built_orderbook_matches_rest(
    settings: KalshiSettings, signer: KalshiSigner
) -> None:
    async with asyncio.timeout(60), clients(settings, signer) as (rest, ws):
        market = await busiest_market(rest)
        history: collections.deque[Any] = collections.deque(maxlen=400)
        async with OrderBookFeed(ws, rest, [market.ticker]) as feed, reading(feed.events()) as r:
            for ev in await r.read_for(6):
                if ev.kind in (SNAPSHOT, DELTA):
                    history.append(frozen(ev.book))
            rest_book = await rest.orderbook(market.ticker)
            for ev in await r.read_for(1.5):  # bracket the REST response in time
                if ev.kind in (SNAPSHOT, DELTA):
                    history.append(frozen(ev.book))
    expected = (
        frozenset((lv.price, lv.quantity) for lv in rest_book.yes),
        frozenset((lv.price, lv.quantity) for lv in rest_book.no),
    )
    matched = expected in history
    print(f"\n  {market.ticker}: {len(history)} book states seen")
    print(f"  REST levels {len(rest_book.yes)}/{len(rest_book.no)}; matched a WS state: {matched}")
    assert matched, "REST book never equalled a WS-built book state"


async def test_ws_trade_stream(settings: KalshiSettings, signer: KalshiSigner) -> None:
    async with asyncio.timeout(40), clients(settings, signer) as (_, ws):
        await ws.subscribe("trade")
        async with reading(ws.messages()) as r:
            msgs = await r.read_for(8)
    print(f"\n  {len(msgs)} trades in 8s; first: {msgs[0].payload() if msgs else None}")
    assert len(msgs) >= 3
    assert [m.seq for m in msgs] == list(range(1, len(msgs) + 1))  # one contiguous counter


async def test_ws_reconnect_resubscribes(settings: KalshiSettings, signer: KalshiSigner) -> None:
    async with asyncio.timeout(60), KalshiWebSocket(settings, signer, reconnect_base=0.2) as ws:
        await ws.subscribe("trade")
        stream = ws.messages()
        await anext(stream)
        await ws._conn.close()  # cut the link from our side
        types: list[str] = []
        async for msg in stream:
            types.append(msg.type)
            if RECONNECTED in types and msg.type != RECONNECTED:
                break
    trades = types.count("trade")
    print(f"\n  after the cut: {trades} trades drained, {RECONNECTED}, then {types[-1]} resumed")
    assert RECONNECTED in types
    assert types[-1] == "trade"  # data flows again on the new connection


async def test_orderbook_recovers_from_an_injected_gap(
    settings: KalshiSettings, signer: KalshiSigner
) -> None:
    async with asyncio.timeout(60), clients(settings, signer) as (rest, ws):
        market = await busiest_market(rest)
        real, seen = ws._dispatch, [0]

        def lossy(msg: Any) -> None:
            if msg.type == "orderbook_delta":
                seen[0] += 1
                if seen[0] == 15:
                    return  # swallow one message: creates a sequence gap
            real(msg)

        ws._dispatch = lossy  # type: ignore[assignment,method-assign]
        gap = fix = None
        async with OrderBookFeed(ws, rest, [market.ticker]) as feed:
            async for ev in feed.events():
                if ev.kind == GAP:
                    gap = ev
                elif gap is not None and ev.kind == SNAPSHOT:
                    fix = ev
                    break
            stale = set(feed.tracker.stale)
    assert gap is not None and fix is not None and fix.book is not None
    print(f"\n  {market.ticker}: gap detected ({gap.detail})")
    print(f"  resync snapshot approximate={fix.book.approximate}: {describe(fix.book)}")
    assert stale == set()
    assert not fix.book.approximate  # exact, in-stream recovery


async def test_clock_is_close_to_kalshi(settings: KalshiSettings, signer: KalshiSigner) -> None:
    async with KalshiRestClient(settings, signer=signer) as rest:
        skew = await rest.clock_skew(samples=7, spacing=0.3)
    print(f"\n  {skew.describe()}")
    assert skew.status == "ok"


async def test_multivariate_lifecycle_stream_parses(
    settings: KalshiSettings, signer: KalshiSigner
) -> None:
    async with asyncio.timeout(40), clients(settings, signer) as (_, ws):
        await ws.subscribe("multivariate_market_lifecycle")
        async with reading(ws.messages()) as r:
            msgs = await r.read_for(6)
    kinds = collections.Counter(
        getattr(m.payload(), "event_type", "event_lifecycle")
        for m in msgs  # raises if unparseable
    )
    print(f"\n  {len(msgs)} lifecycle messages in 6s: {dict(kinds)}")
    assert len(msgs) > 20


async def test_orderbook_feed_adds_and_removes_markets_live_without_false_gaps(
    settings: KalshiSettings, signer: KalshiSigner
) -> None:
    async with asyncio.timeout(90), clients(settings, signer) as (rest, ws):
        page = await rest.markets_page(limit=1000, status="open", mve_filter="exclude")
        ranked = sorted(page.markets, key=lambda m: m.volume_24h_fp or 0, reverse=True)
        busy = [m.ticker for m in ranked[:6]]
        async with OrderBookFeed(ws, rest, busy[:3]) as feed, reading(feed.events()) as r:
            events = await r.read_for(5)
            await feed.add_markets(busy[3:5])
            events += await r.read_for(6)
            await feed.remove_markets([busy[0]])
            events += await r.read_for(6)
            books, stale, watched = set(feed.tracker.books), set(feed.tracker.stale), feed.tickers
    gaps = [e for e in events if e.kind == GAP]
    kinds = collections.Counter(e.kind for e in events)
    print(f"\n  started with {busy[:3]}")
    print(f"  added {busy[3:5]}; removed {busy[0]}")
    print(f"  events: {dict(kinds)}; false gaps: {len(gaps)}")
    print(f"  watching now: {len(watched)} markets; books held: {len(books)}; stale: {len(stale)}")
    assert not gaps
    assert set(watched) == set(busy[1:5]) and books == set(busy[1:5]) and not stale
