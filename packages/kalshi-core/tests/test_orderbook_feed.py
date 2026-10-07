import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from decimal import Decimal as D
from pathlib import Path
from typing import Any

import pytest
import respx
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from kalshi_core.auth import KalshiSigner
from kalshi_core.config import KalshiSettings
from kalshi_core.orderbook import (
    DELTA,
    GAP,
    MESSAGE,
    RESET,
    RESUBSCRIBE_FAILED,
    SNAPSHOT,
    BookEvent,
    OrderBookFeed,
)
from kalshi_core.rest import KalshiRestClient
from kalshi_core.ws import KalshiWebSocket
from websockets.asyncio.server import ServerConnection, serve

Handler = Callable[[ServerConnection], Awaitable[None]]
REST = "https://external-api.demo.kalshi.co/trade-api/v2"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    for name in ("KALSHI_ENV", "KALSHI_KEY_ID", "KALSHI_PRIVATE_KEY_PATH"):
        monkeypatch.delenv(name, raising=False)


@asynccontextmanager
async def fake_server(handler: Handler) -> AsyncIterator[str]:
    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        yield f"ws://127.0.0.1:{port}/trade-api/ws/v2"


def snapshot(
    sid: int, seq: int, ticker: str, yes: Any = (), no: Any = (), id: int | None = None
) -> str:
    msg = {"market_ticker": ticker, "yes_dollars_fp": list(yes), "no_dollars_fp": list(no)}
    body: dict[str, Any] = {"type": "orderbook_snapshot", "sid": sid, "seq": seq, "msg": msg}
    if id is not None:
        body["id"] = id
    return json.dumps(body)


def delta(sid: int, seq: int, ticker: str, side: str, price: str, change: str) -> str:
    msg = {"market_ticker": ticker, "side": side, "price_dollars": price, "delta_fp": change}
    return json.dumps({"type": "orderbook_delta", "sid": sid, "seq": seq, "msg": msg})


async def reply_subscribed(ws: ServerConnection, cmd: dict[str, Any], sid: int) -> None:
    channel = cmd["params"]["channels"][0]
    await ws.send(
        json.dumps({"id": cmd["id"], "type": "subscribed", "msg": {"channel": channel, "sid": sid}})
    )


def make(url: str, **kw: Any) -> tuple[KalshiWebSocket, KalshiRestClient]:
    settings = KalshiSettings()
    signer = KalshiSigner("kid", Ed25519PrivateKey.generate())
    ws = KalshiWebSocket(settings, signer, url=url, reconnect_base=0.01, **kw)
    return ws, KalshiRestClient(settings)


async def take(feed: OrderBookFeed, n: int) -> list[BookEvent]:
    stream = feed.events()
    return [await anext(stream) for _ in range(n)]


async def test_builds_book_from_snapshot_and_deltas_and_passes_other_messages() -> None:
    async def handler(ws: ServerConnection) -> None:
        cmd = json.loads(await ws.recv())
        assert cmd["params"] == {"channels": ["orderbook_delta"], "market_tickers": ["A"]}
        await reply_subscribed(ws, cmd, 4)
        await ws.send(snapshot(4, 1, "A", yes=[["0.40", "10.00"]], no=[["0.55", "5.00"]]))
        await ws.send(delta(4, 2, "A", "yes", "0.41", "3.00"))
        await ws.send(json.dumps({"type": "trade", "sid": 9, "seq": 1, "msg": {}}))
        await ws.wait_closed()

    async with fake_server(handler) as url, asyncio.timeout(5):
        ws, rest = make(url)
        async with ws, rest, OrderBookFeed(ws, rest, ["A"]) as feed:
            events = await take(feed, 3)
            book = feed.tracker.books["A"]
    assert [e.kind for e in events] == [SNAPSHOT, DELTA, MESSAGE]
    assert book.yes == {D("0.40"): D("10.00"), D("0.41"): D("3.00")}
    assert book.best_yes_ask == D("0.45")


async def test_gap_is_repaired_by_in_stream_snapshot_without_touching_rest() -> None:
    requested: list[dict[str, Any]] = []

    async def handler(ws: ServerConnection) -> None:
        cmd = json.loads(await ws.recv())
        await reply_subscribed(ws, cmd, 1)
        await ws.send(snapshot(1, 1, "A", yes=[["0.40", "1.00"]]))
        await ws.send(delta(1, 3, "A", "yes", "0.40", "5.00"))  # seq 2 lost -> gap
        cmd = json.loads(await ws.recv())  # the get_snapshot request
        requested.append(cmd)
        await ws.send(snapshot(1, 4, "A", yes=[["0.40", "7.00"]], id=cmd["id"]))
        await ws.send(delta(1, 5, "A", "yes", "0.40", "1.00"))
        await ws.wait_closed()

    with respx.mock(assert_all_called=False) as router:
        rest_route = router.get(f"{REST}/markets/A/orderbook").respond(500)
        async with fake_server(handler) as url, asyncio.timeout(5):
            ws, rest = make(url)
            async with ws, rest, OrderBookFeed(ws, rest, ["A"]) as feed:
                events = await take(feed, 4)
                book = feed.tracker.books["A"]
    assert [e.kind for e in events] == [SNAPSHOT, GAP, SNAPSHOT, DELTA]
    assert requested[0]["cmd"] == "update_subscription"
    assert requested[0]["params"] == {"sid": 1, "market_tickers": ["A"], "action": "get_snapshot"}
    assert book.yes == {D("0.40"): D("8.00")} and not book.approximate
    assert rest_route.call_count == 0


async def test_rejected_get_snapshot_falls_back_to_rest_and_is_flagged_approximate() -> None:
    async def handler(ws: ServerConnection) -> None:
        cmd = json.loads(await ws.recv())
        await reply_subscribed(ws, cmd, 1)
        await ws.send(snapshot(1, 1, "A", yes=[["0.40", "1.00"]]))
        await ws.send(delta(1, 3, "A", "yes", "0.40", "5.00"))  # gap
        cmd = json.loads(await ws.recv())
        err = {"code": 11, "msg": "Invalid parameter"}
        await ws.send(json.dumps({"id": cmd["id"], "type": "error", "msg": err}))
        await ws.wait_closed()

    body = {
        "orderbook_fp": {"yes_dollars": [["0.4200", "9.00"]], "no_dollars": [["0.5600", "2.00"]]}
    }
    with respx.mock as router:
        router.get(f"{REST}/markets/A/orderbook").respond(json=body)
        async with fake_server(handler) as url, asyncio.timeout(5):
            ws, rest = make(url)
            async with ws, rest, OrderBookFeed(ws, rest, ["A"]) as feed:
                events = await take(feed, 3)
                book = feed.tracker.books["A"]
    assert [e.kind for e in events] == [SNAPSHOT, GAP, SNAPSHOT]
    assert events[2].detail == "rest"
    assert book.approximate and book.yes == {D("0.4200"): D("9.00")}
    assert "A" not in feed.tracker.stale


async def test_unanswered_get_snapshot_times_out_into_rest_fallback() -> None:
    async def handler(ws: ServerConnection) -> None:
        cmd = json.loads(await ws.recv())
        await reply_subscribed(ws, cmd, 1)
        await ws.send(snapshot(1, 1, "A"))
        await ws.send(delta(1, 5, "A", "yes", "0.40", "1.00"))  # gap; request goes unanswered
        await ws.wait_closed()

    body = {"orderbook_fp": {"yes_dollars": [["0.3000", "1.00"]], "no_dollars": []}}
    with respx.mock as router:
        router.get(f"{REST}/markets/A/orderbook").respond(json=body)
        async with fake_server(handler) as url, asyncio.timeout(5):
            ws, rest = make(url)
            async with ws, rest, OrderBookFeed(ws, rest, ["A"], snapshot_timeout=0.2) as feed:
                events = await take(feed, 3)
    assert [e.kind for e in events] == [SNAPSHOT, GAP, SNAPSHOT]
    assert events[2].detail == "rest"


async def test_rest_failure_is_reported_and_book_stays_stale() -> None:
    async def handler(ws: ServerConnection) -> None:
        cmd = json.loads(await ws.recv())
        await reply_subscribed(ws, cmd, 1)
        await ws.send(snapshot(1, 1, "A"))
        await ws.send(delta(1, 5, "A", "yes", "0.40", "1.00"))
        await ws.wait_closed()

    with respx.mock as router:
        router.get(f"{REST}/markets/A/orderbook").respond(404)
        async with fake_server(handler) as url, asyncio.timeout(5):
            ws, rest = make(url)
            async with ws, rest, OrderBookFeed(ws, rest, ["A"], snapshot_timeout=0.1) as feed:
                events = await take(feed, 3)
                stale = set(feed.tracker.stale)
    assert [e.kind for e in events] == [SNAPSHOT, GAP, "resync_failed"]
    assert stale == {"A"}


async def test_gap_resyncs_every_market_in_the_subscription() -> None:
    requested: list[str] = []

    async def handler(ws: ServerConnection) -> None:
        cmd = json.loads(await ws.recv())
        await reply_subscribed(ws, cmd, 1)
        await ws.send(snapshot(1, 1, "A"))
        await ws.send(snapshot(1, 2, "B"))
        await ws.send(delta(1, 4, "A", "yes", "0.40", "1.00"))  # gap affects A and B
        seq = 5
        for _ in range(2):
            cmd = json.loads(await ws.recv())
            ticker = cmd["params"]["market_tickers"][0]
            requested.append(ticker)
            await ws.send(snapshot(1, seq, ticker, id=cmd["id"]))
            seq += 1
        await ws.wait_closed()

    async with fake_server(handler) as url, asyncio.timeout(5):
        ws, rest = make(url)
        async with ws, rest, OrderBookFeed(ws, rest, ["A", "B"]) as feed:
            events = await take(feed, 5)
            stale = set(feed.tracker.stale)
    assert [e.kind for e in events[:3]] == [SNAPSHOT, SNAPSHOT, GAP]
    assert events[2].tickers == ("A", "B")
    assert sorted(requested) == ["A", "B"]
    assert stale == set()


async def test_reconnect_resets_books_and_new_snapshots_rebuild_them() -> None:
    connections = 0

    async def handler(ws: ServerConnection) -> None:
        nonlocal connections
        connections += 1
        cmd = json.loads(await ws.recv())
        await reply_subscribed(ws, cmd, 1)  # same sid both times, like the real service
        if connections == 1:
            await ws.send(snapshot(1, 1, "A", yes=[["0.40", "1.00"]]))
            await ws.close()
        else:
            await ws.send(snapshot(1, 1, "A", yes=[["0.50", "2.00"]]))
            await ws.send(delta(1, 2, "A", "yes", "0.50", "1.00"))
            await ws.wait_closed()

    async with fake_server(handler) as url, asyncio.timeout(5):
        ws, rest = make(url)
        async with ws, rest, OrderBookFeed(ws, rest, ["A"]) as feed:
            events = await take(feed, 4)
            book = feed.tracker.books["A"]
    assert [e.kind for e in events] == [SNAPSHOT, RESET, SNAPSHOT, DELTA]  # no spurious gap
    assert book.yes == {D("0.50"): D("3.00")}


async def test_failed_orderbook_resubscribe_is_reported() -> None:
    connections = 0

    async def handler(ws: ServerConnection) -> None:
        nonlocal connections
        connections += 1
        cmd = json.loads(await ws.recv())
        if connections == 1:
            await reply_subscribed(ws, cmd, 1)
            await ws.send(snapshot(1, 1, "A"))
            await ws.close()
        else:
            err = {"code": 11, "msg": "Invalid parameter"}
            await ws.send(json.dumps({"id": cmd["id"], "type": "error", "msg": err}))
            await ws.wait_closed()

    async with fake_server(handler) as url, asyncio.timeout(5):
        ws, rest = make(url)
        async with ws, rest, OrderBookFeed(ws, rest, ["A"]) as feed:
            events = await take(feed, 3)
    assert [e.kind for e in events] == [SNAPSHOT, RESET, RESUBSCRIBE_FAILED]
    assert events[2].tickers == ("A",)


async def test_terminal_connection_loss_raises_from_events() -> None:
    async def handler(ws: ServerConnection) -> None:
        cmd = json.loads(await ws.recv())
        await reply_subscribed(ws, cmd, 1)
        await ws.close()

    async with fake_server(handler) as url, asyncio.timeout(5):
        ws, rest = make(url, auto_reconnect=False)
        async with ws, rest, OrderBookFeed(ws, rest, ["A"]) as feed:
            from kalshi_core.ws import KalshiWSError

            with pytest.raises(KalshiWSError):
                await anext(feed.events())
