import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from decimal import Decimal as D
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from kalshi_core.auth import KalshiSigner
from kalshi_core.config import KalshiSettings
from kalshi_core.orderbook import DELTA, SNAPSHOT, OrderBookFeed
from kalshi_core.rest import KalshiRestClient
from kalshi_core.ws import RECONNECTED, KalshiWebSocket, KalshiWSError
from websockets.asyncio.server import ServerConnection, serve

Handler = Callable[[ServerConnection], Awaitable[None]]


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


def make_ws(url: str, **kw: Any) -> KalshiWebSocket:
    signer = KalshiSigner("kid", Ed25519PrivateKey.generate())
    kw.setdefault("reconnect_base", 0.01)
    return KalshiWebSocket(KalshiSettings(), signer, url=url, **kw)


async def reply_subscribed(ws: ServerConnection, cmd: dict[str, Any], sid: int) -> None:
    channel = cmd["params"]["channels"][0]
    await ws.send(
        json.dumps({"id": cmd["id"], "type": "subscribed", "msg": {"channel": channel, "sid": sid}})
    )


def trade(sid: int, seq: int) -> str:
    msg = {
        "trade_id": f"t{seq}",
        "market_ticker": "M",
        "yes_price_dollars": "0.5",
        "no_price_dollars": "0.5",
        "count_fp": "1.00",
    }
    return json.dumps({"type": "trade", "sid": sid, "seq": seq, "msg": msg})


async def until(predicate: Callable[[], bool]) -> None:
    while not predicate():  # noqa: ASYNC110 - polling a plain predicate
        await asyncio.sleep(0.01)


async def test_overflow_forces_a_reconnect_with_an_ordered_signal() -> None:
    connections = 0

    async def handler(ws: ServerConnection) -> None:
        nonlocal connections
        connections += 1
        cmd = json.loads(await ws.recv())
        await reply_subscribed(ws, cmd, 1)
        if connections == 1:
            for seq in range(1, 2001):  # far more than the client will queue
                await ws.send(trade(1, seq))
        else:
            await ws.send(trade(1, 1))
        await ws.wait_closed()

    async with (
        fake_server(handler) as url,
        asyncio.timeout(10),
        make_ws(url, max_queue_messages=100) as ws,
    ):
        await ws.subscribe("trade")
        await until(lambda: ws.stats()["overflows"] == 1)  # nobody is consuming
        stream = ws.messages()
        backlog = [await anext(stream) for _ in range(100)]
        event = await anext(stream)
        after = await anext(stream)
        stats = ws.stats()
    assert [m.seq for m in backlog] == list(range(1, 101))  # delivered in order, none skipped
    assert event.type == RECONNECTED
    assert event.msg["reason"] == "overflow"
    assert 1 <= event.msg["dropped"] <= 1900
    assert event.msg["resubscribed"][0]["channel"] == "trade"
    assert after.type == "trade" and after.seq == 1  # fresh connection, fresh counter
    assert stats["queue_limit"] == 100 and stats["high_water"] == 100
    assert stats["overflows"] == 1 and stats["dropped"] == event.msg["dropped"]


async def test_overflow_without_auto_reconnect_ends_the_stream_with_an_error() -> None:
    async def handler(ws: ServerConnection) -> None:
        cmd = json.loads(await ws.recv())
        await reply_subscribed(ws, cmd, 1)
        for seq in range(1, 1001):
            await ws.send(trade(1, seq))
        await ws.wait_closed()

    async with (
        fake_server(handler) as url,
        asyncio.timeout(10),
        make_ws(url, max_queue_messages=50, auto_reconnect=False) as ws,
    ):
        await ws.subscribe("trade")
        await until(lambda: ws.stats()["overflows"] == 1)
        stream = ws.messages()
        received = []
        with pytest.raises(KalshiWSError, match="receive queue overflow"):
            async for message in stream:
                received.append(message.seq)
    assert received == list(range(1, 51))  # the backlog is still delivered first


async def test_no_overflow_when_the_consumer_keeps_up() -> None:
    async def handler(ws: ServerConnection) -> None:
        cmd = json.loads(await ws.recv())
        await reply_subscribed(ws, cmd, 1)
        for seq in range(1, 301):
            await ws.send(trade(1, seq))
        await ws.wait_closed()

    async with (
        fake_server(handler) as url,
        asyncio.timeout(10),
        make_ws(url, max_queue_messages=1000) as ws,
    ):
        await ws.subscribe("trade")
        stream = ws.messages()
        seqs = [(await anext(stream)).seq for _ in range(300)]
        stats = ws.stats()
    assert seqs == list(range(1, 301))
    assert stats["overflows"] == 0 and stats["dropped"] == 0
    assert 1 <= stats["high_water"] <= 300 and stats["queue_depth"] == 0


async def test_feed_applies_backpressure_so_the_backlog_stays_in_the_bounded_queue() -> None:
    async def handler(ws: ServerConnection) -> None:
        cmd = json.loads(await ws.recv())
        await reply_subscribed(ws, cmd, 1)
        snap = {"market_ticker": "A", "yes_dollars_fp": [["0.40", "10.00"]], "no_dollars_fp": []}
        await ws.send(json.dumps({"type": "orderbook_snapshot", "sid": 1, "seq": 1, "msg": snap}))
        for seq in range(2, 502):
            delta = {
                "market_ticker": "A",
                "side": "yes",
                "price_dollars": "0.40",
                "delta_fp": "1.00",
            }
            await ws.send(
                json.dumps({"type": "orderbook_delta", "sid": 1, "seq": seq, "msg": delta})
            )
        await ws.wait_closed()

    async with fake_server(handler) as url, asyncio.timeout(10):
        ws = make_ws(url, max_queue_messages=100_000)
        async with (
            ws,
            KalshiRestClient(KalshiSettings()) as rest,
            OrderBookFeed(ws, rest, ["A"], out_limit=10) as feed,
        ):
            await until(lambda: ws.stats()["queue_depth"] > 100)  # the consumer isn't reading
            held_in_feed = feed.stats()["feed_depth"]
            kinds = []
            stream = feed.events()
            for _ in range(501):
                kinds.append((await anext(stream)).kind)
            book = feed.tracker.books["A"]
    assert held_in_feed <= 10 + 3  # the feed's own queue stayed bounded
    assert kinds == [SNAPSHOT] + [DELTA] * 500  # nothing lost, nothing reordered
    assert book.yes == {D("0.40"): D("510.00")}


async def test_reconnect_after_overflow_waits_for_the_consumer_to_drain() -> None:
    connections = 0

    async def handler(ws: ServerConnection) -> None:
        nonlocal connections
        connections += 1
        cmd = json.loads(await ws.recv())
        await reply_subscribed(ws, cmd, 1)
        if connections == 1:
            for seq in range(1, 1001):
                await ws.send(trade(1, seq))
        else:
            await ws.send(trade(1, 1))
        await ws.wait_closed()

    async with (
        fake_server(handler) as url,
        asyncio.timeout(10),
        make_ws(url, max_queue_messages=100) as ws,
    ):
        await ws.subscribe("trade")
        await until(lambda: ws.stats()["overflows"] == 1)
        await asyncio.sleep(0.4)  # many times the 10 ms backoff
        assert connections == 1  # no reconnect storm while the queue is still full
        stream = ws.messages()
        first = [await anext(stream) for _ in range(50)]  # drain to half the limit
        await until(lambda: connections == 2)  # now it reconnects
        rest = [await anext(stream) for _ in range(50)]  # remaining backlog, then...
        event = await anext(stream)
    assert [m.seq for m in first + rest] == list(range(1, 101))
    assert event.type == RECONNECTED and event.msg["reason"] == "overflow"
    assert ws.stats()["overflows"] == 1  # the new connection did not overflow again
