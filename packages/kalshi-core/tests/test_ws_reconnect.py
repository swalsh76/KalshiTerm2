import asyncio
import base64
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
import websockets.asyncio.client
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from kalshi_core.auth import HEADER_SIGNATURE, HEADER_TIMESTAMP, KalshiSigner
from kalshi_core.config import KalshiSettings
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


class Sleeps:
    """Records backoff delays and returns immediately."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    async def sleep(self, seconds: float) -> None:
        self.delays.append(seconds)
        await asyncio.sleep(0)


class Flaky:
    """Connect factory: first call succeeds, then ``fails`` calls raise OSError."""

    def __init__(self, fails: int) -> None:
        self.fails = fails
        self.calls = 0

    async def __call__(self, url: str, **kw: Any) -> Any:
        self.calls += 1
        if self.calls > 1 and self.fails > 0:
            self.fails -= 1
            raise OSError("server unreachable")
        return await websockets.asyncio.client.connect(url, **kw)


def make_ws(url: str, key: Ed25519PrivateKey | None = None, **kw: Any) -> KalshiWebSocket:
    signer = KalshiSigner("kid", key or Ed25519PrivateKey.generate())
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


async def test_reconnect_resubscribes_with_same_params_and_signals() -> None:
    resub: list[dict[str, Any]] = []
    connections = 0

    async def handler(ws: ServerConnection) -> None:
        nonlocal connections
        connections += 1
        cmd = json.loads(await ws.recv())
        if connections == 1:
            await reply_subscribed(ws, cmd, 1)
            await ws.send(trade(1, 1))
            await ws.close()
        else:
            resub.append(cmd)
            await reply_subscribed(ws, cmd, 9)
            await ws.send(trade(9, 1))
            await ws.wait_closed()

    async with fake_server(handler) as url, asyncio.timeout(5), make_ws(url) as ws:
        await ws.subscribe("trade", market_tickers=["M"])
        stream = ws.messages()
        first = await anext(stream)
        event = await anext(stream)
        after = await anext(stream)
        subs = ws.subscriptions
    assert first.sid == 1
    assert event.type == RECONNECTED
    assert event.msg["resubscribed"] == [{"channel": "trade", "sid": 9, "old_sid": 1}]
    assert event.msg["failed"] == []
    assert after.sid == 9
    assert resub[0]["params"] == {"channels": ["trade"], "market_tickers": ["M"]}
    assert subs == [(9, "trade", {"market_tickers": ["M"]})]


async def test_backoff_grows_and_gives_up_after_max_attempts() -> None:
    async def handler(ws: ServerConnection) -> None:
        cmd = json.loads(await ws.recv())
        await reply_subscribed(ws, cmd, 1)
        await ws.close()

    sleeps = Sleeps()
    flaky = Flaky(fails=100)
    async with (
        fake_server(handler) as url,
        asyncio.timeout(5),
        make_ws(
            url, connect=flaky, sleep=sleeps.sleep, max_reconnect_attempts=4, reconnect_base=1.0
        ) as ws,
    ):
        await ws.subscribe("trade")
        with pytest.raises(KalshiWSError, match="reconnect attempts exhausted"):
            await anext(ws.messages())
    assert len(sleeps.delays) == 4
    for attempt, delay in enumerate(sleeps.delays):
        assert 0.5 * 2**attempt <= delay <= 1.0 * 2**attempt
    assert flaky.calls == 1 + 4


async def test_backoff_is_capped() -> None:
    async def handler(ws: ServerConnection) -> None:
        await ws.close()

    sleeps = Sleeps()
    async with (
        fake_server(handler) as url,
        asyncio.timeout(5),
        make_ws(
            url,
            connect=Flaky(fails=100),
            sleep=sleeps.sleep,
            max_reconnect_attempts=8,
            reconnect_base=1.0,
            reconnect_max=5.0,
        ) as ws,
    ):
        with pytest.raises(KalshiWSError):
            await anext(ws.messages())
    assert max(sleeps.delays) <= 5.0


async def test_recovers_after_transient_connect_failures() -> None:
    connections = 0

    async def handler(ws: ServerConnection) -> None:
        nonlocal connections
        connections += 1
        cmd = json.loads(await ws.recv())
        await reply_subscribed(ws, cmd, connections)
        if connections == 1:
            await ws.close()
        else:
            await ws.wait_closed()

    sleeps = Sleeps()
    async with (
        fake_server(handler) as url,
        asyncio.timeout(5),
        make_ws(url, connect=Flaky(fails=2), sleep=sleeps.sleep) as ws,
    ):
        await ws.subscribe("trade")
        event = await anext(ws.messages())
    assert event.type == RECONNECTED
    assert len(sleeps.delays) == 3  # two failed attempts, then success


async def test_deliberate_close_during_backoff_ends_cleanly() -> None:
    async def handler(ws: ServerConnection) -> None:
        await ws.recv()
        await ws.close()

    backing_off = asyncio.Event()

    async def hang(_: float) -> None:
        backing_off.set()
        await asyncio.Event().wait()

    async with fake_server(handler) as url, asyncio.timeout(5):
        ws = make_ws(url, connect=Flaky(fails=100), sleep=hang)
        await ws.connect()
        await ws._conn.send(json.dumps({"id": 1, "cmd": "list_subscriptions"}))
        await backing_off.wait()
        await ws.close()
        assert [m async for m in ws.messages()] == []


async def test_each_connection_is_signed_freshly() -> None:
    key = Ed25519PrivateKey.generate()
    seen: list[tuple[str, str]] = []

    async def handler(ws: ServerConnection) -> None:
        assert ws.request is not None
        seen.append((ws.request.headers[HEADER_TIMESTAMP], ws.request.headers[HEADER_SIGNATURE]))
        cmd = json.loads(await ws.recv())
        await reply_subscribed(ws, cmd, len(seen))
        if len(seen) == 1:
            await asyncio.sleep(0.05)
            await ws.close()
        else:
            await ws.wait_closed()

    async with fake_server(handler) as url, asyncio.timeout(5), make_ws(url, key) as ws:
        await ws.subscribe("trade")
        await anext(ws.messages())
    assert len(seen) == 2
    assert seen[0][0] != seen[1][0]
    for ts, sig in seen:
        key.public_key().verify(base64.b64decode(sig), f"{ts}GET/trade-api/ws/v2".encode())


async def test_rejected_resubscribe_is_reported_and_dropped() -> None:
    connections = 0

    async def handler(ws: ServerConnection) -> None:
        nonlocal connections
        connections += 1
        cmd = json.loads(await ws.recv())
        if connections == 1:
            await reply_subscribed(ws, cmd, 1)
            await ws.close()
        else:
            err = {"code": 11, "msg": "Invalid parameter"}
            await ws.send(json.dumps({"id": cmd["id"], "type": "error", "msg": err}))
            await ws.wait_closed()

    async with fake_server(handler) as url, asyncio.timeout(5), make_ws(url) as ws:
        await ws.subscribe("trade")
        event = await anext(ws.messages())
        subs = ws.subscriptions
    assert event.msg["resubscribed"] == []
    assert event.msg["failed"][0]["channel"] == "trade"
    assert "Invalid parameter" in event.msg["failed"][0]["error"]
    assert subs == []


async def test_unsubscribed_channels_are_not_resubscribed() -> None:
    second_connection: list[dict[str, Any]] = []
    connections = 0

    async def handler(ws: ServerConnection) -> None:
        nonlocal connections
        connections += 1
        if connections == 1:
            sid = 0
            async for raw in ws:
                cmd = json.loads(raw)
                if cmd["cmd"] == "subscribe":
                    sid += 1
                    await reply_subscribed(ws, cmd, sid)
                else:
                    await ws.send(json.dumps({"id": cmd["id"], "type": "unsubscribed", "sid": 1}))
                    await ws.close()
        else:
            async for raw in ws:
                cmd = json.loads(raw)
                second_connection.append(cmd)
                await reply_subscribed(ws, cmd, 10 + len(second_connection))

    async with fake_server(handler) as url, asyncio.timeout(5), make_ws(url) as ws:
        first = await ws.subscribe("trade")
        await ws.subscribe("ticker")
        await ws.unsubscribe(first)
        event = await anext(ws.messages())
    assert [c["params"]["channels"] for c in second_connection] == [["ticker"]]
    assert [r["channel"] for r in event.msg["resubscribed"]] == ["ticker"]


async def test_drop_during_resubscribe_discards_held_data_and_retries() -> None:
    connections = 0

    async def handler(ws: ServerConnection) -> None:
        nonlocal connections
        connections += 1
        mine = connections
        if mine == 1:
            for sid in (1, 2):
                await reply_subscribed(ws, json.loads(await ws.recv()), sid)
            await ws.close()
        elif mine == 2:
            await reply_subscribed(ws, json.loads(await ws.recv()), 5)
            await ws.send(trade(5, 1))  # data from a half-restored connection
            await ws.close()  # drops before the second resubscribe
        else:
            for sid in (7, 8):
                await reply_subscribed(ws, json.loads(await ws.recv()), sid)
            await ws.wait_closed()

    async with fake_server(handler) as url, asyncio.timeout(5), make_ws(url) as ws:
        await ws.subscribe("trade")
        await ws.subscribe("ticker")
        stream = ws.messages()
        event = await anext(stream)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(anext(stream), 0.2)  # held trade was discarded
        subs = ws.subscriptions
    assert event.type == RECONNECTED
    assert [(r["channel"], r["sid"]) for r in event.msg["resubscribed"]] == [
        ("trade", 7),
        ("ticker", 8),
    ]
    assert [(sid, channel) for sid, channel, _ in subs] == [(7, "trade"), (8, "ticker")]
    assert connections == 3
