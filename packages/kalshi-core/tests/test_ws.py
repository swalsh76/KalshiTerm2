import asyncio
import base64
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from kalshi_core.auth import HEADER_KEY, HEADER_SIGNATURE, HEADER_TIMESTAMP, KalshiSigner
from kalshi_core.config import KalshiSettings
from kalshi_core.ws import KalshiWebSocket, KalshiWSError
from kalshi_core.ws_models import LifecycleMsg, TickerMsg, TradeMsg
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


def make_ws(
    url: str, key: Ed25519PrivateKey | None = None, command_timeout: float = 10.0
) -> KalshiWebSocket:
    signer = KalshiSigner("kid", key or Ed25519PrivateKey.generate())
    return KalshiWebSocket(KalshiSettings(), signer, url=url, command_timeout=command_timeout)


def reply_subscribed(command: dict[str, object], sid: int) -> str:
    channels = command["params"]["channels"]  # type: ignore[index]
    return json.dumps(
        {"id": command["id"], "type": "subscribed", "msg": {"channel": channels[0], "sid": sid}}
    )


async def test_handshake_is_signed_over_ws_path() -> None:
    key = Ed25519PrivateKey.generate()
    seen: dict[str, str] = {}

    async def handler(ws: ServerConnection) -> None:
        assert ws.request is not None
        for name in (HEADER_KEY, HEADER_SIGNATURE, HEADER_TIMESTAMP):
            seen[name] = ws.request.headers[name]
        await ws.wait_closed()

    async with fake_server(handler) as url:
        async with asyncio.timeout(5):
            async with make_ws(url, key):
                await asyncio.sleep(0.05)
    assert seen[HEADER_KEY] == "kid"
    key.public_key().verify(
        base64.b64decode(seen[HEADER_SIGNATURE]),
        f"{seen[HEADER_TIMESTAMP]}GET/trade-api/ws/v2".encode(),
    )


async def test_subscribe_and_unsubscribe_send_commands_and_return_sid() -> None:
    received: list[dict[str, object]] = []

    async def handler(ws: ServerConnection) -> None:
        async for raw in ws:
            cmd = json.loads(raw)
            received.append(cmd)
            if cmd["cmd"] == "subscribe":
                await ws.send(reply_subscribed(cmd, 42))
            else:
                await ws.send(
                    json.dumps({"id": cmd["id"], "sid": 42, "seq": 1, "type": "unsubscribed"})
                )

    async with fake_server(handler) as url, asyncio.timeout(5), make_ws(url) as ws:
        sid = await ws.subscribe("trade", market_tickers=["A", "B"])
        await ws.unsubscribe(sid)
    assert sid == 42
    assert received[0] == {
        "id": 1,
        "cmd": "subscribe",
        "params": {"channels": ["trade"], "market_tickers": ["A", "B"]},
    }
    assert received[1] == {"id": 2, "cmd": "unsubscribe", "params": {"sids": [42]}}


async def test_error_response_raises_with_code() -> None:
    async def handler(ws: ServerConnection) -> None:
        async for raw in ws:
            cmd = json.loads(raw)
            await ws.send(
                json.dumps(
                    {
                        "id": cmd["id"],
                        "type": "error",
                        "msg": {"code": 8, "msg": "Unknown channel name"},
                    }
                )
            )

    async with fake_server(handler) as url, asyncio.timeout(5), make_ws(url) as ws:
        with pytest.raises(KalshiWSError) as exc:
            await ws.subscribe("nope")
    assert exc.value.code == 8
    assert "Unknown channel" in str(exc.value)


async def test_data_messages_are_streamed_and_typed() -> None:
    async def handler(ws: ServerConnection) -> None:
        cmd = json.loads(await ws.recv())
        await ws.send(reply_subscribed(cmd, 1))
        await ws.send(
            json.dumps(
                {
                    "type": "trade",
                    "sid": 1,
                    "seq": 1,
                    "msg": {
                        "trade_id": "t1",
                        "market_ticker": "M",
                        "yes_price_dollars": "0.5600",
                        "no_price_dollars": "0.4400",
                        "count_fp": "3.00",
                        "taker_side": "yes",
                        "extra": 1,
                    },
                }
            )
        )
        ticker = {"market_ticker": "M", "price_dollars": "0.5"}
        await ws.send(json.dumps({"type": "ticker", "sid": 2, "msg": ticker}))
        life = {"market_ticker": "M", "event_type": "settled"}
        await ws.send(json.dumps({"type": "market_lifecycle_v2", "sid": 3, "seq": 1, "msg": life}))
        await ws.wait_closed()

    async with fake_server(handler) as url, asyncio.timeout(5), make_ws(url) as ws:
        await ws.subscribe("trade")
        stream = ws.messages()
        trade = await anext(stream)
        ticker = await anext(stream)
        life = await anext(stream)
    t = trade.payload()
    assert isinstance(t, TradeMsg) and t.yes_price_dollars == Decimal("0.5600")
    assert trade.sid == 1 and trade.seq == 1
    assert isinstance(ticker.payload(), TickerMsg)
    p = life.payload()
    assert isinstance(p, LifecycleMsg) and p.event_type == "settled"


async def test_command_times_out_when_server_is_silent() -> None:
    async def handler(ws: ServerConnection) -> None:
        await ws.wait_closed()

    async with (
        fake_server(handler) as url,
        asyncio.timeout(5),
        make_ws(url, command_timeout=0.2) as ws,
    ):
        with pytest.raises(KalshiWSError, match="timed out"):
            await ws.subscribe("trade")


async def test_dropped_connection_raises_not_ends() -> None:
    async def handler(ws: ServerConnection) -> None:
        await ws.recv()
        await ws.close()

    async with fake_server(handler) as url, asyncio.timeout(5):
        ws = make_ws(url)
        await ws.connect()
        with pytest.raises(KalshiWSError, match="connection closed"):
            await ws.subscribe("trade")  # server closes instead of replying
        with pytest.raises(KalshiWSError, match="connection closed"):
            await anext(ws.messages())
        await ws.close()


async def test_deliberate_close_ends_stream_cleanly() -> None:
    async def handler(ws: ServerConnection) -> None:
        await ws.wait_closed()

    async with fake_server(handler) as url, asyncio.timeout(5):
        ws = make_ws(url)
        await ws.connect()
        await ws.close()
        assert [m async for m in ws.messages()] == []


async def test_client_answers_server_pings() -> None:
    ponged = asyncio.Event()

    async def handler(ws: ServerConnection) -> None:
        pong = await ws.ping(b"heartbeat")
        await asyncio.wait_for(pong, 2)
        ponged.set()
        await ws.wait_closed()

    async with fake_server(handler) as url, asyncio.timeout(5), make_ws(url):
        await asyncio.wait_for(ponged.wait(), 3)
