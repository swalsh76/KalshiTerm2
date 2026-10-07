"""Live add/remove of markets on an orderbook subscription, against a fake that behaves like
the real service did when probed: replies consume a sequence number, added markets get no
automatic snapshot, ``get_snapshot`` is answered in-stream, replies carry the full market set.
"""

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from decimal import Decimal as D
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from kalshi_core.auth import KalshiSigner
from kalshi_core.config import KalshiSettings
from kalshi_core.orderbook import (
    DELTA,
    GAP,
    SNAPSHOT,
    BookEvent,
    BookTracker,
    OrderBookFeed,
)
from kalshi_core.rest import KalshiRestClient
from kalshi_core.ws import KalshiWebSocket, KalshiWSError
from kalshi_core.ws_models import WsMessage
from websockets.asyncio.server import ServerConnection, serve


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    for name in ("KALSHI_ENV", "KALSHI_KEY_ID", "KALSHI_PRIVATE_KEY_PATH"):
        monkeypatch.delenv(name, raising=False)


class FakeBookServer:
    """One orderbook_delta subscription with a single per-subscription sequence counter."""

    def __init__(self, tickers_by_connection: list[list[str]] | None = None) -> None:
        self.sid = 1
        self.seq = 0
        self.tickers: list[str] = []
        self.commands: list[dict[str, Any]] = []
        self.ws: ServerConnection | None = None
        self.connections = 0
        self.resubscribed_with: list[list[str]] = []

    def _next(self) -> int:
        self.seq += 1
        return self.seq

    async def snapshot(self, ticker: str, cmd_id: int | None = None, qty: str = "10.00") -> None:
        assert self.ws is not None
        msg = {"market_ticker": ticker, "yes_dollars_fp": [["0.40", qty]], "no_dollars_fp": []}
        body: dict[str, Any] = {"type": "orderbook_snapshot", "sid": self.sid, "seq": self._next()}
        body["msg"] = msg
        if cmd_id is not None:
            body["id"] = cmd_id
        await self.ws.send(json.dumps(body))

    async def delta(self, ticker: str, change: str = "1.00") -> None:
        assert self.ws is not None
        msg = {"market_ticker": ticker, "side": "yes", "price_dollars": "0.40", "delta_fp": change}
        frame = {"type": "orderbook_delta", "sid": self.sid, "seq": self._next(), "msg": msg}
        await self.ws.send(json.dumps(frame))

    async def handler(self, ws: ServerConnection) -> None:
        self.ws = ws
        self.connections += 1
        self.seq = 0
        async for raw in ws:
            cmd = json.loads(raw)
            self.commands.append(cmd)
            params = cmd.get("params", {})
            if cmd["cmd"] == "subscribe":
                if self.commands[:-1] and any(c["cmd"] == "subscribe" for c in self.commands[:-1]):
                    self.sid += 1  # a new subscription: new id, its own sequence from 1
                    self.seq = 0
                self.tickers = list(params.get("market_tickers", []))
                if self.connections > 1:
                    self.resubscribed_with.append(list(self.tickers))
                reply = {"id": cmd["id"], "type": "subscribed"}
                reply["msg"] = {"channel": "orderbook_delta", "sid": self.sid}
                await ws.send(json.dumps(reply))
                for ticker in self.tickers:
                    await self.snapshot(ticker)
            elif cmd["cmd"] == "update_subscription":
                action = params["action"]
                if action == "get_snapshot":
                    await self.snapshot(params["market_tickers"][0], cmd["id"])
                    continue
                if action == "add_markets":
                    self.tickers += [t for t in params["market_tickers"] if t not in self.tickers]
                else:
                    self.tickers = [t for t in self.tickers if t not in params["market_tickers"]]
                ok = {"id": cmd["id"], "type": "ok", "sid": self.sid, "seq": self._next()}
                ok["msg"] = {"market_tickers": list(self.tickers)}
                await ws.send(json.dumps(ok))
            elif cmd["cmd"] == "unsubscribe":
                reply = {"id": cmd["id"], "type": "unsubscribed", "sid": self.sid}
                reply["seq"] = self._next()
                await ws.send(json.dumps(reply))
                self.tickers = []


@asynccontextmanager
async def running(server: FakeBookServer) -> AsyncIterator[str]:
    async with serve(server.handler, "127.0.0.1", 0) as s:
        yield f"ws://127.0.0.1:{s.sockets[0].getsockname()[1]}/trade-api/ws/v2"


def make(url: str, tickers: list[str], **ws_kw: Any) -> tuple[KalshiWebSocket, OrderBookFeed]:
    signer = KalshiSigner("kid", Ed25519PrivateKey.generate())
    ws = KalshiWebSocket(KalshiSettings(), signer, url=url, reconnect_base=0.01, **ws_kw)
    feed = OrderBookFeed(ws, KalshiRestClient(KalshiSettings()), tickers, snapshot_timeout=2)
    return ws, feed


async def collect(feed: OrderBookFeed, kinds: int, seconds: float = 3.0) -> list[BookEvent]:
    out: list[BookEvent] = []
    stream = feed.events()
    async with asyncio.timeout(seconds):
        while len(out) < kinds:
            out.append(await anext(stream))
    return out


# ------------------------------------------------------------------ websocket client
async def test_update_subscription_returns_the_full_set_and_updates_the_reconnect_registry() -> (
    None
):
    server = FakeBookServer()
    async with running(server) as url, asyncio.timeout(10):
        ws, _ = make(url, [])
        async with ws:
            sid = await ws.subscribe("orderbook_delta", market_tickers=["A", "B"])
            added = await ws.update_subscription(sid, "add_markets", ["C"])
            assert added == ["A", "B", "C"]
            assert ws.subscriptions[0][2]["market_tickers"] == ["A", "B", "C"]
            removed = await ws.update_subscription(sid, "delete_markets", ["A"])
            assert removed == ["B", "C"]
            assert ws.subscriptions[0][2]["market_tickers"] == ["B", "C"]


async def test_replies_that_consume_a_sequence_number_appear_as_ordered_control_markers() -> None:
    server = FakeBookServer()
    async with running(server) as url, asyncio.timeout(10):
        ws, _ = make(url, [])
        async with ws:
            sid = await ws.subscribe("orderbook_delta", market_tickers=["A"])
            await ws.update_subscription(sid, "add_markets", ["B"])
            await server.delta("A")
            stream = ws.messages()
            seen = [await anext(stream) for _ in range(3)]
    assert [(m.type, m.seq) for m in seen] == [
        ("orderbook_snapshot", 1),
        ("control", 2),  # the add_markets reply, in order between the snapshot and the delta
        ("orderbook_delta", 3),
    ]


async def test_an_update_that_gets_no_market_set_back_is_an_error() -> None:
    async def handler(ws: ServerConnection) -> None:
        async for raw in ws:
            cmd = json.loads(raw)
            reply = {"id": cmd["id"], "type": "subscribed", "msg": {"channel": "x", "sid": 1}}
            if cmd["cmd"] == "update_subscription":
                reply = {"id": cmd["id"], "type": "ok", "sid": 1, "seq": 1, "msg": {}}
            await ws.send(json.dumps(reply))

    async with serve(handler, "127.0.0.1", 0) as s, asyncio.timeout(10):
        url = f"ws://127.0.0.1:{s.sockets[0].getsockname()[1]}/trade-api/ws/v2"
        ws, _ = make(url, [])
        async with ws:
            sid = await ws.subscribe("orderbook_delta", market_tickers=["A"])
            with pytest.raises(KalshiWSError, match="no market set"):
                await ws.update_subscription(sid, "add_markets", ["B"])


# ------------------------------------------------------------------ tracker
def snap(seq: int, ticker: str) -> WsMessage:
    msg = {"market_ticker": ticker, "yes_dollars_fp": [["0.40", "1.00"]], "no_dollars_fp": []}
    return WsMessage(type="orderbook_snapshot", sid=1, seq=seq, msg=msg)


def delta(seq: int, ticker: str, change: str = "1.00") -> WsMessage:
    msg = {"market_ticker": ticker, "side": "yes", "price_dollars": "0.40", "delta_fp": change}
    return WsMessage(type="orderbook_delta", sid=1, seq=seq, msg=msg)


def control(seq: int) -> WsMessage:
    return WsMessage(type="control", sid=1, seq=seq)


def test_a_control_marker_in_step_is_not_a_gap_and_out_of_step_is() -> None:
    t = BookTracker()
    t.begin_subscription(1, ["A"])
    t.process(snap(1, "A"))
    assert t.process(control(2)) == []
    assert [e.kind for e in t.process(delta(3, "A"))] == [DELTA]  # no false gap after the ack
    events = t.process(control(9))  # sequence numbers 4-8 went missing
    assert [e.kind for e in events] == [GAP] and events[0].tickers == ("A",)
    assert t.stale == {"A"}


def test_a_control_marker_for_an_unknown_subscription_is_ignored() -> None:
    t = BookTracker()
    assert t.process(WsMessage(type="control", sid=7, seq=3)) == []


def test_snapshot_and_delta_events_carry_their_originating_message() -> None:
    t = BookTracker()
    t.begin_subscription(1, ["A"])
    first, second = snap(1, "A"), delta(2, "A")
    [snapshot_event] = t.process(first)
    [delta_event] = t.process(second)
    assert snapshot_event.message is first and delta_event.message is second


def test_added_markets_are_stale_until_their_snapshot_and_removed_ones_are_forgotten() -> None:
    t = BookTracker()
    t.begin_subscription(1, ["A", "B"])
    t.process(snap(1, "A"))
    t.process(snap(2, "B"))
    t.begin_markets(1, ["C"])
    assert t.stale == {"C"}
    assert t.process(delta(3, "C")) == []  # no snapshot yet: ignored, not applied
    assert [e.kind for e in t.process(snap(4, "C"))] == [SNAPSHOT]
    t.forget_markets(1, ["B"])
    assert "B" not in t.books
    late = t.process(delta(5, "B"))  # a straggler for the dropped market
    assert late == [] and "B" not in t.books and t.stale == set()
    assert [e.kind for e in t.process(delta(6, "A"))] == [DELTA]  # sequence stayed in step


def test_a_gap_after_a_removal_never_resyncs_the_removed_market() -> None:
    t = BookTracker()
    t.begin_subscription(1, ["A", "B"])
    t.process(snap(1, "A"))
    t.process(snap(2, "B"))
    t.forget_markets(1, ["B"])
    events = t.process(delta(9, "A"))  # a real gap
    assert events[0].kind == GAP and events[0].tickers == ("A",)


# ------------------------------------------------------------------ feed against the fake
async def test_adding_a_market_requests_its_snapshot_and_raises_no_false_gap() -> None:
    server = FakeBookServer()
    async with running(server) as url, asyncio.timeout(15):
        ws, feed = make(url, ["A", "B"])
        async with ws, feed:
            first = await collect(feed, 2)  # snapshots of A and B
            await feed.add_markets(["C"])
            assert feed.tickers == ["A", "B", "C"]
            await server.delta("A")
            after = await collect(feed, 2)  # C's requested snapshot, then A's delta
            await server.delta("C", "5.00")
            last = await collect(feed, 1)
            book = feed.tracker.books["C"]
    assert [e.kind for e in first] == [SNAPSHOT, SNAPSHOT]
    kinds = [e.kind for e in after + last]
    assert GAP not in kinds
    assert SNAPSHOT in [e.kind for e in after] and kinds.count(DELTA) == 2
    assert book.yes == {D("0.40"): D("15.00")}
    assert [
        c["params"]["action"] for c in server.commands if c["cmd"] == "update_subscription"
    ] == [
        "add_markets",
        "get_snapshot",
    ]


async def test_removing_a_market_drops_it_and_ignores_stragglers_without_a_gap() -> None:
    server = FakeBookServer()
    async with running(server) as url, asyncio.timeout(15):
        ws, feed = make(url, ["A", "B"])
        async with ws, feed:
            await collect(feed, 2)
            await feed.remove_markets(["B"])
            assert feed.tickers == ["A"] and "B" not in feed.tracker.books
            await server.delta("B")  # in flight when we removed it
            await server.delta("A", "2.00")
            events = await collect(feed, 1)
            await asyncio.sleep(0.1)
            book = feed.tracker.books["A"]
            stale = set(feed.tracker.stale)
    assert [e.kind for e in events] == [DELTA] and events[0].ticker == "A"
    assert book.yes == {D("0.40"): D("12.00")} and stale == set()


async def test_adding_or_removing_nothing_sends_no_command() -> None:
    server = FakeBookServer()
    async with running(server) as url, asyncio.timeout(15):
        ws, feed = make(url, ["A"])
        async with ws, feed:
            await collect(feed, 1)
            before = len(server.commands)
            await feed.add_markets(["A"])  # already watched
            await feed.remove_markets(["Z"])  # never watched
    assert len(server.commands) == before


async def test_removing_the_last_market_unsubscribes_instead_of_sending_an_empty_list() -> None:
    server = FakeBookServer()
    async with running(server) as url, asyncio.timeout(15):
        ws, feed = make(url, ["A"])
        async with ws, feed:
            await collect(feed, 1)
            await feed.remove_markets(["A"])
            assert feed.tickers == []
            assert not ws.subscriptions  # nothing left to resubscribe after a reconnect
            await feed.add_markets(["D"])  # starts a fresh subscription
            events = await collect(feed, 1)
    cmds = [c["cmd"] for c in server.commands]
    assert cmds == ["subscribe", "unsubscribe", "subscribe"]
    assert all(
        c["params"].get("market_tickers") for c in server.commands if c["cmd"] == "subscribe"
    )
    assert events[0].kind == SNAPSHOT and events[0].ticker == "D"


async def test_a_feed_started_with_no_markets_subscribes_to_nothing_until_one_is_added() -> None:
    server = FakeBookServer()
    async with running(server) as url, asyncio.timeout(15):
        ws, feed = make(url, [])
        async with ws, feed:
            await asyncio.sleep(0.1)
            assert server.commands == []  # an empty list would mean "every market" to Kalshi
            await feed.add_markets(["A"])
            events = await collect(feed, 1)
    assert [c["cmd"] for c in server.commands] == ["subscribe"]
    assert server.commands[0]["params"]["market_tickers"] == ["A"]
    assert events[0].kind == SNAPSHOT and events[0].ticker == "A"


async def test_a_reconnect_resubscribes_to_the_updated_market_set() -> None:
    server = FakeBookServer()
    async with running(server) as url, asyncio.timeout(15):
        ws, feed = make(url, ["A", "B"])
        async with ws, feed:
            await collect(feed, 2)
            await feed.add_markets(["C"])
            await feed.remove_markets(["A"])
            assert sorted(feed.tickers) == ["B", "C"]
            assert server.ws is not None
            await server.ws.close()  # the link drops
            events = await collect(
                feed, 4, seconds=8
            )  # snapshot(C) + delta backlog, reset, 2 snaps
    assert server.resubscribed_with == [["B", "C"]]
    assert any(e.kind == "reset" for e in events)


async def test_periodic_snapshots_are_spread_round_robin_and_stop_with_the_feed() -> None:
    server = FakeBookServer()
    async with running(server) as url, asyncio.timeout(15):
        signer = KalshiSigner("kid", Ed25519PrivateKey.generate())
        ws = KalshiWebSocket(KalshiSettings(), signer, url=url, reconnect_base=0.01)
        feed = OrderBookFeed(
            ws, KalshiRestClient(KalshiSettings()), ["A", "B"], periodic_snapshot_interval=0.4
        )
        async with ws, feed:
            await collect(feed, 2)  # the two initial snapshots
            events = await collect(feed, 4, seconds=5)  # periodic ones, one every 0.2 s
            requested = [
                c["params"]["market_tickers"][0]
                for c in server.commands
                if c["cmd"] == "update_subscription"
            ]
        count_at_close = len(server.commands)
        await asyncio.sleep(0.5)
    assert [e.kind for e in events] == [SNAPSHOT] * 4
    assert requested[:4] == ["A", "B", "A", "B"]  # evenly alternating, not a burst
    assert len(server.commands) == count_at_close  # nothing is sent after the feed closed


async def test_periodic_snapshots_follow_the_watch_set_as_it_changes() -> None:
    server = FakeBookServer()
    async with running(server) as url, asyncio.timeout(15):
        signer = KalshiSigner("kid", Ed25519PrivateKey.generate())
        ws = KalshiWebSocket(KalshiSettings(), signer, url=url, reconnect_base=0.01)
        feed = OrderBookFeed(
            ws, KalshiRestClient(KalshiSettings()), ["A", "B"], periodic_snapshot_interval=0.3
        )
        async with ws, feed:
            await collect(feed, 2)
            await feed.remove_markets(["B"])
            server.commands.clear()
            await asyncio.sleep(0.8)
            requested = {
                c["params"]["market_tickers"][0]
                for c in server.commands
                if c["cmd"] == "update_subscription"
            }
    assert requested == {"A"}  # B is no longer snapshotted once removed
