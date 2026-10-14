"""The live feed behind the browser: routing, coalescing, restarts, link state, limits."""

import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import pytest
from kalshiterm_client.errors import ConnectionProblem
from kalshiterm_client.models import BookLevel
from kalshiterm_client.stream_events import (
    AlertEvent,
    BookDeltaEvent,
    BookSnapshotEvent,
    CaughtUp,
    Connected,
    Disconnected,
    Resync,
    StreamEvent,
    TickerEvent,
    TradeEvent,
)
from kalshiterm_client.ui.stream_hub import Connection, StreamHub

NOW = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)


class FakeStream:
    """Yields what the test pushes; ``fail`` makes it raise instead (a fatal stream error)."""

    def __init__(self, channels: Sequence[str], tickers: Sequence[str]) -> None:
        self.channels, self.tickers = tuple(channels), tuple(tickers)
        self.queue: asyncio.Queue[Any] = asyncio.Queue()
        self.closed = False

    def push(self, *events: StreamEvent | Exception) -> None:
        for event in events:
            self.queue.put_nowait(event)

    def __aiter__(self) -> AsyncIterator[StreamEvent]:
        return self._events()

    async def _events(self) -> AsyncIterator[StreamEvent]:
        while True:
            item = await self.queue.get()
            if isinstance(item, Exception):
                raise item
            yield item

    async def aclose(self) -> None:
        self.closed = True


class Factory:
    def __init__(self) -> None:
        self.streams: list[FakeStream] = []

    def __call__(self, channels: Sequence[str], tickers: Sequence[str]) -> FakeStream:
        stream = FakeStream(channels, tickers)
        self.streams.append(stream)
        return stream

    @property
    def current(self) -> FakeStream:
        return self.streams[-1]


def hub_and_factory(**kw: Any) -> tuple[StreamHub, Factory]:
    factory = Factory()
    hub = StreamHub(factory, flush_interval=0.01, restart_delay=0.01, **kw)
    return hub, factory


async def settle(seconds: float = 0.08) -> None:
    await asyncio.sleep(seconds)


def drain(conn: Connection) -> list[dict[str, Any]]:
    out = []
    while not conn.queue.empty():
        out.append(conn.queue.get_nowait())
    return out


def types(messages: list[dict[str, Any]]) -> list[str]:
    return [msg["type"] for msg in messages]


def ticker(symbol: str, cursor: int = 1, price: str = "0.56") -> TickerEvent:
    return TickerEvent(ticker=symbol, time=NOW, cursor=cursor, price=Decimal(price))


def trade(symbol: str, cursor: int = 2) -> TradeEvent:
    return TradeEvent(
        ticker=symbol,
        time=NOW,
        cursor=cursor,
        trade_id=UUID("0b3f1a72-2c1f-4f5e-9b6d-5a0e8c1d7e11"),
        yes_price=Decimal("0.56"),
        no_price=Decimal("0.44"),
        count=Decimal("2.5"),
        is_block_trade=False,
    )


def snapshot(
    symbol: str, yes: dict[str, str], no: dict[str, str], seq: int = 1
) -> BookSnapshotEvent:
    return BookSnapshotEvent(
        ticker=symbol, time=NOW, sid=1, seq=seq,
        yes=[BookLevel(price=Decimal(p), size=Decimal(s)) for p, s in yes.items()],
        no=[BookLevel(price=Decimal(p), size=Decimal(s)) for p, s in no.items()],
    )  # fmt: skip


def delta(symbol: str, side: str, price: str, change: str, seq: int) -> BookDeltaEvent:
    return BookDeltaEvent(
        ticker=symbol, time=NOW, sid=1, seq=seq, side=side,  # type: ignore[arg-type]
        price=Decimal(price), delta=Decimal(change),
    )  # fmt: skip


def connected(
    unknown: Sequence[str] = (), no_book: Sequence[str] = (), resumed: bool = False
) -> Connected:
    return Connected(
        resumed=resumed, markets=[], unknown=list(unknown), no_orderbook=list(no_book), cursor=7
    )


# --- what gets streamed -------------------------------------------------------------------------


async def test_one_stream_is_opened_for_the_union_of_what_every_page_wants() -> None:
    hub, factory = hub_and_factory()
    a, b = Connection(), Connection()
    hub.attach(a)
    hub.attach(b)
    hub.subscribe(a, ["ticker"], ["T1", "T2"])
    hub.subscribe(b, ["trades", "book"], ["T2", "T3"])
    await settle()
    assert len(factory.streams) == 1  # several quick changes -> one (re)start
    assert factory.current.channels == ("alerts", "orderbook", "ticker", "trades")
    assert factory.current.tickers == ("T1", "T2", "T3")
    await hub.close()


async def test_each_page_gets_only_what_it_asked_for() -> None:
    hub, factory = hub_and_factory()
    a, b = Connection(), Connection()
    hub.attach(a)
    hub.attach(b)
    hub.subscribe(a, ["ticker"], ["T1"])
    hub.subscribe(b, ["trades"], ["T2"])
    await settle()
    factory.current.push(ticker("T1"), ticker("T2"), trade("T1"), trade("T2"))
    await settle()
    got_a, got_b = drain(a), drain(b)
    assert [(x["type"], x["ticker"]) for x in got_a if x["type"] in ("ticker", "trade")] == [
        ("ticker", "T1")
    ]
    assert [(x["type"], x["ticker"]) for x in got_b if x["type"] in ("ticker", "trade")] == [
        ("trade", "T2")
    ]
    await hub.close()


async def test_prices_cross_as_exact_strings() -> None:
    hub, factory = hub_and_factory()
    conn = Connection()
    hub.attach(conn)
    hub.subscribe(conn, ["ticker", "trades"], ["T1"])
    await settle()
    factory.current.push(ticker("T1", price="0.560000"), trade("T1"))
    await settle()
    got = {x["type"]: x for x in drain(conn) if x["type"] in ("ticker", "trade")}
    assert got["ticker"]["price"] == "0.560000" and got["trade"]["count"] == "2.5"
    assert got["trade"]["yes_price"] == "0.56"
    await hub.close()


async def test_adding_a_market_restarts_the_stream_with_the_new_union_and_closes_the_old() -> None:
    hub, factory = hub_and_factory()
    conn = Connection()
    hub.attach(conn)
    hub.subscribe(conn, ["ticker"], ["T1"])
    await settle()
    first = factory.current
    hub.subscribe(conn, ["ticker"], ["T2"])
    await settle()
    assert len(factory.streams) == 2 and first.closed
    assert factory.current.tickers == ("T1", "T2")
    await hub.close()


async def test_a_late_page_gets_the_current_state_of_a_market_already_streaming() -> None:
    hub, factory = hub_and_factory()
    early = Connection()
    hub.attach(early)
    hub.subscribe(early, ["ticker", "book"], ["T1"])
    await settle()
    factory.current.push(
        connected(), ticker("T1", price="0.61"), snapshot("T1", {"0.55": "10"}, {"0.43": "7"})
    )
    await settle()
    late = Connection()
    hub.attach(late)
    hub.subscribe(late, ["ticker", "book"], ["T1"])
    got = drain(late)
    assert [x["type"] for x in got if x["type"] in ("ticker", "book")] == ["book", "ticker"]
    book = next(x for x in got if x["type"] == "book")
    assert book["yes"] == [["0.55", "10"]] and book["no"] == [["0.43", "7"]]
    await settle()
    assert len(factory.streams) == 1  # no restart was needed
    await hub.close()


async def test_nothing_is_streamed_when_nobody_wants_anything() -> None:
    hub, factory = hub_and_factory()
    conn = Connection()
    hub.attach(conn)
    conn.alerts = False
    hub.set_alerts(conn, False)
    await settle()
    assert factory.streams == []
    hub.set_alerts(conn, True)
    await settle()
    assert factory.current.channels == ("alerts",) and factory.current.tickers == ()
    hub.detach(conn)
    await settle()
    assert factory.current.closed
    await hub.close()


async def test_unsubscribing_stops_the_events_and_shrinks_the_stream() -> None:
    hub, factory = hub_and_factory()
    conn = Connection()
    hub.attach(conn)
    hub.subscribe(conn, ["ticker"], ["T1", "T2"])
    await settle()
    hub.unsubscribe(conn, ["ticker"], ["T2"])
    await settle()
    assert factory.current.tickers == ("T1",)
    factory.current.push(ticker("T2"))
    await settle()
    assert not [x for x in drain(conn) if x["type"] == "ticker"]
    await hub.close()


# --- order books --------------------------------------------------------------------------------


async def test_a_burst_of_book_changes_reaches_the_page_as_one_whole_book() -> None:
    hub, factory = hub_and_factory()
    conn = Connection()
    hub.attach(conn)
    hub.subscribe(conn, ["book"], ["T1"])
    await settle()
    factory.current.push(
        snapshot("T1", {"0.50": "10", "0.55": "4"}, {"0.40": "3"}),
        delta("T1", "yes", "0.55", "6", 2),
        delta("T1", "yes", "0.52", "2", 3),
        delta("T1", "no", "0.40", "-3", 4),
    )
    await settle()
    books = [x for x in drain(conn) if x["type"] == "book"]
    assert 1 <= len(books) <= 2  # coalesced, not four
    last = books[-1]
    assert last["yes"] == [["0.55", "10"], ["0.52", "2"], ["0.50", "10"]]  # best price first
    assert last["no"] == []
    await hub.close()


async def test_books_are_capped_to_the_configured_depth() -> None:
    hub, factory = hub_and_factory(depth=3)
    conn = Connection()
    hub.attach(conn)
    hub.subscribe(conn, ["book"], ["T1"])
    await settle()
    factory.current.push(snapshot("T1", {f"0.{n:02d}": "1" for n in range(10, 30)}, {}))
    await settle()
    book = [x for x in drain(conn) if x["type"] == "book"][-1]
    assert [level[0] for level in book["yes"]] == ["0.29", "0.28", "0.27"]
    await hub.close()


async def test_a_resync_clears_the_books_and_tells_the_page() -> None:
    hub, factory = hub_and_factory()
    conn = Connection()
    hub.attach(conn)
    hub.subscribe(conn, ["book"], ["T1"])
    await settle()
    factory.current.push(snapshot("T1", {"0.50": "1"}, {}))
    await settle()
    drain(conn)
    factory.current.push(Resync(reason="cursor_too_old", missed_after=3, cursor=9))
    await settle()
    assert [x for x in drain(conn) if x["type"] == "resync"][0]["reason"] == "cursor_too_old"
    late = Connection()
    hub.attach(late)
    hub.subscribe(late, ["book"], ["T1"])
    assert not [x for x in drain(late) if x["type"] == "book"]  # forgotten until a fresh snapshot
    factory.current.push(snapshot("T1", {"0.60": "2"}, {}, seq=1))
    await settle()
    assert [x for x in drain(late) if x["type"] == "book"][-1]["yes"] == [["0.60", "2"]]
    await hub.close()


# --- alerts and link state ---------------------------------------------------------------------


async def test_alerts_go_to_pages_that_want_them() -> None:
    hub, factory = hub_and_factory()
    wants, declines = Connection(), Connection()
    hub.attach(wants)
    hub.attach(declines)
    hub.set_alerts(declines, False)
    await settle()
    factory.current.push(
        AlertEvent(
            alert_cursor=1,
            id=9,
            rule_name="r",
            fired_at=NOW,
            metric="spread",
            op=">",
            threshold=Decimal("0.1"),
            value=Decimal("0.12"),
            ticker="T1",
        )  # fmt: skip
    )
    await settle()
    assert [x for x in drain(wants) if x["type"] == "alert"][0]["value"] == "0.12"
    assert not [x for x in drain(declines) if x["type"] == "alert"]
    await hub.close()


async def test_the_page_is_told_whether_the_link_is_up_down_or_failed() -> None:
    hub, factory = hub_and_factory()
    conn = Connection()
    hub.attach(conn)
    hub.subscribe(conn, ["ticker"], ["T1"])
    await settle()
    factory.current.push(
        connected(unknown=["T9"], no_book=["T1"]), CaughtUp(stream="market", cursor=7)
    )
    await settle()
    got = drain(conn)
    assert {"type": "link", "state": "up", "retry_in": None, "error": None} in got
    subscribed = next(x for x in got if x["type"] == "subscribed")
    assert subscribed["no_orderbook"] == ["T1"] and subscribed["unknown"] == []  # only my markets
    factory.current.push(Disconnected(reason="idle", retry_in=2.5))
    await settle()
    assert [x for x in drain(conn) if x["type"] == "link"][-1]["retry_in"] == 2.5
    late = Connection()
    hub.attach(late)
    assert drain(late)[0]["type"] == "link"  # a new page learns the current state at once
    await hub.close()


async def test_a_fatal_stream_error_is_reported_and_not_retried_in_a_loop() -> None:
    hub, factory = hub_and_factory()
    conn = Connection()
    hub.attach(conn)
    hub.subscribe(conn, ["ticker"], ["T1"])
    await settle()
    factory.current.push(ConnectionProblem("the server's certificate changed"))
    await settle(0.2)
    link = [x for x in drain(conn) if x["type"] == "link"][-1]
    assert link["state"] == "failed" and "certificate" in link["error"]
    assert len(factory.streams) == 1 and factory.streams[0].closed  # no hot restart loop
    hub.subscribe(conn, ["ticker"], ["T2"])  # a changed request is a new attempt
    await settle()
    assert len(factory.streams) == 2
    factory.current.push(ConnectionProblem("still broken"))
    await settle(0.2)
    reloaded = Connection()  # the user reloads the page: the same request, asked again
    hub.attach(reloaded)
    hub.subscribe(reloaded, ["ticker"], ["T1", "T2"])
    await settle()
    assert len(factory.streams) == 3  # exactly one retry for the new request
    await settle(0.2)
    assert len(factory.streams) == 3  # and none on its own
    await hub.close()


async def test_an_unexpected_crash_is_reported_without_details_and_without_a_loop() -> None:
    hub, factory = hub_and_factory()
    conn = Connection()
    hub.attach(conn)
    hub.subscribe(conn, ["ticker"], ["T1"])
    await settle()
    factory.current.push(RuntimeError("secret internal detail"))
    await settle(0.2)
    link = [x for x in drain(conn) if x["type"] == "link"][-1]
    assert link["state"] == "failed" and "secret" not in str(link)
    assert len(factory.streams) == 1
    await hub.close()


# --- limits -------------------------------------------------------------------------------------


async def test_a_page_that_cannot_keep_up_is_dropped_and_others_are_unaffected() -> None:
    hub, factory = hub_and_factory()
    slow, fast = Connection(limit=3), Connection()
    hub.attach(slow)
    hub.attach(fast)
    hub.subscribe(slow, ["ticker"], ["T1"])
    hub.subscribe(fast, ["ticker"], ["T1"])
    await settle()
    factory.current.push(*(ticker("T1", cursor=n) for n in range(1, 21)))
    await settle()
    assert slow.overflowed and slow.queue.qsize() <= 3  # bounded
    assert not fast.overflowed and len([x for x in drain(fast) if x["type"] == "ticker"]) == 20
    await hub.close()


async def test_the_limits_per_page_and_across_the_ui() -> None:
    hub, _ = hub_and_factory()
    a, b, c = Connection(), Connection(), Connection()
    for conn in (a, b, c):
        hub.attach(conn)
    with pytest.raises(ValueError, match="per connection"):
        hub.subscribe(a, ["ticker"], [f"T{n}" for n in range(101)])
    hub.subscribe(a, ["ticker"], [f"A{n}" for n in range(100)])
    hub.subscribe(b, ["ticker"], [f"B{n}" for n in range(100)])  # 200 in all: the limit
    hub.subscribe(c, ["ticker"], ["A0"])  # a market someone already streams adds nothing
    with pytest.raises(ValueError, match="across the UI"):
        hub.subscribe(c, ["ticker"], ["one-more"])
    await hub.close()


async def test_without_a_server_a_subscription_is_refused_clearly() -> None:
    hub = StreamHub(None)
    conn = Connection()
    hub.attach(conn)
    with pytest.raises(ValueError, match="not configured"):
        hub.subscribe(conn, ["ticker"], ["T1"])
    await hub.close()


async def test_closing_stops_the_stream_and_the_hubs_own_tasks() -> None:
    hub, factory = hub_and_factory()
    conn = Connection()
    hub.attach(conn)
    hub.subscribe(conn, ["ticker"], ["T1"])
    await settle()
    tasks = list(hub._tasks)
    await hub.close()
    assert factory.current.closed and hub._consumer is None and hub._tasks == []
    assert all(task.done() for task in tasks)


# --- through the real app and WebSocket ---------------------------------------------------------

PORT = 51236


class ScriptedStream:
    """A finite script, then silence; safe to build inside the app's event loop."""

    def __init__(self, events: Sequence[StreamEvent]) -> None:
        self.events = list(events)

    def __aiter__(self) -> AsyncIterator[StreamEvent]:
        return self._run()

    async def _run(self) -> AsyncIterator[StreamEvent]:
        await asyncio.sleep(0.05)  # let the page finish subscribing first
        for event in self.events:
            yield event
            await asyncio.sleep(0.001)
        await asyncio.Event().wait()

    async def aclose(self) -> None:
        pass


def browser_socket(events: Sequence[StreamEvent] | None) -> Any:
    from kalshiterm_client.ui.app import create_app
    from starlette.testclient import TestClient

    factory = None if events is None else (lambda channels, tickers: ScriptedStream(events))
    app = create_app(
        port=PORT, token="tok", environment="demo", assets_dir=None, stream_factory=factory
    )
    client = TestClient(app, base_url=f"http://127.0.0.1:{PORT}")
    return client.websocket_connect(
        "/api/ws", headers={"host": f"127.0.0.1:{PORT}", "origin": f"http://127.0.0.1:{PORT}"}
    )


def receive_until(ws: Any, wanted: str, limit: int = 30) -> list[dict[str, Any]]:
    seen = []
    for _ in range(limit):
        message = ws.receive_json()
        seen.append(message)
        if message["type"] == wanted:
            return seen
    raise AssertionError(f"never saw {wanted}: {[x['type'] for x in seen]}")


def open_session(ws: Any) -> None:
    ws.send_json({"type": "auth", "token": "tok"})
    assert ws.receive_json()["type"] == "hello"


def test_a_page_subscribes_and_receives_live_events_through_the_websocket() -> None:
    script: list[StreamEvent] = [connected(), ticker("T1", price="0.57"), trade("T1")]
    with browser_socket(script) as ws:
        open_session(ws)
        ws.send_json({"type": "subscribe", "channels": ["ticker", "trades"], "tickers": ["T1"]})
        got = receive_until(ws, "trade")
        assert any(x["type"] == "ticker" and x["price"] == "0.57" for x in got)
        assert any(x["type"] == "link" and x["state"] == "up" for x in got)


@pytest.mark.parametrize(
    "bad",
    [
        {"type": "subscribe", "channels": ["quotes"], "tickers": ["T1"]},
        {"type": "subscribe", "channels": ["ticker"], "tickers": []},
        {"type": "subscribe", "channels": [], "tickers": ["T1"]},
        {"type": "subscribe", "channels": ["ticker"], "tickers": ["bad ticker!"]},
        {"type": "subscribe", "channels": ["ticker"], "tickers": ["x" * 129]},
        {"type": "subscribe", "channels": ["ticker"], "tickers": ["T1"], "extra": 1},
        {"type": "teleport"},
        {"type": "auth", "token": "tok"},
        [],
        "text",
        {"channels": ["ticker"]},
    ],
)
def test_a_malformed_message_gets_an_error_and_the_connection_survives(bad: object) -> None:
    with browser_socket([]) as ws:
        open_session(ws)
        ws.send_json(bad)
        assert ws.receive_json()["type"] == "error"
        ws.send_json({"type": "ping"})
        assert ws.receive_json() == {"type": "pong"}


def test_without_a_server_the_page_is_told_and_the_session_carries_on() -> None:
    with browser_socket(None) as ws:
        open_session(ws)
        ws.send_json({"type": "subscribe", "channels": ["ticker"], "tickers": ["T1"]})
        message = ws.receive_json()
        assert message["type"] == "error" and "not configured" in message["error"]
        ws.send_json({"type": "ping"})
        assert ws.receive_json() == {"type": "pong"}


class StalledSocket:
    """A page that has stopped reading: after ``accepts`` messages every send blocks for good."""

    def __init__(self, accepts: int = 1) -> None:
        self.accepts = accepts
        self.sent: list[Any] = []
        self.closed_with: int | None = None
        self._never = asyncio.Event()

    async def send_json(self, message: Any) -> None:
        if len(self.sent) >= self.accepts:
            await self._never.wait()
        self.sent.append(message)

    async def receive_json(self) -> Any:
        await self._never.wait()  # the page sends nothing

    async def close(self, code: int = 1000) -> None:
        self.closed_with = code


async def test_a_page_that_stops_reading_is_dropped_with_1013_even_though_its_send_is_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kalshiterm_client.ui import stream_hub
    from kalshiterm_client.ui.app import serve_connection

    monkeypatch.setattr(stream_hub, "QUEUE_LIMIT", 5)
    hub, factory = hub_and_factory()
    socket = StalledSocket()
    serving = asyncio.create_task(serve_connection(socket, hub))  # type: ignore[arg-type]
    await settle()
    conn = next(iter(hub._connections))
    hub.subscribe(conn, ["ticker"], ["T1"])
    await settle()
    factory.current.push(*(ticker("T1", cursor=n) for n in range(1, 60)))
    await asyncio.wait_for(serving, timeout=3)  # it ends by itself: no waiting on the dead socket
    assert socket.closed_with == 1013
    assert conn.queue.qsize() <= 5  # memory stayed bounded
    assert not hub._connections  # and the hub let go of it
    await hub.close()


async def test_a_page_that_leaves_is_detached_and_its_stream_released() -> None:
    from kalshiterm_client.ui.app import serve_connection

    class Leaving(StalledSocket):
        async def receive_json(self) -> Any:
            await asyncio.sleep(0.05)
            from starlette.websockets import WebSocketDisconnect

            raise WebSocketDisconnect(1001)

    hub, _ = hub_and_factory()
    socket = Leaving(accepts=100)
    await asyncio.wait_for(serve_connection(socket, hub), timeout=3)  # type: ignore[arg-type]
    assert not hub._connections and socket.closed_with is None
    await hub.close()


def test_the_session_is_read_only_and_says_so() -> None:
    with browser_socket([]) as ws:
        ws.send_json({"type": "auth", "token": "tok"})
        assert ws.receive_json() == {"type": "hello", "mode": "read-only", "environment": "demo"}
