"""StreamClient against a scripted socket: resume cursors, dedup, resync, backoff, fatal errors."""

import asyncio
import json
import ssl
from collections.abc import Callable
from decimal import Decimal
from typing import Any

import pytest
from kalshiterm_client.errors import ConnectionProblem, InvalidRequest, Unauthorized
from kalshiterm_client.stream import StreamClient
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
    UnknownEvent,
    WatchlistChanged,
)
from websockets.exceptions import ConnectionClosed
from websockets.frames import Close

TOKEN = "kt_7_" + "Zy9" * 14
HANG = object()  # a socket that says nothing


def closed(code: int) -> ConnectionClosed:
    return ConnectionClosed(Close(code, ""), None)


class FakeSocket:
    def __init__(self, *frames: Any) -> None:
        self.frames = list(frames)
        self.sent: list[dict[str, Any]] = []
        self.closed = False

    async def recv(self) -> str:
        if not self.frames:
            await asyncio.Event().wait()  # nothing more to say: block until cancelled
        frame = self.frames.pop(0)
        if frame is HANG:
            await asyncio.Event().wait()
        if isinstance(frame, BaseException):
            raise frame
        return frame if isinstance(frame, str) else json.dumps(frame)

    async def send(self, text: str) -> None:
        self.sent.append(json.loads(text))

    async def close(self) -> None:
        self.closed = True


class Script:
    """Hands out one scripted socket per connection attempt and records how it was made."""

    def __init__(self, *connections: Any) -> None:
        self.connections = list(connections)
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.sleeps: list[float] = []

    async def connect(self, url: str, **kw: Any) -> FakeSocket:
        self.calls.append((url, kw))
        assert self.connections, "the script ran out of connections"
        item = self.connections.pop(0)
        if isinstance(item, BaseException):
            raise item
        assert isinstance(item, FakeSocket)
        return item

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)

    def stream(self, **kw: Any) -> StreamClient:
        defaults: dict[str, Any] = {
            "channels": ["trades", "ticker"],
            "markets": ["KXA-E1-X"],
            "verify": False,
            "idle_timeout": 5.0,
            "jitter": lambda: 1.0,
        }
        return StreamClient(
            "https://example.test",
            TOKEN,
            connect=self.connect,
            sleep=self.sleep,
            **{**defaults, **kw},
        )


def subscribed(cursor: int = 100, **extra: Any) -> dict[str, Any]:
    return {
        "type": "subscribed",
        "channels": ["trades", "ticker"],
        "markets": ["KXA-E1-X"],
        "unknown": [],
        "no_orderbook": [],
        "cursor": str(cursor),
        **extra,
    }


def trade(cursor: int, n: int | None = None) -> dict[str, Any]:
    return {
        "type": "trade",
        "ticker": "KXA-E1-X",
        "time": "2026-10-12T12:00:00.000000+00:00",
        "cursor": str(cursor),
        "trade_id": f"00000000-0000-0000-0000-{n or cursor:012d}",
        "yes_price": "0.500000",
        "no_price": "0.500000",
        "count": "1.00",
        "taker_side": "yes",
        "is_block_trade": False,
    }


def ticker(cursor: int) -> dict[str, Any]:
    return {
        "type": "ticker",
        "ticker": "KXA-E1-X",
        "time": "2026-10-12T12:00:00.000000+00:00",
        "cursor": str(cursor),
        "price": "0.510000",
        "yes_bid": "0.500000",
        "yes_ask": "0.520000",
        "yes_bid_size": "10.00",
        "yes_ask_size": "5.00",
        "volume": "100.00",
        "open_interest": "50.00",
        "last_trade_size": "1.00",
    }


def alert(cursor: int) -> dict[str, Any]:
    return {
        "type": "alert",
        "alert_cursor": str(cursor),
        "id": cursor,
        "rule_id": 1,
        "rule_name": "r",
        "fired_at": "2026-10-12T12:00:00.000000+00:00",
        "metric": "market_quotes.spread",
        "op": "gt",
        "threshold": "0.050000",
        "value": "0.080000",
        "unit": "dollars",
        "acknowledged_at": None,
        "ticker": "KXA-E1-X",
    }


async def collect(
    stream: StreamClient, done: Callable[[list[StreamEvent]], bool], limit: int = 200
) -> list[StreamEvent]:
    """Events until ``done`` says enough, then close the stream."""
    seen: list[StreamEvent] = []
    try:
        async with asyncio.timeout(10):
            async for event in stream:
                seen.append(event)
                if done(seen) or len(seen) >= limit:
                    break
    finally:
        await stream.aclose()
    return seen


def kinds(events: list[StreamEvent]) -> list[str]:
    return [e.type for e in events]


# ------------------------------------------------------------------ the first connection


async def test_the_first_connection_subscribes_fresh_and_delivers_typed_events() -> None:
    script = Script(FakeSocket(subscribed(), trade(101), ticker(102), HANG))
    events = await collect(script.stream(), lambda e: len(e) == 3)
    connected, first, second = events
    assert (
        isinstance(connected, Connected) and connected.resumed is False and connected.cursor == 100
    )
    assert (
        isinstance(first, TradeEvent)
        and first.cursor == 101
        and first.yes_price == Decimal("0.500000")
    )
    assert isinstance(second, TickerEvent) and second.yes_ask == Decimal("0.520000")
    [(url, kw)] = script.calls
    assert url == "wss://example.test/v1/stream" and kw["additional_headers"] == {
        "Authorization": f"Bearer {TOKEN}"
    }
    assert kw["ssl"] is False


async def test_the_subscribe_message_has_no_since_the_first_time() -> None:
    socket = FakeSocket(subscribed(), HANG)
    script = Script(socket)
    await collect(script.stream(markets=["B", "A", "A"], watchlist=True), lambda e: len(e) == 1)
    assert socket.sent == [
        {
            "op": "subscribe",
            "channels": ["trades", "ticker"],
            "markets": ["B", "A"],
            "watchlist": True,
        }
    ]


# ------------------------------------------------------------------ reconnect and resume


async def test_after_a_drop_it_resumes_from_the_last_delivered_cursor_with_nothing_repeated() -> (
    None
):
    first = FakeSocket(subscribed(), trade(101), trade(102), closed(1006))
    second = FakeSocket(subscribed(), trade(103), trade(104), HANG)
    script = Script(first, second)
    stream = script.stream()
    events = await collect(stream, lambda e: sum(isinstance(x, TradeEvent) for x in e) == 4)
    assert [e.cursor for e in events if isinstance(e, TradeEvent)] == [101, 102, 103, 104]
    assert second.sent[0]["since"] == "102"
    assert kinds(events) == [
        "connected",
        "trade",
        "trade",
        "disconnected",
        "connected",
        "trade",
        "trade",
    ]
    reconnect = [e for e in events if isinstance(e, Connected)][1]
    assert reconnect.resumed is True
    assert stream.stats.reconnects == 1 and stream.stats.events == 4


async def test_an_event_the_server_replays_twice_is_delivered_once() -> None:
    first = FakeSocket(subscribed(), trade(101), closed(1006))
    second = FakeSocket(
        subscribed(), trade(101), trade(101), trade(102), ticker(102), trade(103), HANG
    )
    script = Script(first, second)
    stream = script.stream()
    events = await collect(
        stream, lambda e: sum(isinstance(x, (TradeEvent, TickerEvent)) for x in e) == 3
    )
    data = [(e.type, e.cursor) for e in events if isinstance(e, TradeEvent | TickerEvent)]
    assert data == [("trade", 101), ("trade", 102), ("trade", 103)]
    assert stream.stats.duplicates_suppressed == 3  # 101 twice, and ticker(102) after trade(102)


async def test_heartbeats_and_caught_up_move_the_resume_point_forward() -> None:
    first = FakeSocket(
        subscribed(), trade(101), {"type": "heartbeat", "cursor": "250"}, closed(1006)
    )
    second = FakeSocket(subscribed(), {"type": "caught_up", "cursor": "300"}, closed(1006))
    third = FakeSocket(subscribed(), HANG)
    script = Script(first, second, third)
    events = await collect(script.stream(), lambda e: kinds(e).count("connected") == 3)
    assert second.sent[0]["since"] == "250" and third.sent[0]["since"] == "300"
    caught = [e for e in events if isinstance(e, CaughtUp)]
    assert [(c.stream, c.cursor) for c in caught] == [("market", 300)]


async def test_a_heartbeat_never_moves_the_cursor_backwards() -> None:
    first = FakeSocket(
        subscribed(), trade(500), {"type": "heartbeat", "cursor": "400"}, closed(1006)
    )
    second = FakeSocket(subscribed(), HANG)
    script = Script(first, second)
    await collect(script.stream(), lambda e: kinds(e).count("connected") == 2)
    assert second.sent[0]["since"] == "500"


# ------------------------------------------------------------------ backoff


async def test_the_wait_grows_each_time_is_capped_and_starts_over_after_a_good_connection() -> None:
    script = Script(
        OSError("refused"),
        OSError("refused"),
        OSError("refused"),
        OSError("refused"),
        FakeSocket(subscribed(), closed(1006)),  # connects, then drops
        OSError("refused"),
        FakeSocket(subscribed(), HANG),
    )
    stream = script.stream(backoff=(0.5, 3.0))
    await collect(stream, lambda e: kinds(e).count("connected") == 2)
    # capped at 3; the good connection restarts it at 0.5, and it grows again if the next one fails
    assert script.sleeps == [0.5, 1.0, 2.0, 3.0, 0.5, 1.0]


async def test_the_wait_is_jittered_between_half_and_all_of_the_backoff() -> None:
    low = Script(OSError("x"), FakeSocket(subscribed(), HANG))
    await collect(low.stream(jitter=lambda: 0.0), lambda e: kinds(e).count("connected") == 1)
    assert low.sleeps == [0.25]
    high = Script(OSError("x"), FakeSocket(subscribed(), HANG))
    await collect(high.stream(jitter=lambda: 1.0), lambda e: kinds(e).count("connected") == 1)
    assert high.sleeps == [0.5]


async def test_the_disconnected_event_says_why_and_when_it_will_retry() -> None:
    script = Script(FakeSocket(subscribed(), closed(1006)), FakeSocket(subscribed(), HANG))
    events = await collect(script.stream(), lambda e: kinds(e).count("connected") == 2)
    [drop] = [e for e in events if isinstance(e, Disconnected)]
    assert drop.reason == "connection closed (1006)" and drop.retry_in == 0.5


@pytest.mark.parametrize(
    ("closing", "fragment"),
    [
        (closed(1013), "reading too slowly"),
        (closed(4429), "too many connections"),
        (closed(1011), "connection closed (1011)"),
        ({"type": "error", "error": "too_many_connections", "limit": 5}, "limit 5"),
    ],
)
async def test_these_endings_are_retried(closing: Any, fragment: str) -> None:
    script = Script(FakeSocket(subscribed(), closing), FakeSocket(subscribed(), HANG))
    events = await collect(script.stream(), lambda e: kinds(e).count("connected") == 2)
    [drop] = [e for e in events if isinstance(e, Disconnected)]
    assert fragment in drop.reason


async def test_silence_beyond_the_idle_timeout_reconnects() -> None:
    script = Script(FakeSocket(subscribed(), trade(101), HANG), FakeSocket(subscribed(), HANG))
    events = await collect(
        script.stream(idle_timeout=0.05), lambda e: kinds(e).count("connected") == 2
    )
    [drop] = [e for e in events if isinstance(e, Disconnected)]
    assert "nothing from the server for 0.05 seconds" in drop.reason


async def test_a_failed_connect_is_retried() -> None:
    script = Script(ConnectionRefusedError(), TimeoutError(), FakeSocket(subscribed(), HANG))
    events = await collect(script.stream(), lambda e: kinds(e).count("connected") == 1)
    reasons = [e.reason for e in events if isinstance(e, Disconnected)]
    assert len(reasons) == 2 and all("cannot connect" in r for r in reasons)


# ------------------------------------------------------------------ fatal errors


@pytest.mark.parametrize(
    "ending",
    [{"type": "error", "error": "unauthorized"}, closed(4401)],
)
async def test_being_unauthorized_raises_and_does_not_loop(ending: Any) -> None:
    script = Script(FakeSocket(ending))
    with pytest.raises(Unauthorized, match="rejected the token") as caught:
        await collect(script.stream(), lambda e: False)
    assert TOKEN not in str(caught.value) and len(script.calls) == 1 and script.sleeps == []


async def test_a_refused_subscription_raises_with_the_servers_reason() -> None:
    bad = {"type": "error", "error": "invalid_parameter", "fields": ["markets"], "limit": 100}
    script = Script(FakeSocket(bad))
    with pytest.raises(InvalidRequest, match=r"invalid_parameter \(markets\)") as caught:
        await collect(script.stream(), lambda e: False)
    assert caught.value.code == "invalid_parameter" and caught.value.extra == {
        "fields": ["markets"],
        "limit": 100,
    }
    assert len(script.calls) == 1


async def test_a_server_that_cannot_parse_us_is_not_retried_forever() -> None:
    with pytest.raises(ConnectionProblem, match="close code 4400"):
        await collect(Script(FakeSocket(closed(4400))).stream(), lambda e: False)


async def test_a_certificate_problem_raises_and_does_not_loop() -> None:
    refusal = OSError("certificate verify failed")
    refusal.__cause__ = ssl.SSLCertVerificationError(1, "certificate has expired")
    script = Script(refusal)
    with pytest.raises(ConnectionProblem, match="certificate was rejected"):
        await collect(script.stream(), lambda e: False)
    assert len(script.calls) == 1 and script.sleeps == []


async def test_a_message_that_does_not_fit_its_model_names_the_field_not_the_value() -> None:
    broken = {**trade(101), "yes_price": "not-a-number"}
    script = Script(FakeSocket(subscribed(), broken))
    with pytest.raises(
        ConnectionProblem, match=r"trade message does not fit \(yes_price\)"
    ) as caught:
        await collect(script.stream(), lambda e: False)
    assert "not-a-number" not in str(caught.value)


# ------------------------------------------------------------------ resync


async def test_a_gap_resubscribes_from_now_and_says_the_state_is_stale() -> None:
    socket = FakeSocket(
        subscribed(),
        trade(101),
        {"type": "gap", "reason": "cursor_too_old", "cursor": "9000"},
        subscribed(9000),
        trade(9001),
        HANG,
    )
    seen = await collect(
        Script(socket).stream(), lambda e: sum(isinstance(x, TradeEvent) for x in e) == 2
    )
    assert kinds(seen) == ["connected", "trade", "resync", "connected", "trade"]
    resync = next(e for e in seen if isinstance(e, Resync))
    assert resync.reason == "cursor_too_old" and resync.missed_after == 101
    assert resync.cursor == 9000
    assert [m.get("since") for m in socket.sent] == [None, None]  # the second subscribe is fresh
    assert [e for e in seen if isinstance(e, Connected)][1].resumed is False


async def test_a_resync_keeps_the_alerts_cursor_so_no_alert_is_lost() -> None:
    socket = FakeSocket(
        subscribed(alerts_cursor="7"),
        alert(8),
        {"type": "gap", "reason": "cursor_too_old", "cursor": "9000"},
        subscribed(9000, alerts_cursor="8"),
        alert(9),
        HANG,
    )
    script = Script(socket)
    await collect(
        script.stream(channels=["trades", "alerts"]),
        lambda e: sum(isinstance(x, AlertEvent) for x in e) == 2,
    )
    assert socket.sent[1].get("since") is None and socket.sent[1]["alerts_since"] == "8"


async def test_a_cursor_from_the_future_after_a_restore_starts_over_once() -> None:
    future = {
        "type": "error",
        "error": "invalid_parameter",
        "fields": ["since"],
        "reason": "in_the_future",
    }
    first = FakeSocket(subscribed(), trade(5000), closed(1006))
    second = FakeSocket(future, subscribed(40), trade(41), HANG)  # the database was restored
    script = Script(first, second)
    events = await collect(
        script.stream(), lambda e: sum(isinstance(x, TradeEvent) for x in e) == 2
    )
    resync = next(e for e in events if isinstance(e, Resync))
    assert resync.reason == "since_in_the_future" and resync.missed_after == 5000
    assert second.sent[0]["since"] == "5000" and "since" not in second.sent[1]
    assert [e.cursor for e in events if isinstance(e, TradeEvent)] == [5000, 41]


async def test_a_server_that_keeps_refusing_the_cursor_is_given_up_on() -> None:
    future = {
        "type": "error",
        "error": "invalid_parameter",
        "fields": ["since"],
        "reason": "in_the_future",
    }
    script = Script(FakeSocket(subscribed(), trade(5000), closed(1006)), FakeSocket(future, future))
    with pytest.raises(ConnectionProblem, match="keeps refusing since"):
        await collect(script.stream(), lambda e: False)


# ------------------------------------------------------------------ alerts


async def test_alerts_have_their_own_cursor_resume_and_catch_up() -> None:
    first = FakeSocket(subscribed(alerts_cursor="0"), alert(1), alert(2), closed(1006))
    second = FakeSocket(
        subscribed(alerts_cursor="2"),
        alert(2),
        alert(3),
        {"type": "alerts_caught_up", "alerts_cursor": "3"},
        alert(4),
        HANG,
    )
    script = Script(first, second)
    stream = script.stream(channels=["alerts"], markets=[])
    events = await collect(stream, lambda e: sum(isinstance(x, AlertEvent) for x in e) == 4)
    assert [e.alert_cursor for e in events if isinstance(e, AlertEvent)] == [1, 2, 3, 4]
    assert "since" not in first.sent[0] and "since" not in second.sent[0]  # no market channels
    assert second.sent[0]["alerts_since"] == "2"
    assert any(isinstance(e, CaughtUp) and e.stream == "alerts" for e in events)
    assert stream.stats.duplicates_suppressed == 1


async def test_market_and_alert_cursors_do_not_mix() -> None:
    first = FakeSocket(
        subscribed(alerts_cursor="10"),
        trade(105),
        alert(11),
        {"type": "heartbeat", "cursor": "110", "alerts_cursor": "12"},
        closed(1006),
    )
    second = FakeSocket(subscribed(alerts_cursor="12"), HANG)
    script = Script(first, second)
    await collect(
        script.stream(channels=["trades", "alerts"]), lambda e: kinds(e).count("connected") == 2
    )
    assert second.sent[0]["since"] == "110" and second.sent[0]["alerts_since"] == "12"


# ------------------------------------------------------------------ the rest of the protocol


async def test_book_events_watchlist_changes_and_unknown_messages_come_through() -> None:
    snapshot = {
        "type": "book_snapshot",
        "ticker": "KXA-E1-X",
        "time": "2026-10-12T12:00:00.000000+00:00",
        "replay": True,
        "sid": 1,
        "seq": 10,
        "approximate": False,
        "yes": [{"price": "0.400000", "size": "10.00"}],
        "no": [],
    }
    delta = {
        "type": "book_delta",
        "ticker": "KXA-E1-X",
        "time": "2026-10-12T12:00:01.000000+00:00",
        "cursor": "101",
        "sid": 1,
        "seq": 11,
        "side": "yes",
        "price": "0.400000",
        "delta": "-2.00",
    }
    script = Script(
        FakeSocket(
            subscribed(),
            snapshot,
            delta,
            {"type": "watchlist_changed", "added": ["B"], "removed": []},
            {"type": "from_the_future", "x": 1},
            {"type": "pong"},
            HANG,
        )
    )
    events = await collect(script.stream(channels=["orderbook"]), lambda e: len(e) == 5)
    assert (
        isinstance(events[1], BookSnapshotEvent)
        and events[1].cursor is None
        and events[1].replay is True
    )
    assert isinstance(events[2], BookDeltaEvent) and events[2].delta == Decimal("-2.00")
    assert isinstance(events[3], WatchlistChanged) and events[3].added == ["B"]
    assert isinstance(events[4], UnknownEvent) and events[4].original_type == "from_the_future"


async def test_orderbook_replay_events_without_a_cursor_are_never_treated_as_duplicates() -> None:
    snap = {
        "type": "book_snapshot",
        "ticker": "KXA-E1-X",
        "time": "2026-10-12T12:00:00.000000+00:00",
        "replay": True,
        "sid": 1,
        "seq": 10,
        "yes": [],
        "no": [],
    }
    script = Script(FakeSocket(subscribed(), snap, snap, snap, HANG))
    stream = script.stream(channels=["orderbook"])
    events = await collect(stream, lambda e: len(e) == 4)
    assert kinds(events) == ["connected", "book_snapshot", "book_snapshot", "book_snapshot"]
    assert stream.stats.duplicates_suppressed == 0


# ------------------------------------------------------------------ construction and use


def test_a_stream_needs_valid_channels_and_something_to_watch() -> None:
    bad: list[dict[str, Any]] = [
        {"channels": []},
        {"channels": ["nonsense"]},
        {"channels": ["trades"], "markets": []},
    ]
    for kwargs in bad:
        with pytest.raises(ValueError):
            StreamClient("https://example.test", TOKEN, **{"markets": ["A"], **kwargs})
    StreamClient("https://example.test", TOKEN, channels=["alerts"])  # alerts need no markets
    StreamClient("https://example.test", TOKEN, channels=["trades"], watchlist=True)


async def test_plain_http_servers_use_ws_without_tls() -> None:
    script = Script(FakeSocket(subscribed(), HANG))
    stream = StreamClient(
        "http://127.0.0.1:8700",
        TOKEN,
        channels=["trades"],
        markets=["A"],
        connect=script.connect,
        sleep=script.sleep,
    )
    await collect(stream, lambda e: len(e) == 1)
    [(url, kw)] = script.calls
    assert url == "ws://127.0.0.1:8700/v1/stream" and kw["ssl"] is None


async def test_only_one_consumer_at_a_time_and_closing_ends_the_iteration() -> None:
    script = Script(FakeSocket(subscribed(), HANG))
    stream = script.stream()
    iterator = stream.events()
    assert isinstance((await iterator.__anext__()), Connected)
    with pytest.raises(RuntimeError, match="one consumer"):
        await stream.events().__anext__()
    await stream.aclose()
    with pytest.raises(StopAsyncIteration):
        await iterator.__anext__()
