import asyncio
import random
import threading
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient
from kalshiterm_server import auth, db
from kalshiterm_server.api import stream as stream_module
from kalshiterm_server.api.app import create_app
from kalshiterm_server.config import ServerSettings
from kalshiterm_server.ingest.stream import PUBLISH
from sqlalchemy import text
from starlette.websockets import WebSocketDisconnect

pytestmark = pytest.mark.db

NOW = datetime.now(UTC).replace(microsecond=0)
A, B, C = "KXA-E1-X", "KXB-E1-Y", "KXC-E1-Z"


# ---------------------------------------------------------------- the database side


async def prepare(url: str) -> dict[str, Any]:
    engine = db.make_engine(url)
    try:
        async with engine.begin() as conn:
            ids = {}
            for ticker in (A, B, C):
                ids[ticker] = (
                    await conn.execute(
                        text(
                            "INSERT INTO markets (ticker, event_ticker, market_type, status) "
                            "VALUES (:t, 'KX-E1', 'binary', 'active') RETURNING id"
                        ),
                        {"t": ticker},
                    )
                ).scalar_one()
        tokens = {}
        for name in ("alice", "bob"):
            await auth.add_user(engine, name)
            tokens[name] = (await auth.create_token(engine, name, "read"))[1]
        return {"ids": ids, "tokens": tokens}
    finally:
        await engine.dispose()


class Writer:
    """Commits rows the way the ingestor does: a batch, the watermark and a NOTIFY together."""

    def __init__(self, url: str, ids: dict[str, int]) -> None:
        self.url = url
        self.ids = ids
        self.n = 0

    def batch(
        self,
        *,
        trades: tuple[str, ...] = (),
        tickers: tuple[str, ...] = (),
        snapshots: tuple[tuple[str, int, int, list[tuple[int, int]]], ...] = (),
        deltas: tuple[tuple[str, int, int, int, int], ...] = (),
    ) -> dict[str, list[int]]:
        """trades/tickers: market tickers; snapshots: (ticker, sid, seq, yes levels);
        deltas: (ticker, sid, seq, price_e6, delta_e2). Returns the ingest_seq of each row."""
        return asyncio.run(self._batch(trades, tickers, snapshots, deltas))

    async def _batch(self, trades: Any, tickers: Any, snapshots: Any, deltas: Any) -> Any:
        engine = db.make_engine(self.url)
        seqs: dict[str, list[int]] = {"trades": [], "tickers": [], "snapshots": [], "deltas": []}
        try:
            async with engine.connect() as conn:
                raw = await conn.get_raw_connection()
                driver = raw.driver_connection
                assert driver is not None
                async with driver.transaction():
                    for ticker in trades:
                        self.n += 1
                        seqs["trades"].append(
                            await driver.fetchval(
                                "INSERT INTO trades (ts, received_at, market_id, trade_id, "
                                "yes_price_e6, count_e2, taker_side) VALUES ($1, $1, $2, $3, "
                                "$4, 100, 'yes') RETURNING ingest_seq",
                                NOW + timedelta(milliseconds=self.n),
                                self.ids[ticker],
                                uuid.UUID(int=self.n),
                                500000 + self.n,
                            )
                        )
                    for ticker in tickers:
                        self.n += 1
                        seqs["tickers"].append(
                            await driver.fetchval(
                                "INSERT INTO tickers (ts, received_at, market_id, price_e6, "
                                "yes_bid_e6) VALUES ($1, $1, $2, $3, 1) RETURNING ingest_seq",
                                NOW + timedelta(milliseconds=self.n),
                                self.ids[ticker],
                                400000 + self.n,
                            )
                        )
                    for ticker, sid, seq, yes in snapshots:
                        self.n += 1
                        seqs["snapshots"].append(
                            await driver.fetchval(
                                "INSERT INTO orderbook_snapshots (ts, received_at, market_id, "
                                "seq, sid, approximate, yes_prices_e6, yes_sizes_e2, no_prices_e6,"
                                " no_sizes_e2) VALUES ($1, $1, $2, $3, $4, false, $5, $6, '{}', "
                                "'{}') RETURNING ingest_seq",
                                NOW + timedelta(milliseconds=self.n),
                                self.ids[ticker],
                                seq,
                                sid,
                                [p for p, _ in yes],
                                [s for _, s in yes],
                            )
                        )
                    for ticker, sid, seq, price, change in deltas:
                        self.n += 1
                        seqs["deltas"].append(
                            await driver.fetchval(
                                "INSERT INTO orderbook_deltas (ts, received_at, market_id, seq, "
                                "sid, is_yes, price_e6, delta_e2) VALUES ($1, $1, $2, $3, $4, "
                                "true, $5, $6) RETURNING ingest_seq",
                                NOW + timedelta(milliseconds=self.n),
                                self.ids[ticker],
                                seq,
                                sid,
                                price,
                                change,
                            )
                        )
                    watermark = await driver.fetchval(PUBLISH)
                    await driver.execute("SELECT pg_notify('kterm_ingest', $1)", str(watermark))
        finally:
            await engine.dispose()
        return seqs


# ---------------------------------------------------------------- the websocket side


class Wire:
    """A WebSocket test session whose receives time out instead of hanging a failing test."""

    def __init__(self, session: Any) -> None:
        self.session = session
        self.pool = ThreadPoolExecutor(max_workers=1)

    def send(self, message: dict[str, Any]) -> None:
        self.session.send_json(message)

    def recv(self, timeout: float = 8.0) -> dict[str, Any]:
        future = self.pool.submit(self.session.receive_json)
        try:
            result: dict[str, Any] = future.result(timeout)
        except FutureTimeout as exc:
            raise AssertionError("no message arrived in time") from exc
        return result

    def recv_until(self, kind: str, limit: int = 400) -> list[dict[str, Any]]:
        """Messages up to and including the first of ``kind``."""
        seen = []
        for _ in range(limit):
            seen.append(self.recv())
            if seen[-1]["type"] == kind:
                return seen
        raise AssertionError(f"no {kind!r} within {limit} messages: {seen[-3:]}")

    def recv_events(self, count: int, *kinds: str) -> list[dict[str, Any]]:
        got: list[dict[str, Any]] = []
        while len(got) < count:
            message = self.recv()
            if message["type"] in kinds:
                got.append(message)
        return got


@pytest.fixture
def env(migrated_db_url: str) -> Iterator[Any]:
    prepared = asyncio.run(prepare(migrated_db_url))
    state: dict[str, Any] = {"url": migrated_db_url, **prepared}
    state["writer"] = Writer(migrated_db_url, prepared["ids"])
    yield state


@contextmanager
def server(env: dict[str, Any], **settings: Any) -> Iterator[TestClient]:
    app = create_app(ServerSettings(db_url=env["url"], **settings))
    with TestClient(app) as client:
        env["app"] = app
        yield client


@contextmanager
def connect(
    client: TestClient, env: dict[str, Any], user: str = "alice", **kw: Any
) -> Iterator[Wire]:
    headers = {"Authorization": f"Bearer {env['tokens'][user]}"}
    with client.websocket_connect("/v1/stream", headers=headers, **kw) as session:
        yield Wire(session)


def subscribe(wire: Wire, channels: list[str], markets: list[str], **extra: Any) -> dict[str, Any]:
    wire.send({"op": "subscribe", "channels": channels, "markets": markets, **extra})
    ack = wire.recv()
    assert ack["type"] == "subscribed", ack
    return ack


# ---------------------------------------------------------------- authentication


def test_a_valid_token_in_the_header_is_accepted(env: Any) -> None:
    with server(env) as client, connect(client, env) as wire:
        assert subscribe(wire, ["trades"], [A])["markets"] == [A]


def test_a_token_in_a_first_frame_works_for_clients_that_cannot_set_headers(env: Any) -> None:
    with server(env) as client, client.websocket_connect("/v1/stream") as session:
        wire = Wire(session)
        wire.send({"op": "auth", "token": env["tokens"]["bob"]})
        assert subscribe(wire, ["trades"], [A])["type"] == "subscribed"


@pytest.mark.parametrize("how", ["bad_header", "bad_frame", "no_credentials"])
def test_every_kind_of_failed_login_gets_the_same_answer_and_a_closed_socket(
    env: Any, how: str
) -> None:
    with server(env, stream_auth_timeout=0.4, auth_failure_limit=1000) as client:
        headers = {"Authorization": "Bearer kt_1_" + "x" * 43} if how == "bad_header" else {}
        with client.websocket_connect("/v1/stream", headers=headers) as session:
            wire = Wire(session)
            if how == "bad_frame":
                wire.send({"op": "auth", "token": "kt_1_" + "y" * 43})
            assert wire.recv() == {"type": "error", "error": "unauthorized"}
            with pytest.raises(WebSocketDisconnect) as closed:
                wire.recv()
            assert closed.value.code == 4401


def test_repeated_failures_lock_the_address_out_even_for_a_valid_token(env: Any) -> None:
    with server(env, auth_failure_limit=2, stream_auth_timeout=0.3) as client:
        for _ in range(2):
            with client.websocket_connect(
                "/v1/stream", headers={"Authorization": "Bearer nope"}
            ) as s:
                Wire(s).recv()
        with connect(client, env) as wire:  # a valid token, from the locked-out address
            assert wire.recv()["error"] == "unauthorized"


def test_the_connection_limit_per_user_is_enforced_and_released(env: Any) -> None:
    # (The test client cannot hold two sessions open at once, so the hub's count for the
    # user is set directly; the code path under test is the same one real sessions use.)
    with server(env, stream_max_connections_per_user=2) as client:
        hub = env["app"].state.hub
        hub.connections[1] = 2  # alice already has two sockets open
        with connect(client, env) as refused:
            assert refused.recv() == {"type": "error", "error": "too_many_connections", "limit": 2}
            with pytest.raises(WebSocketDisconnect) as closed:
                refused.recv()
            assert closed.value.code == 4429
        assert hub.connections[1] == 2  # a refused connection takes no slot
        with connect(client, env, user="bob") as other:  # limits are per user
            assert subscribe(other, ["trades"], [A])["type"] == "subscribed"
        hub.connections[1] = 1
        with connect(client, env) as allowed:
            subscribe(allowed, ["trades"], [A])
            assert hub.connections[1] == 2
        deadline = 50
        while hub.connections[1] != 1 and deadline:  # the slot is released when the socket ends
            threading.Event().wait(0.05)
            deadline -= 1
        assert hub.connections[1] == 1


# ---------------------------------------------------------------- delivery


def test_live_events_arrive_in_cursor_order_with_exact_values_for_subscribed_markets_only(
    env: Any,
) -> None:
    writer: Writer = env["writer"]
    with server(env) as client, connect(client, env) as wire:
        ack = subscribe(wire, ["trades", "ticker"], [A])
        assert ack["unknown"] == [] and ack["cursor"] == "0"
        seqs = writer.batch(trades=(A, B, A), tickers=(B, A))
        events = wire.recv_events(3, "trade", "ticker")  # A's two trades and A's ticker
        assert [e["type"] for e in events] == ["trade", "trade", "ticker"]
        assert [int(e["cursor"]) for e in events] == [
            seqs["trades"][0],
            seqs["trades"][2],
            seqs["tickers"][1],
        ]
        assert events[0] == {
            "type": "trade", "ticker": A, "cursor": str(seqs["trades"][0]),
            "time": events[0]["time"], "trade_id": str(uuid.UUID(int=1)),
            "yes_price": "0.500001", "no_price": "0.499999", "count": "1.00",
            "taker_side": "yes", "is_block_trade": False,
        }  # fmt: skip
        assert events[2]["price"] == "0.400005" and events[2]["yes_bid"] == "0.000001"


def test_only_the_channels_asked_for_are_delivered(env: Any) -> None:
    writer: Writer = env["writer"]
    with server(env) as client, connect(client, env) as wire:
        subscribe(wire, ["ticker"], [A])
        writer.batch(trades=(A,), tickers=(A,))
        assert wire.recv()["type"] == "ticker"
        writer.batch(trades=(A,))
        writer.batch(tickers=(A,))
        assert wire.recv()["type"] == "ticker"  # the trade in between never appeared


def test_unknown_markets_are_reported_not_fatal_and_bad_requests_leave_the_connection_open(
    env: Any,
) -> None:
    with server(env, stream_max_markets=3) as client, connect(client, env) as wire:
        ack = subscribe(wire, ["trades"], [A, "NOPE-1"])
        assert ack["markets"] == [A] and ack["unknown"] == ["NOPE-1"]
        wire.send({"op": "subscribe", "channels": ["quotes"], "markets": [A]})
        assert wire.recv() == {
            "type": "error",
            "error": "invalid_parameter",
            "fields": ["channels"],
        }
        wire.send({"op": "subscribe", "channels": ["trades"], "markets": ["bad ticker"]})
        assert wire.recv()["fields"] == ["markets"]
        wire.send({"op": "subscribe", "channels": ["trades"], "markets": [A, B, C, "KX-4"]})
        assert wire.recv()["error"] == "invalid_parameter"  # over the per-subscription limit
        wire.send({"op": "subscribe", "channels": ["trades"], "markets": [A], "since": "12ab"})
        assert wire.recv()["fields"] == ["since"]
        wire.send({"op": "subscribe", "channels": ["trades"], "markets": [A], "since": "999999"})
        assert wire.recv()["reason"] == "in_the_future"
        assert subscribe(wire, ["trades"], [A])["type"] == "subscribed"  # still usable


def test_unsubscribe_stops_the_flow_and_ping_is_answered(env: Any) -> None:
    writer: Writer = env["writer"]
    with server(env) as client, connect(client, env) as wire:
        subscribe(wire, ["trades"], [A])
        wire.send({"op": "ping"})
        assert wire.recv() == {"type": "pong"}
        wire.send({"op": "unsubscribe"})
        assert wire.recv() == {"type": "unsubscribed"}
        writer.batch(trades=(A,))
        wire.send({"op": "ping"})
        assert wire.recv() == {"type": "pong"}  # nothing arrived in between


def test_garbage_frames_get_errors_and_too_many_close_the_connection(env: Any) -> None:
    with server(env) as client, connect(client, env) as wire:
        for _ in range(4):
            wire.session.send_text("not json")
            assert wire.recv() == {"type": "error", "error": "invalid_message"}
        wire.session.send_text("[1, 2]")
        assert wire.recv()["error"] == "invalid_message"
        with pytest.raises(WebSocketDisconnect) as closed:
            wire.recv()
        assert closed.value.code == 4400


def test_a_heartbeat_carries_the_current_cursor(env: Any) -> None:
    writer: Writer = env["writer"]
    seqs = writer.batch(trades=(A,))
    with server(env, stream_heartbeat_seconds=0.3) as client, connect(client, env) as wire:
        subscribe(wire, ["trades"], [A])
        beat = wire.recv_until("heartbeat")[-1]
        assert beat["cursor"] == str(seqs["trades"][0])


def test_the_two_streams_are_merged_in_cursor_order_across_pages(env: Any) -> None:
    writer: Writer = env["writer"]
    expected: list[int] = []
    with server(env, stream_page=3) as client, connect(client, env) as wire:
        subscribe(wire, ["trades", "ticker"], [A])
        for _ in range(4):  # many rows per batch, a tiny page: forces the merge ceiling logic
            seqs = writer.batch(trades=(A,) * 5, tickers=(A,) * 5)
            expected += sorted(seqs["trades"] + seqs["tickers"])
        got = wire.recv_events(len(expected), "trade", "ticker")
    assert [int(e["cursor"]) for e in got] == expected  # nothing missing, nothing twice, in order


# ---------------------------------------------------------------- reconnecting


def test_a_reconnect_with_since_replays_exactly_what_was_missed_then_goes_live(env: Any) -> None:
    writer: Writer = env["writer"]
    seqs = [writer.batch(trades=(A,))["trades"][0] for _ in range(8)]
    with server(env) as client, connect(client, env) as wire:
        ack = subscribe(wire, ["trades"], [A], since=str(seqs[3]))
        assert ack["cursor"] == str(seqs[3])
        replayed = wire.recv_until("caught_up")
        assert [int(e["cursor"]) for e in replayed[:-1]] == seqs[4:]
        assert replayed[-1] == {"type": "caught_up", "cursor": str(seqs[-1])}
        live = writer.batch(trades=(A,))["trades"][0]
        assert int(wire.recv()["cursor"]) == live


def test_a_client_too_far_behind_is_told_to_refetch_instead_of_being_replayed(env: Any) -> None:
    writer: Writer = env["writer"]
    seqs = [writer.batch(trades=(A,))["trades"][0] for _ in range(10)]
    with server(env, stream_catchup_horizon=3) as client, connect(client, env) as wire:
        subscribe(wire, ["trades"], [A], since=str(seqs[0]))
        assert wire.recv() == {"type": "gap", "reason": "cursor_too_old", "cursor": str(seqs[-1])}
        live = writer.batch(trades=(A,))["trades"][0]
        assert int(wire.recv()["cursor"]) == live  # live delivery continues from the head


def test_exactly_once_across_random_disconnects_while_rows_keep_arriving(env: Any) -> None:
    """The property push exists for: whatever the timing, resuming from the last cursor seen
    yields every row once. Rows are committed in random-sized batches by a second thread."""
    writer: Writer = env["writer"]
    written: list[str] = []
    stop = threading.Event()
    rng = random.Random(7)

    def produce() -> None:
        while not stop.is_set() and len(written) < 150:
            size = rng.randint(1, 6)
            writer.batch(trades=(A,) * size)
            written.extend(str(uuid.UUID(int=writer.n - size + i + 1)) for i in range(size))

    received: list[str] = []
    cursor = "0"
    with server(env, stream_page=4) as client:
        producer = threading.Thread(target=produce)
        producer.start()
        try:
            for _ in range(60):  # connect, take a few events, drop, repeat
                with connect(client, env) as wire:
                    subscribe(wire, ["trades"], [A], since=cursor)
                    for _ in range(rng.randint(1, 9)):
                        try:
                            message = wire.recv(timeout=0.6)
                        except AssertionError:
                            break
                        if message["type"] == "trade":
                            received.append(message["trade_id"])
                            cursor = message["cursor"]
                if not producer.is_alive() and len(received) >= len(written):
                    break
        finally:
            stop.set()
            producer.join(timeout=30)
        with connect(client, env) as wire:  # drain what is left
            subscribe(wire, ["trades"], [A], since=cursor)
            while len(received) < len(written):
                message = wire.recv()
                if message["type"] == "trade":
                    received.append(message["trade_id"])
    assert len(written) >= 100  # a meaningful amount was written
    assert received == written  # every row, once, in order


# ---------------------------------------------------------------- the order book


def test_a_fresh_orderbook_subscription_starts_from_the_snapshot_and_the_changes_after_it(
    env: Any,
) -> None:
    writer: Writer = env["writer"]
    writer.batch(
        snapshots=((A, 5, 100, [(500000, 1000), (490000, 500)]),),
        deltas=(
            (A, 5, 99, 500000, 777),  # sequenced BEFORE the snapshot: already in it
            (A, 5, 101, 500000, 200),
            (A, 4, 500, 500000, 999),  # an earlier subscription's: not part of this book
            (A, 5, 102, 480000, 300),
        ),
    )
    with server(env) as client, connect(client, env) as wire:
        ack = subscribe(wire, ["orderbook"], [A, B])
        assert ack["no_orderbook"] == [B] and ack["cursor"] != "0"
        replay = [wire.recv() for _ in range(3)]
        assert [m["type"] for m in replay] == ["book_snapshot", "book_delta", "book_delta"]
        assert all(m["replay"] is True and "cursor" not in m for m in replay)
        assert replay[0]["yes"] == [
            {"price": "0.500000", "size": "10.00"},
            {"price": "0.490000", "size": "5.00"},
        ]
        assert [(m["sid"], m["seq"]) for m in replay[1:]] == [(5, 101), (5, 102)]
        live = writer.batch(deltas=((A, 5, 103, 510000, 50),))
        event = wire.recv()
        assert event["type"] == "book_delta" and event["cursor"] == str(live["deltas"][0])
        assert "replay" not in event and (event["sid"], event["seq"]) == (5, 103)


def test_live_orderbook_events_after_a_new_snapshot_follow_the_new_reference(env: Any) -> None:
    writer: Writer = env["writer"]
    writer.batch(snapshots=((A, 5, 100, [(500000, 1000)]),))
    with server(env) as client, connect(client, env) as wire:
        subscribe(wire, ["orderbook"], [A])
        assert wire.recv()["type"] == "book_snapshot"
        writer.batch(deltas=((A, 5, 101, 500000, 1),))
        writer.batch(snapshots=((A, 5, 200, [(500000, 5000)]),))  # a periodic snapshot
        writer.batch(deltas=((A, 5, 150, 500000, 9), (A, 5, 201, 500000, 2)))
        events = wire.recv_events(3, "book_snapshot", "book_delta")
        assert [(e["type"], e["seq"]) for e in events] == [
            ("book_delta", 101),
            ("book_snapshot", 200),
            ("book_delta", 201),  # seq 150 is older than the new snapshot: it is in it
        ]


# ---------------------------------------------------------------- the watchlist


def test_the_users_watchlist_can_be_subscribed_and_follows_changes(
    env: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(stream_module, "WATCHLIST_REFRESH", 0.4)
    writer: Writer = env["writer"]

    def add_to_list(ticker: str) -> None:
        async def go() -> None:
            engine = db.make_engine(env["url"])
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO user_watchlists (user_id, market_id) "
                        "SELECT u.id, m.id FROM users u, markets m WHERE u.name = 'alice' "
                        "AND m.ticker = :t"
                    ),
                    {"t": ticker},
                )
            await engine.dispose()

        asyncio.run(go())

    add_to_list(A)
    with server(env) as client, connect(client, env) as wire:
        ack = subscribe(wire, ["trades"], [], watchlist=True)
        assert ack["markets"] == [A]
        add_to_list(B)
        change = wire.recv_until("watchlist_changed")[-1]
        assert change == {"type": "watchlist_changed", "added": [B], "removed": []}
        writer.batch(trades=(C, B))
        assert wire.recv()["ticker"] == B  # C is not on the list


# ---------------------------------------------------------------- a stalled client


def test_a_client_that_cannot_keep_up_is_dropped_so_it_can_catch_up_later(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    closed: list[int] = []

    class Stalled:
        async def send_text(self, _: str) -> None:
            await asyncio.sleep(3600)

        async def close(self, code: int = 1000) -> None:
            closed.append(code)

    monkeypatch.setattr(stream_module, "SEND_TIMEOUT", 0.1)
    session = stream_module.Session.__new__(stream_module.Session)
    session.ws = Stalled()  # type: ignore[assignment]

    async def go() -> None:
        with pytest.raises(WebSocketDisconnect) as dropped:
            await session.send({"type": "trade"})
        assert dropped.value.code == 1013

    asyncio.run(go())
    assert closed == [1013]
