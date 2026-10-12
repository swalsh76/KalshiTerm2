"""Alerts over /v1/stream: their own cursor, per-user delivery, exactly-once resume."""

import asyncio
import threading
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg  # type: ignore[import-untyped]
import pytest
from kalshiterm_server import db
from kalshiterm_server.analytics.base import AnalysisContext
from kalshiterm_server.analytics.builtin.alert_evaluator import AlertEvaluator
from sqlalchemy import text
from test_api_stream import A, connect, env, server, subscribe  # noqa: F401

pytestmark = pytest.mark.db

NOW = datetime(2026, 10, 12, 12, 0, tzinfo=UTC)


class Alerts:
    """Commits alerts the way the evaluator does: rows plus a NOTIFY in one transaction."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.dsn = url.replace("postgresql+asyncpg://", "postgresql://")
        self._users_by_name: dict[str, int] | None = None
        self.n = 0

    @property
    def users(self) -> dict[str, int]:
        """Looked up on first use, so building an ``Alerts`` inside a running loop is fine."""
        if self._users_by_name is None:
            self._users_by_name = asyncio.run(self._users())
        return self._users_by_name

    async def _users(self) -> dict[str, int]:
        engine = db.make_engine(self.url)
        try:
            async with engine.connect() as conn:
                return {n: i for n, i in await conn.execute(text("select name, id from users"))}
        finally:
            await engine.dispose()

    def add(self, *who: str) -> list[int]:
        """One transaction inserting an alert per name; returns their ids."""
        _ = self.users  # look the users up now, outside the coroutine's event loop
        return asyncio.run(self._add(who))

    async def _add(self, who: tuple[str, ...]) -> list[int]:
        conn = await asyncpg.connect(self.dsn)
        try:
            async with conn.transaction():
                ids = []
                for name in who:
                    self.n += 1
                    ids.append(await self._insert(conn, name))
                await conn.execute("SELECT pg_notify('kterm_alerts', $1)", f"x:{ids[-1]}")
            return ids
        finally:
            await conn.close()

    async def _insert(self, conn: Any, name: str) -> int:
        return int(
            await conn.fetchval(
                "INSERT INTO alerts (user_id, fired_at, rule_name, analyzer, metric, op, "
                "threshold_e6, value_e6, event_ticker) VALUES ($1, $2, $3, 'arbitrage_bid', "
                "'profit', 'gt', 0, $4, 'KX-E1') RETURNING id",
                self.users[name],
                NOW + timedelta(seconds=self.n),
                f"{name}-{self.n}",
                self.n * 1000,
            )
        )

    def add_slowly(self, name: str, release: threading.Event) -> tuple[threading.Thread, list[int]]:
        """Insert an alert in a transaction that stays open until ``release`` is set."""
        _ = self.users
        ids: list[int] = []
        opened = threading.Event()

        async def hold() -> None:
            conn = await asyncpg.connect(self.dsn)
            try:
                tx = conn.transaction()
                await tx.start()
                self.n += 1
                ids.append(await self._insert(conn, name))
                opened.set()
                await asyncio.to_thread(release.wait, 30)  # a threading.Event set by the test
                await conn.execute("SELECT pg_notify('kterm_alerts', 'x:y')")
                await tx.commit()
            finally:
                await conn.close()

        thread = threading.Thread(target=lambda: asyncio.run(hold()))
        thread.start()
        assert opened.wait(10)
        return thread, ids


@pytest.fixture
def alerts(env: Any) -> Alerts:
    return Alerts(env["url"])


def only(messages: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    return [m for m in messages if m["type"] == kind]


def test_an_alerts_only_subscription_gets_new_alerts_for_the_caller_alone(
    env: Any,
    alerts: Alerts,
) -> None:
    before = alerts.add("alice")[0]
    with server(env) as client, connect(client, env) as wire:
        ack = subscribe(wire, ["alerts"], [])
        assert ack["channels"] == ["alerts"] and ack["alerts_cursor"] == str(before)
        a, b = alerts.add("bob", "alice")  # bob's must not reach alice
        event = wire.recv()
        assert event["type"] == "alert" and event["id"] == b and event["alert_cursor"] == str(b)
        assert "cursor" not in event  # never mistaken for a market cursor
        assert event["rule_name"].startswith("alice-") and event["event_ticker"] == "KX-E1"
        assert event["metric"] == "arbitrage_bid.profit" and event["unit"] == "dollars"
        assert event["value"] == f"{(b and alerts.n) * 1000 / 1_000_000:.6f}"
        assert a != b and event["acknowledged_at"] is None


def test_alerts_from_before_the_subscription_are_not_replayed_without_alerts_since(
    env: Any,
    alerts: Alerts,
) -> None:
    old = alerts.add("alice", "alice")
    with server(env) as client, connect(client, env) as wire:
        assert subscribe(wire, ["alerts"], [])["alerts_cursor"] == str(old[-1])
        new = alerts.add("alice")[0]
        assert wire.recv()["id"] == new  # the first thing it hears is the new one


def test_resuming_with_alerts_since_replays_exactly_the_missed_alerts_then_says_so(
    env: Any,
    alerts: Alerts,
) -> None:
    ids = alerts.add("alice", "bob", "alice", "bob", "alice")
    mine = [ids[0], ids[2], ids[4]]
    with server(env) as client, connect(client, env) as wire:
        ack = subscribe(wire, ["alerts"], [], alerts_since=str(mine[0]))
        assert ack["alerts_cursor"] == str(mine[0])
        got = wire.recv_until("alerts_caught_up")
        assert [m["id"] for m in only(got, "alert")] == mine[1:]  # after the cursor, mine only
        assert got[-1] == {"type": "alerts_caught_up", "alerts_cursor": str(mine[-1])}
        later = alerts.add("alice")[0]
        assert wire.recv()["id"] == later  # and then live, with nothing repeated


def test_a_reconnecting_client_sees_every_alert_exactly_once(
    env: Any,
    alerts: Alerts,
) -> None:
    held: list[int] = []
    with server(env) as client:
        with connect(client, env) as wire:
            subscribe(wire, ["alerts"], [])
            for _ in range(3):
                held += [alerts.add("alice", "bob")[0]]
                assert wire.recv()["id"] == held[-1]
        missed = [alerts.add("alice")[0], alerts.add("bob", "alice")[1], alerts.add("alice")[0]]
        with connect(client, env) as wire:
            subscribe(wire, ["alerts"], [], alerts_since=str(held[-1]))
            replayed = only(wire.recv_until("alerts_caught_up"), "alert")
            assert [m["id"] for m in replayed] == missed
            fresh = alerts.add("alice")[0]
            assert wire.recv()["id"] == fresh
    everything = held + missed + [fresh]
    assert len(set(everything)) == len(everything) == 7 and everything == sorted(everything)


def test_a_long_backlog_is_delivered_in_order_across_pages(
    env: Any,
    alerts: Alerts,
) -> None:
    ids = [alerts.add("alice")[0] for _ in range(7)]
    with server(env, stream_page=2) as client, connect(client, env) as wire:
        subscribe(wire, ["alerts"], [], alerts_since="0")
        got = wire.recv_until("alerts_caught_up")
        assert [m["id"] for m in only(got, "alert")] == ids


def test_market_data_and_alerts_share_a_connection_with_separate_cursors(
    env: Any,
    alerts: Alerts,
) -> None:
    with server(env) as client, connect(client, env) as wire:
        ack = subscribe(wire, ["trades", "alerts"], [A])
        assert "cursor" in ack and "alerts_cursor" in ack
        seqs = env["writer"].batch(trades=(A,))
        alert_id = alerts.add("alice")[0]
        seen = wire.recv_events(2, "trade", "alert")
        trade = next(e for e in seen if e["type"] == "trade")
        alert = next(e for e in seen if e["type"] == "alert")
        assert trade["cursor"] == str(seqs["trades"][0]) and "alert_cursor" not in trade
        assert alert["alert_cursor"] == str(alert_id) and "cursor" not in alert


def test_the_heartbeat_carries_the_alerts_cursor_only_when_subscribed(
    env: Any,
    alerts: Alerts,
) -> None:
    head = alerts.add("alice")[0]
    with server(env, stream_heartbeat_seconds=0.3) as client:
        with connect(client, env) as wire:
            subscribe(wire, ["alerts"], [])
            assert wire.recv_until("heartbeat")[-1]["alerts_cursor"] == str(head)
        with connect(client, env) as wire:
            subscribe(wire, ["trades"], [A])
            assert "alerts_cursor" not in wire.recv_until("heartbeat")[-1]


def test_an_alert_still_uncommitted_at_subscribe_time_is_delivered_when_it_commits(
    env: Any,
    alerts: Alerts,
) -> None:
    """The mechanism behind exactly-once: a live subscription starts after the highest
    COMMITTED id, so a transaction still open at that moment is delivered when it commits."""
    release = threading.Event()
    thread, pending = alerts.add_slowly("alice", release)
    try:
        with server(env) as client, connect(client, env) as wire:
            ack = subscribe(wire, ["alerts"], [])
            assert int(ack["alerts_cursor"]) < pending[0]  # not yet visible, so not counted
            release.set()
            thread.join(10)
            assert wire.recv()["id"] == pending[0]
    finally:
        release.set()
        thread.join(10)


@pytest.mark.parametrize(
    ("extra", "channels", "field", "reason"),
    [
        ({"alerts_since": "abc"}, ["alerts"], "alerts_since", None),
        ({"alerts_since": 5}, ["alerts"], "alerts_since", None),
        ({"alerts_since": "-1"}, ["alerts"], "alerts_since", None),
        ({"alerts_since": "999999"}, ["alerts"], "alerts_since", "in_the_future"),
        ({"alerts_since": "0"}, ["trades"], "alerts_since", None),  # no alerts channel
        ({"since": "0"}, ["alerts"], "since", "not_applicable"),
    ],
)
def test_bad_alert_cursors_are_rejected_without_changing_the_subscription(
    env: Any,
    extra: dict[str, Any],
    channels: list[str],
    field: str,
    reason: str | None,
) -> None:
    with server(env) as client, connect(client, env) as wire:
        wire.send({"op": "subscribe", "channels": channels, "markets": [A], **extra})
        error = wire.recv()
        assert error["type"] == "error" and error["error"] == "invalid_parameter"
        assert error["fields"] == [field] and error.get("reason") == reason


def test_the_evaluator_to_the_socket_end_to_end(env: Any, alerts: Alerts) -> None:
    """A rule, a matching analyzer result, one evaluator run: the alert is pushed to the owner."""

    async def setup() -> int:
        engine = db.make_engine(env["url"])
        try:
            async with engine.begin() as conn:
                market_id = env["ids"][A]
                await conn.execute(
                    text(
                        "insert into alert_rules (user_id, name, analyzer, metric, market_id, op, "
                        "threshold_e6, cooldown_seconds) values (:u, 'wide', 'market_quotes', "
                        "'spread', :m, 'gt', 50000, 300)"
                    ),
                    {"u": alerts.users["alice"], "m": market_id},
                )
                await conn.execute(
                    text(
                        "insert into analysis_results (ts, analyzer, metric, market_id, value_e6) "
                        "values (:t, 'market_quotes', 'spread', :m, 80000)"
                    ),
                    {"t": NOW, "m": market_id},
                )
            return int(market_id)
        finally:
            await engine.dispose()

    async def evaluate() -> None:
        engine = db.make_engine(env["url"])
        try:
            await AlertEvaluator().run(AnalysisContext(engine, NOW + timedelta(seconds=1)))
        finally:
            await engine.dispose()

    _ = alerts.users  # looked up here, not inside setup()'s event loop
    asyncio.run(setup())
    with server(env) as client, connect(client, env) as wire:
        subscribe(wire, ["alerts"], [])
        asyncio.run(evaluate())
        event = wire.recv()
    assert event["type"] == "alert" and event["rule_name"] == "wide" and event["ticker"] == A
    assert event["metric"] == "market_quotes.spread" and event["value"] == "0.080000"
