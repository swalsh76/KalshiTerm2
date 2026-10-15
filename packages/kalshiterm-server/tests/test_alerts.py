"""Alert rules and the evaluator against a real TimescaleDB."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg  # type: ignore[import-untyped]
import pytest
from kalshiterm_server import alerts, auth, db
from kalshiterm_server.analytics.base import AnalysisContext
from kalshiterm_server.analytics.builtin.alert_evaluator import CHANNEL, AlertEvaluator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from test_stream import rows, scalar
from timescale_jobs import quiet_background_jobs

pytestmark = pytest.mark.db

NOW = datetime(2026, 10, 12, 12, 0, tzinfo=UTC)
MIN_COOLDOWN = 60


@pytest.fixture
async def engine(migrated_db_url: str) -> AsyncIterator[AsyncEngine]:
    engine = db.make_engine(migrated_db_url)
    await quiet_background_jobs(engine)
    yield engine
    await engine.dispose()


class World:
    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine
        self.users: dict[str, int] = {}
        self.markets: dict[str, int] = {}

    async def user(self, name: str) -> int:
        await auth.add_user(self.engine, name)
        async with self.engine.connect() as conn:
            self.users[name] = (
                await conn.execute(text("select id from users where name = :n"), {"n": name})
            ).scalar_one()
        return self.users[name]

    async def market(self, ticker: str, watched_by: tuple[str, ...] = ()) -> int:
        async with self.engine.begin() as conn:
            await conn.execute(
                text(
                    "insert into events (event_ticker, series_ticker, title, mutually_exclusive) "
                    "values ('KX-E1', 'KX', 't', true) on conflict do nothing"
                )
            )
            market_id: int = (
                await conn.execute(
                    text(
                        "insert into markets (ticker, event_ticker, market_type, status) "
                        "values (:t, 'KX-E1', 'binary', 'active') returning id"
                    ),
                    {"t": ticker},
                )
            ).scalar_one()
            for name in watched_by:
                await conn.execute(
                    text("insert into user_watchlists (user_id, market_id) values (:u, :m)"),
                    {"u": self.users[name], "m": market_id},
                )
        self.markets[ticker] = market_id
        return market_id

    async def result(
        self, key: str, value: int, *, market: str | None = None, event: str | None = None,
        at: datetime = NOW,
    ) -> None:  # fmt: skip
        analyzer, metric = key.split(".", 1)
        async with self.engine.begin() as conn:
            await conn.execute(
                text(
                    "insert into analysis_results (ts, analyzer, metric, market_id, event_ticker, "
                    "value_e6) values (:ts, :a, :m, :mk, :ev, :v)"
                ),
                {"ts": at, "a": analyzer, "m": metric, "mk": self.markets.get(market or ""),
                 "ev": event, "v": value},
            )  # fmt: skip

    async def rule(
        self, who: str, key: str, subject: str, op: str, threshold: int, **kw: Any
    ) -> int:
        rule = await alerts.create_rule(
            self.engine,
            self.users[who],
            key=key,
            subject=subject,
            op=op,
            threshold_e6=threshold,
            cooldown_seconds=kw.get("cooldown", 300),
            name=kw.get("name", ""),
            max_rules=kw.get("max_rules", 50),
            min_cooldown=MIN_COOLDOWN,
        )
        return rule.id

    async def evaluate(self, now: datetime = NOW) -> None:
        assert await AlertEvaluator().run(AnalysisContext(self.engine, now)) == []

    async def alert_count(self, who: str | None = None) -> int:
        where = f"where user_id = {self.users[who]}" if who else ""
        return int(await scalar_for(self.engine, f"select count(*) from alerts {where}"))


async def scalar_for(engine: AsyncEngine, sql: str) -> Any:
    async with engine.connect() as conn:
        return (await conn.execute(text(sql))).scalar_one()


@pytest.fixture
async def world(engine: AsyncEngine) -> World:
    w = World(engine)
    await w.user("alice")
    await w.user("bob")
    await w.market("KX-E1-A", watched_by=("alice", "bob"))
    return w


async def test_a_rule_fires_once_when_its_condition_becomes_true_and_not_again(
    world: World,
) -> None:
    rule_id = await world.rule("alice", "market_moves.change_15m", "KX-E1-A", "abs_gt", 100_000,
                               name="big move")  # fmt: skip
    await world.result("market_moves.change_15m", 50_000, market="KX-E1-A")
    await world.evaluate()
    assert await world.alert_count() == 0  # 5 cents is under the 10-cent threshold

    later = NOW + timedelta(seconds=30)
    await world.result("market_moves.change_15m", -150_000, market="KX-E1-A", at=later)
    await world.evaluate(later)
    [alert] = await rows(
        world.engine.url.render_as_string(hide_password=False),
        "select rule_id, rule_name, metric, op, threshold_e6, value_e6, fired_at, acknowledged_at "
        "from alerts",
    )
    assert alert == (rule_id, "big move", "change_15m", "abs_gt", 100_000, -150_000, later, None)

    again = later + timedelta(seconds=30)
    await world.result("market_moves.change_15m", -160_000, market="KX-E1-A", at=again)
    await world.evaluate(again)  # still true: no second alert
    assert await world.alert_count() == 1


async def test_it_is_re_armed_when_the_condition_lapses_and_fires_again_after_the_cooldown(
    world: World,
) -> None:
    await world.rule("alice", "market_quotes.spread", "KX-E1-A", "gt", 50_000, cooldown=300)
    t = NOW
    await world.result("market_quotes.spread", 80_000, market="KX-E1-A", at=t)
    await world.evaluate(t)
    assert await world.alert_count() == 1

    t1 = t + timedelta(seconds=60)  # condition lapses
    await world.result("market_quotes.spread", 20_000, market="KX-E1-A", at=t1)
    await world.evaluate(t1)
    assert await scalar_for(world.engine, "select active from alert_rules") is False

    t2 = t + timedelta(seconds=120)  # true again, but only 2 minutes after the first alert
    await world.result("market_quotes.spread", 90_000, market="KX-E1-A", at=t2)
    await world.evaluate(t2)
    assert await world.alert_count() == 1  # cooling down

    t3 = t + timedelta(seconds=301)  # still true when the cooldown ends -> fires
    await world.result("market_quotes.spread", 90_000, market="KX-E1-A", at=t3)
    await world.evaluate(t3)
    assert await world.alert_count() == 2


async def test_a_result_that_is_too_old_does_not_count_and_re_arms_the_rule(
    world: World,
) -> None:
    await world.rule("alice", "market_moves.last", "KX-E1-A", "gt", 800_000)
    await world.result("market_moves.last", 900_000, market="KX-E1-A", at=NOW)
    await world.evaluate(NOW)
    assert await world.alert_count() == 1
    # 4 minutes later the only result is 4 minutes old; moves are fresh for 3 (3 x 60 s)
    await world.evaluate(NOW + timedelta(minutes=4))
    assert await scalar_for(world.engine, "select active from alert_rules") is False


async def test_a_missing_result_never_fires(world: World) -> None:
    await world.rule("alice", "market_moves.last", "KX-E1-A", "lt", 800_000)
    await world.evaluate()
    assert await world.alert_count() == 0  # no data is not "below 80 cents"


async def test_each_user_gets_their_own_alert_from_the_same_result(world: World) -> None:
    await world.rule("alice", "market_moves.last", "KX-E1-A", "gt", 500_000, name="alice's")
    await world.rule("bob", "market_moves.last", "KX-E1-A", "gt", 900_000, name="bob's")
    await world.result("market_moves.last", 700_000, market="KX-E1-A")
    await world.evaluate()
    assert await world.alert_count("alice") == 1 and await world.alert_count("bob") == 0
    await world.result(
        "market_moves.last", 950_000, market="KX-E1-A", at=NOW + timedelta(seconds=10)
    )
    await world.evaluate(NOW + timedelta(seconds=10))
    assert await world.alert_count("alice") == 1  # alice's is already active
    assert await world.alert_count("bob") == 1


async def test_event_rules_follow_the_arbitrage_profit(world: World) -> None:
    await world.rule("alice", "arbitrage_bid.profit", "KX-E1", "gt", 0)
    await world.result("arbitrage_bid.profit", -260_000, event="KX-E1")
    await world.evaluate()
    assert await world.alert_count() == 0  # fees ate the edge
    t = NOW + timedelta(seconds=30)
    await world.result("arbitrage_bid.profit", 20_000, event="KX-E1", at=t)
    await world.evaluate(t)
    assert await world.alert_count() == 1
    t2 = t + timedelta(minutes=5)  # the opportunity disappears: no result at all
    await world.evaluate(t2)
    assert await scalar_for(world.engine, "select active from alert_rules") is False


async def test_disabled_rules_are_ignored(world: World) -> None:
    rule_id = await world.rule("alice", "market_moves.last", "KX-E1-A", "gt", 1)
    await alerts.update_rule(
        world.engine, world.users["alice"], rule_id, {"enabled": False}, MIN_COOLDOWN
    )
    await world.result("market_moves.last", 900_000, market="KX-E1-A")
    await world.evaluate()
    assert await world.alert_count() == 0


async def test_running_twice_for_the_same_moment_does_not_double_fire(world: World) -> None:
    await world.rule("alice", "market_moves.last", "KX-E1-A", "gt", 1)
    await world.result("market_moves.last", 900_000, market="KX-E1-A")
    await asyncio.gather(world.evaluate(), world.evaluate())
    assert await world.alert_count() == 1


async def test_the_new_alert_is_announced_to_listeners(migrated_db_url: str, world: World) -> None:
    listener = await asyncpg.connect(
        migrated_db_url.replace("postgresql+asyncpg://", "postgresql://")
    )
    heard: asyncio.Queue[str] = asyncio.Queue()
    await listener.add_listener(CHANNEL, lambda _c, _p, _ch, payload: heard.put_nowait(payload))
    try:
        await world.rule("alice", "market_moves.last", "KX-E1-A", "gt", 1)
        await world.result("market_moves.last", 900_000, market="KX-E1-A")
        await world.evaluate()
        async with asyncio.timeout(30):
            payload = await heard.get()
    finally:
        await listener.close()
    alert_id = await scalar(migrated_db_url, "select id from alerts")
    assert payload == f"{world.users['alice']}:{alert_id}"


async def test_an_alert_outlives_its_rule_and_old_alerts_are_pruned(world: World) -> None:
    rule_id = await world.rule("alice", "market_moves.last", "KX-E1-A", "gt", 1, name="gone soon")
    await world.result("market_moves.last", 900_000, market="KX-E1-A")
    await world.evaluate()
    await alerts.delete_rule(world.engine, world.users["alice"], rule_id)
    [kept] = await rows_for(world.engine, "select rule_id, rule_name from alerts")
    assert kept == (None, "gone soon")
    async with world.engine.begin() as conn:
        await conn.execute(
            text(
                "insert into alerts (user_id, fired_at, analyzer, metric, op, threshold_e6, "
                "value_e6) values (:u, :t, 'a', 'm', 'gt', 1, 2)"
            ),
            {"u": world.users["alice"], "t": NOW - timedelta(days=91)},
        )
    assert await world.alert_count() == 2
    await world.evaluate()
    assert await world.alert_count() == 1  # the 91-day-old one is gone


async def rows_for(engine: AsyncEngine, sql: str) -> list[tuple[Any, ...]]:
    async with engine.connect() as conn:
        return [tuple(r) for r in await conn.execute(text(sql))]


# ------------------------------------------------------------------ creating rules


async def test_rule_creation_enforces_its_checks_and_changes_nothing_when_refused(
    world: World,
) -> None:
    await world.market("KX-E1-B")  # exists but nobody watches it
    await world.market("KXMVECROSS-1", watched_by=("alice",))
    async with world.engine.begin() as conn:
        await conn.execute(
            text(
                "insert into markets (ticker, event_ticker, market_type, status, settlement_ts) "
                "values ('KX-DONE', 'KX-E1', 'binary', 'finalized', now())"
            )
        )
        done_id = (
            await conn.execute(text("select id from markets where ticker='KX-DONE'"))
        ).scalar_one()
        await conn.execute(
            text("insert into user_watchlists (user_id, market_id) values (:u, :m)"),
            {"u": world.users["alice"], "m": done_id},
        )
        await conn.execute(
            text(
                "insert into events (event_ticker, series_ticker, title, mutually_exclusive) "
                "values ('KX-NOEXCL', 'KX', 't', false)"
            )
        )
    bad: list[tuple[str, str, str, int, int, str, int]] = [
        # key, subject, op, threshold, cooldown, expected code, status
        ("market_moves.nope", "KX-E1-A", "gt", 1, 300, "unknown_metric", 422),
        ("market_moves.volume_1h", "KX-E1-A", "lt", 1, 300, "invalid_comparison", 422),
        ("market_moves.change_1h", "KX-E1-A", "abs_gt", 0, 300, "invalid_parameter", 422),
        ("market_moves.change_1h", "KX-E1-A", "gt", 1, 59, "cooldown_too_short", 422),
        ("market_moves.last", "KX-E1-B", "gt", 1, 300, "not_watched", 422),
        ("market_moves.last", "KX-NOPE", "gt", 1, 300, "not_found", 404),
        ("market_moves.last", "KXMVECROSS-1", "gt", 1, 300, "not_alertable", 422),
        ("market_moves.last", "KX-DONE", "gt", 1, 300, "not_alertable", 422),
        ("arbitrage_bid.profit", "KX-NOEXCL", "gt", 1, 300, "not_alertable", 422),
        ("arbitrage_bid.profit", "KX-NOPE", "gt", 1, 300, "not_found", 404),
    ]  # fmt: skip
    for key, subject, op, threshold, cooldown, code, status in bad:
        with pytest.raises(alerts.AlertError) as caught:
            await world.rule("alice", key, subject, op, threshold, cooldown=cooldown)
        assert (caught.value.code, caught.value.status) == (code, status), (key, subject, op)
    assert await scalar_for(world.engine, "select count(*) from alert_rules") == 0


async def test_the_per_user_limit_holds_under_simultaneous_requests(world: World) -> None:
    results = await asyncio.gather(
        *[
            world.rule("alice", "market_moves.last", "KX-E1-A", "gt", n + 1, max_rules=3)
            for n in range(8)
        ],
        return_exceptions=True,
    )
    assert sum(isinstance(r, int) for r in results) == 3
    assert all(
        isinstance(r, alerts.AlertError) and r.code == "rule_limit"
        for r in results
        if not isinstance(r, int)
    )  # noqa: E501
    assert await world.rule(
        "bob", "market_moves.last", "KX-E1-A", "gt", 1, max_rules=3
    )  # not shared
