import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from kalshi_core.models import Trade
from kalshi_core.ws import RECONNECTED
from kalshi_core.ws_models import WsMessage
from kalshiterm_server import db
from kalshiterm_server.ingest.backfill import GapBackfiller
from kalshiterm_server.ingest.stream import GapInfo, StreamIngestor
from sqlalchemy import text
from test_stream import T_MS, Gate, rows, scalar, ticker_msg, trade_msg, until

pytestmark = pytest.mark.db

T0 = datetime.fromtimestamp(T_MS / 1000, UTC)


def rest_trade(
    tid: str | None = None,
    ticker: str = "KXA-E1-X",
    at: timedelta = timedelta(0),
    count: str = "3.00",
    price: str = "0.5600",
) -> Trade:
    return Trade(
        trade_id=tid or str(uuid.uuid4()),
        ticker=ticker,
        yes_price_dollars=Decimal(price),
        no_price_dollars=Decimal("1") - Decimal(price),
        count_fp=Decimal(count),
        taker_side="yes",
        created_time=T0 + at,
    )


class FakeRest:
    def __init__(self, trades: list[Trade], fail_times: int = 0) -> None:
        self.trades = trades
        self.fail_times = fail_times
        self.calls: list[dict[str, Any]] = []

    async def iter_trades(self, **params: Any) -> AsyncIterator[Trade]:
        self.calls.append(params)
        if self.fail_times > 0:
            self.fail_times -= 1
            raise ConnectionError("rest down")
        for trade in self.trades:
            if params["min_ts"] <= trade.created_time.timestamp() <= params["max_ts"]:
                yield trade


class Env:
    def __init__(self, url: str, rest: FakeRest, **kwargs: Any) -> None:
        async def instant(_: float) -> None:
            return

        self.engine = db.make_engine(url)
        self.gate = Gate()
        self.ingestor = StreamIngestor(self.gate.__aiter__(), self.engine, flush_interval=0.05)
        self.rest = rest
        self.backfiller = GapBackfiller(
            rest,  # type: ignore[arg-type]
            self.engine,
            self.ingestor,
            sleep=instant,
            clock=lambda: T0 + timedelta(hours=1),
            **kwargs,
        )
        self.tasks = [
            asyncio.create_task(self.ingestor.run()),
            asyncio.create_task(self.backfiller.run()),
        ]

    async def stop(self) -> None:
        self.gate.close()
        await self.tasks[0]
        self.tasks[1].cancel()
        await asyncio.gather(self.tasks[1], return_exceptions=True)
        await self.engine.dispose()


def gap(start: timedelta, end: timedelta, reason: str = "connection_lost") -> GapInfo:
    return GapInfo(started=T0 + start, ended=T0 + end, reason=reason, dropped=0)


async def gap_rows(url: str) -> list[tuple[Any, ...]]:
    return await rows(
        url,
        "select reason, status, trades_found, trades_added, duplicates, combo_large_added, "
        "combo_skipped, note from ingest_gaps order by id",
    )


async def test_missed_trades_are_added_and_already_stored_or_live_ones_are_not_repeated(
    migrated_db_url: str,
) -> None:
    stored = trade_msg(offset_ms=1_000)  # arrives live and is written before the gap is handled
    env = Env(migrated_db_url, FakeRest([]))
    async with asyncio.timeout(30):
        env.gate.put(stored)
        await until(lambda: env.ingestor.written["trades"] == 1)
        live_id = stored.msg["trade_id"]
        missed = [rest_trade(at=timedelta(seconds=s)) for s in (2, 3, 4)]
        env.rest.trades = [rest_trade(live_id, at=timedelta(seconds=1)), *missed]
        env.backfiller.report(gap(timedelta(seconds=1), timedelta(seconds=5)))
        await until(lambda: env.backfiller.processed == 1)
        await until(lambda: env.ingestor.written["trades"] == 4)
        await env.stop()

    assert await scalar(migrated_db_url, "select count(*) from trades") == 4
    assert await scalar(migrated_db_url, "select count(distinct trade_id) from trades") == 4
    assert await gap_rows(migrated_db_url) == [("connection_lost", "done", 4, 3, 1, 0, 0, "")]
    call = env.rest.calls[0]  # the window is padded by 10 s on each side, in whole seconds
    assert (call["min_ts"], call["max_ts"]) == (T_MS // 1000 + 1 - 10, T_MS // 1000 + 5 + 10)


async def test_backfilled_values_are_exact_and_keep_the_exchange_time(
    migrated_db_url: str,
) -> None:
    env = Env(migrated_db_url, FakeRest([rest_trade(at=timedelta(microseconds=1_234_567))]))
    async with asyncio.timeout(30):
        env.backfiller.report(gap(timedelta(0), timedelta(seconds=3)))
        await until(lambda: env.ingestor.written["trades"] == 1)
        await env.stop()
    found = await rows(
        migrated_db_url,
        "select yes_price_e6, count_e2, taker_side from trades",
    )
    assert found == [(560000, 300, "yes")]
    ts = await scalar(migrated_db_url, "select extract(microsecond from ts)::int from trades")
    assert ts == 1_234_567  # seconds and microseconds of the exchange time, untouched


async def test_combo_trades_only_reach_the_large_trade_log_and_never_the_counters(
    migrated_db_url: str,
) -> None:
    big = rest_trade(ticker="KXMVEFOO-S1-A", count="2000.00", price="0.5000")  # $1000 paid
    small = rest_trade(ticker="KXMVEFOO-S1-B", count="10.00")
    env = Env(migrated_db_url, FakeRest([big, small]))
    async with asyncio.timeout(30):
        env.backfiller.report(gap(timedelta(0), timedelta(seconds=3)))
        await until(lambda: env.backfiller.processed == 1)
        await env.stop()
    assert await scalar(migrated_db_url, "select count(*) from combo_large_trades") == 1
    assert await scalar(migrated_db_url, "select count(*) from trades") == 0
    assert await scalar(migrated_db_url, "select count(*) from combo_stats_1m") == 0
    assert await gap_rows(migrated_db_url) == [("connection_lost", "done", 2, 0, 0, 1, 1, "")]


async def test_running_the_same_gap_twice_adds_nothing_the_second_time(
    migrated_db_url: str,
) -> None:
    trades = [rest_trade(at=timedelta(seconds=s)) for s in (1, 2)]
    env = Env(migrated_db_url, FakeRest(trades))
    async with asyncio.timeout(30):
        env.backfiller.report(gap(timedelta(0), timedelta(seconds=3)))
        await until(lambda: env.ingestor.written["trades"] == 2)
        env.backfiller.report(gap(timedelta(0), timedelta(seconds=3)))
        await until(lambda: env.backfiller.processed == 2)
        await env.stop()
    assert await scalar(migrated_db_url, "select count(*) from trades") == 2
    assert [r[1:5] for r in await gap_rows(migrated_db_url)] == [
        ("done", 2, 2, 0),
        ("done", 2, 0, 2),
    ]


async def test_a_rest_failure_is_retried_then_recorded_as_failed(migrated_db_url: str) -> None:
    flaky = Env(migrated_db_url, FakeRest([rest_trade(at=timedelta(seconds=1))], fail_times=2))
    async with asyncio.timeout(30):
        flaky.backfiller.report(gap(timedelta(0), timedelta(seconds=3)))
        await until(lambda: flaky.backfiller.processed == 1)
        await flaky.stop()
    assert await gap_rows(migrated_db_url) == [
        ("connection_lost", "done", 1, 1, 0, 0, 0, "3 attempts")
    ]

    down = Env(migrated_db_url, FakeRest([], fail_times=99))
    async with asyncio.timeout(30):
        down.backfiller.report(gap(timedelta(0), timedelta(seconds=3)))
        await until(lambda: down.backfiller.processed == 1)
        await down.stop()
    last = (await gap_rows(migrated_db_url))[-1]
    assert last[1] == "failed" and "ConnectionError: rest down" in last[7]


async def test_a_very_long_outage_is_truncated_and_says_so(migrated_db_url: str) -> None:
    env = Env(migrated_db_url, FakeRest([]), max_window=timedelta(hours=1))
    async with asyncio.timeout(30):
        env.backfiller.report(gap(timedelta(hours=-10), timedelta(0)))
        await until(lambda: env.backfiller.processed == 1)
        await env.stop()
    call = env.rest.calls[0]
    assert call["min_ts"] == T_MS // 1000 - 3600
    assert "truncated" in (await gap_rows(migrated_db_url))[0][7]


async def test_a_reconnect_message_opens_a_gap_from_the_last_message_heard(
    migrated_db_url: str,
) -> None:
    env = Env(migrated_db_url, FakeRest([rest_trade(at=timedelta(seconds=2))]))
    reconnect = WsMessage(
        type=RECONNECTED,
        msg={"reason": "overflow", "dropped": 7, "resubscribed": [], "failed": []},
        received_at=T_MS / 1000 + 5,
    )
    async with asyncio.timeout(30):
        env.gate.put(ticker_msg(offset_ms=1_000), reconnect)
        await until(lambda: env.backfiller.processed == 1)
        await env.stop()
    row = await rows(
        migrated_db_url, "select reason, dropped, started_at, ended_at from ingest_gaps"
    )
    reason, dropped, started, ended = row[0]
    assert (reason, dropped) == ("overflow", 7)
    assert (ended - started).total_seconds() == pytest.approx(
        3.75
    )  # heard at +1.25 s, back at +5 s
    assert await scalar(migrated_db_url, "select count(*) from trades") == 1


async def test_starting_queues_the_gap_since_the_last_stored_trade_and_marks_old_runs(
    migrated_db_url: str,
) -> None:
    engine = db.make_engine(migrated_db_url)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "insert into ingest_gaps (started_at, ended_at, reason, status) "
                "values (now(), now(), 'connection_lost', 'running')"
            )
        )
        await conn.execute(
            text(
                "insert into markets (ticker, event_ticker, market_type, status) "
                "values ('KXA-E1-X', 'KXA-E1', 'binary', 'active')"
            )
        )
        await conn.execute(
            text(
                "insert into trades (ts, received_at, market_id, trade_id, yes_price_e6, "
                "count_e2) select :ts, :ts, id, :t, 500000, 100 from markets"
            ),
            {"ts": T0 - timedelta(minutes=20), "t": uuid.uuid4()},
        )
    await engine.dispose()

    env = Env(migrated_db_url, FakeRest([rest_trade(at=timedelta(minutes=-10))]))
    async with asyncio.timeout(30):
        await env.backfiller.start()
        await until(lambda: env.backfiller.processed == 1)
        await env.stop()
    statuses = await rows(migrated_db_url, "select reason, status, trades_added from ingest_gaps")
    assert statuses == [("connection_lost", "interrupted", 0), ("startup", "done", 1)]


async def test_a_fresh_database_has_no_startup_gap(migrated_db_url: str) -> None:
    env = Env(migrated_db_url, FakeRest([]))
    await env.backfiller.start()
    await env.stop()
    assert await scalar(migrated_db_url, "select count(*) from ingest_gaps") == 0
    assert env.rest.calls == []


async def test_backfilled_trades_of_a_watched_market_also_reach_the_permanent_copy(
    migrated_db_url: str,
) -> None:
    env = Env(migrated_db_url, FakeRest([rest_trade(at=timedelta(seconds=2))]))
    async with asyncio.timeout(30):
        env.ingestor.watch(["KXA-E1-X"])
        await until(lambda: "KXA-E1-X" in env.ingestor.watched)
        env.backfiller.report(gap(timedelta(0), timedelta(seconds=3)))
        await until(lambda: env.ingestor.written["trades_watchlist"] == 1)
        await env.stop()
    assert await scalar(migrated_db_url, "select count(*) from trades_watchlist") == 1


async def test_a_trade_in_the_sliver_the_second_rounding_adds_is_recognised_as_stored(
    migrated_db_url: str,
) -> None:
    """Found live: REST returns whole seconds, so it hands back trades just before the window
    start that an exact-timestamp lookup of stored ids did not cover; they were stored twice.
    The trade is written straight to the database, as a *previous* process would have left it
    (the running ingestor's own memory of recent ids would otherwise hide the problem)."""
    trade_id = str(uuid.uuid4())
    engine = db.make_engine(migrated_db_url)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO markets (ticker, event_ticker, market_type, status) "
                "VALUES ('KXA-E1-X', 'KXA-E1', 'binary', 'active')"
            )
        )
        await conn.execute(
            text(
                "INSERT INTO trades (ts, received_at, market_id, trade_id, yes_price_e6, "
                "count_e2) SELECT :ts, :ts, id, :t, 560000, 300 FROM markets"
            ),
            {"ts": T0 - timedelta(seconds=8.8), "t": uuid.UUID(trade_id)},
        )
    await engine.dispose()

    env = Env(migrated_db_url, FakeRest([rest_trade(trade_id, at=timedelta(seconds=-8.8))]))
    async with asyncio.timeout(30):
        # gap started at +1.5 s, so the padded window starts at -8.5 s: REST rounds down to -9 s
        env.backfiller.report(gap(timedelta(seconds=1.5), timedelta(seconds=5)))
        await until(lambda: env.backfiller.processed == 1)
        await env.stop()
    assert env.rest.calls[0]["min_ts"] == T_MS // 1000 - 9  # the sliver really was requested
    assert await rows_and_ids(migrated_db_url) == (1, 1)  # one row, not two
    assert (await gap_rows(migrated_db_url))[0][1:5] == ("done", 1, 0, 1)  # found 1, added 0, dup 1


async def rows_and_ids(url: str) -> tuple[int, int]:
    return (
        await scalar(url, "select count(*) from trades"),
        await scalar(url, "select count(distinct trade_id) from trades"),
    )
