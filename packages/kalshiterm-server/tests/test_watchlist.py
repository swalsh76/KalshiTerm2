import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from kalshiterm_server import db
from kalshiterm_server.ingest.stream import StreamIngestor
from kalshiterm_server.ingest.watchlist import (
    WatchlistConfig,
    WatchlistController,
    top_by_volume,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from test_stream import Gate, rows, scalar, trade_msg, until

pytestmark = pytest.mark.db

NOW = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)


@pytest.fixture
async def engine(migrated_db_url: str) -> AsyncIterator[AsyncEngine]:
    engine = db.make_engine(migrated_db_url)
    yield engine
    await engine.dispose()


async def add_market(engine: AsyncEngine, ticker: str, status: str = "active") -> int:
    async with engine.begin() as conn:
        market_id: int = (
            await conn.execute(
                text(
                    "insert into markets (ticker, event_ticker, market_type, status) "
                    "values (:t, :e, 'binary', :s) returning id"
                ),
                {"t": ticker, "e": ticker.rpartition("-")[0], "s": status},
            )
        ).scalar_one()
        return market_id


async def add_trades(
    engine: AsyncEngine, ticker: str, count_e2: int, age: timedelta, n: int = 1
) -> None:
    async with engine.begin() as conn:
        for _ in range(n):
            await conn.execute(
                text(
                    "insert into trades (ts, received_at, market_id, trade_id, yes_price_e6, "
                    "count_e2) select :ts, :ts, id, :tid, 500000, :c from markets "
                    "where ticker = :t"
                ),
                {"ts": NOW - age, "tid": uuid.uuid4(), "c": count_e2, "t": ticker},
            )


# ---------------------------------------------------------------- the trade copy


class Running:
    """An ingestor on a hand-fed stream, so a test can interleave watch calls and trades."""

    def __init__(self, url: str) -> None:
        self.engine = db.make_engine(url)
        self.gate = Gate()
        self.ingestor = StreamIngestor(self.gate.__aiter__(), self.engine, flush_interval=0.05)
        self.task = asyncio.create_task(self.ingestor.run())

    async def stop(self) -> None:
        self.gate.close()
        await self.task
        await self.engine.dispose()


async def trade_ids(url: str, table: str) -> list[str]:
    found = await rows(url, f"select trade_id::text from {table} order by ts, trade_id")
    return [r[0] for r in found]


async def test_watching_copies_the_recent_history_then_writes_new_trades_to_both_tables(
    migrated_db_url: str,
) -> None:
    run = Running(migrated_db_url)
    async with asyncio.timeout(30):
        old = trade_msg(offset_ms=0)
        run.gate.put(old, trade_msg("KXOTHER-E1-X", offset_ms=1))
        await until(lambda: run.ingestor.written["trades"] == 2)

        run.ingestor.watch(["KXA-E1-X"])
        await until(lambda: "KXA-E1-X" in run.ingestor.watched)
        assert await trade_ids(migrated_db_url, "trades_watchlist") == [old.msg["trade_id"]]

        new = trade_msg(offset_ms=1_000)
        run.gate.put(new)
        await until(lambda: run.ingestor.written["trades_watchlist"] == 1)
        await run.stop()

    assert await trade_ids(migrated_db_url, "trades_watchlist") == [
        old.msg["trade_id"],
        new.msg["trade_id"],
    ]
    assert await scalar(migrated_db_url, "select count(*) from trades") == 3
    period = await rows(
        migrated_db_url, "select ticker, source, removed_at from watchlist_periods_v"
    )
    assert period == [("KXA-E1-X", "manual", None)]


async def test_a_trade_in_the_same_batch_as_the_watch_is_written_once_to_each_table(
    migrated_db_url: str,
) -> None:
    run = Running(migrated_db_url)
    async with asyncio.timeout(30):
        # Queue the watch while the first trade is still in the intake buffer.
        run.ingestor.watch(["KXA-E1-X"])
        first = trade_msg(offset_ms=0)
        run.gate.put(first)
        await until(lambda: run.ingestor.written["trades"] == 1)
        await until(lambda: "KXA-E1-X" in run.ingestor.watched)
        await run.stop()
    assert await trade_ids(migrated_db_url, "trades") == [first.msg["trade_id"]]
    assert await trade_ids(migrated_db_url, "trades_watchlist") == [first.msg["trade_id"]]


async def test_unwatching_closes_the_period_keeps_the_copy_and_a_rewatch_fills_the_gap_once(
    migrated_db_url: str,
) -> None:
    run = Running(migrated_db_url)
    sent = []
    async with asyncio.timeout(30):
        sent.append(trade_msg(offset_ms=0))
        run.gate.put(sent[0])
        await until(lambda: run.ingestor.written["trades"] == 1)
        run.ingestor.watch(["KXA-E1-X"])
        await until(lambda: "KXA-E1-X" in run.ingestor.watched)

        run.ingestor.unwatch(["KXA-E1-X"])
        await until(lambda: "KXA-E1-X" not in run.ingestor.watched)
        sent.append(trade_msg(offset_ms=1_000))  # while unwatched: ordinary table only
        run.gate.put(sent[1])
        await until(lambda: run.ingestor.written["trades"] == 2)
        assert await trade_ids(migrated_db_url, "trades_watchlist") == [sent[0].msg["trade_id"]]

        run.ingestor.watch(["KXA-E1-X"], "auto")
        await until(lambda: "KXA-E1-X" in run.ingestor.watched)
        await run.stop()

    assert await trade_ids(migrated_db_url, "trades_watchlist") == [
        m.msg["trade_id"] for m in sent
    ]  # the gap trade arrived by the history copy; nothing is duplicated
    periods = await rows(
        migrated_db_url,
        "select source, removed_at is not null from watchlist_periods_v order by id",
    )
    assert periods == [("manual", True), ("auto", False)]


async def test_watching_twice_opens_one_period_and_copies_nothing_twice(
    migrated_db_url: str,
) -> None:
    run = Running(migrated_db_url)
    async with asyncio.timeout(30):
        first = trade_msg(offset_ms=0)
        run.gate.put(first)
        await until(lambda: run.ingestor.written["trades"] == 1)
        run.ingestor.watch(["KXA-E1-X"])
        run.ingestor.watch(["KXA-E1-X"])
        await until(lambda: "KXA-E1-X" in run.ingestor.watched)
        await run.stop()
    assert await scalar(migrated_db_url, "select count(*) from watchlist_periods") == 1
    assert await trade_ids(migrated_db_url, "trades_watchlist") == [first.msg["trade_id"]]


async def test_a_failed_write_keeps_the_watch_request_for_the_retry(migrated_db_url: str) -> None:
    async def instant(_: float) -> None:
        return

    engine = db.make_engine(migrated_db_url)
    gate = Gate()
    ingestor = StreamIngestor(gate.__aiter__(), engine, flush_interval=0.05, sleep=instant)
    real = ingestor._write  # noqa: SLF001
    calls = 0

    async def flaky(batch: Any, agg: Any) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("db down")
        await real(batch, agg)

    ingestor._write = flaky  # type: ignore[method-assign]  # noqa: SLF001
    task = asyncio.create_task(ingestor.run())
    async with asyncio.timeout(30):
        ingestor.watch(["KXA-E1-X"])
        await until(lambda: "KXA-E1-X" in ingestor.watched)
        gate.close()
        await task
    await engine.dispose()
    assert ingestor.retries == 1
    assert await scalar(migrated_db_url, "select count(*) from watchlist_periods") == 1


# ---------------------------------------------------------------- the controller


class FakeFeed:
    def __init__(self, bad: set[str] | None = None) -> None:
        self.bad = bad or set()
        self.calls: list[tuple[str, list[str]]] = []
        self.watching: list[str] = []

    async def add_markets(self, tickers: list[str]) -> None:
        self.calls.append(("add", list(tickers)))
        if self.bad & set(tickers):
            raise RuntimeError("market not found")
        self.watching += tickers

    async def remove_markets(self, tickers: list[str]) -> None:
        self.calls.append(("remove", list(tickers)))
        self.watching = [t for t in self.watching if t not in tickers]


class FakeRecorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[str], str]] = []

    def watch(self, tickers: list[str], source: str = "manual") -> None:
        self.calls.append(("watch", list(tickers), source))

    def unwatch(self, tickers: list[str]) -> None:
        self.calls.append(("unwatch", list(tickers), ""))


class Clock:
    def __init__(self) -> None:
        self.now = NOW

    def __call__(self) -> datetime:
        return self.now


async def seed_volumes(engine: AsyncEngine) -> None:
    for ticker in ("KXBIG-E1-X", "KXMID-E1-X", "KXSMALL-E1-X", "KXOLD-E1-X", "KXDONE-E1-X"):
        await add_market(engine, ticker, "finalized" if "DONE" in ticker else "active")
    await add_trades(engine, "KXBIG-E1-X", 100_000, timedelta(minutes=10), n=3)  # 3000 contracts
    await add_trades(engine, "KXMID-E1-X", 50_000, timedelta(minutes=20), n=2)  # 1000
    await add_trades(engine, "KXSMALL-E1-X", 100, timedelta(minutes=5))  # 1
    await add_trades(engine, "KXOLD-E1-X", 9_000_000, timedelta(hours=3))  # outside the window
    await add_trades(engine, "KXDONE-E1-X", 9_000_000, timedelta(minutes=5))  # not trading any more


async def test_top_by_volume_ranks_recent_contracts_and_skips_old_settled_and_combo_markets(
    engine: AsyncEngine,
) -> None:
    await seed_volumes(engine)
    await add_market(engine, "KXMVECROSS-E1-X")
    await add_trades(engine, "KXMVECROSS-E1-X", 9_000_000, timedelta(minutes=5))
    top = await top_by_volume(engine, 10, timedelta(hours=1), NOW)
    assert top == ["KXBIG-E1-X", "KXMID-E1-X", "KXSMALL-E1-X"]
    assert await top_by_volume(engine, 2, timedelta(hours=1), NOW) == top[:2]
    assert await top_by_volume(engine, 0, timedelta(hours=1), NOW) == []


async def test_the_controller_adds_manual_and_top_markets_then_holds_steady(
    engine: AsyncEngine,
) -> None:
    await seed_volumes(engine)
    feed, recorder, clock = FakeFeed(), FakeRecorder(), Clock()
    config = WatchlistConfig(frozenset({"KXOLD-E1-X"}), auto_top_n=2)
    controller = WatchlistController(engine, feed, recorder, lambda: config, clock=clock)

    await controller.reconcile()
    assert feed.calls == [("add", ["KXOLD-E1-X", "KXBIG-E1-X", "KXMID-E1-X"])]
    assert recorder.calls == [
        ("watch", ["KXOLD-E1-X"], "manual"),
        ("watch", ["KXBIG-E1-X"], "auto"),
        ("watch", ["KXMID-E1-X"], "auto"),
    ]
    await controller.reconcile()
    assert len(feed.calls) == 1  # nothing to do the second time


async def test_an_auto_market_that_drops_out_stays_for_the_dwell_then_goes(
    engine: AsyncEngine,
) -> None:
    await seed_volumes(engine)
    feed, recorder, clock = FakeFeed(), FakeRecorder(), Clock()
    config = WatchlistConfig(auto_top_n=1)
    controller = WatchlistController(engine, feed, recorder, lambda: config, clock=clock)
    await controller.reconcile()
    assert feed.watching == ["KXBIG-E1-X"]

    clock.now += timedelta(hours=1)
    await add_trades(engine, "KXMID-E1-X", 100_000_000, timedelta(minutes=-30))  # busiest now
    await controller.reconcile()
    assert feed.watching == ["KXBIG-E1-X", "KXMID-E1-X"]  # BIG still inside its 12 hours

    clock.now += timedelta(hours=11, minutes=1)  # BIG is now over 12 h old; MID is not
    await add_trades(engine, "KXMID-E1-X", 100_000_000, timedelta(hours=-12))
    await controller.reconcile()
    assert feed.watching == ["KXMID-E1-X"]
    assert recorder.calls[-1] == ("unwatch", ["KXBIG-E1-X"], "")


async def test_editing_the_file_applies_live_and_manual_markets_are_not_auto_removed(
    engine: AsyncEngine,
) -> None:
    await seed_volumes(engine)
    feed, recorder, clock = FakeFeed(), FakeRecorder(), Clock()
    state = {"config": WatchlistConfig(frozenset({"KXSMALL-E1-X"}))}
    controller = WatchlistController(engine, feed, recorder, lambda: state["config"], clock=clock)
    await controller.reconcile()
    clock.now += timedelta(days=3)
    await controller.reconcile()
    assert feed.watching == ["KXSMALL-E1-X"]  # no top-N, no volume rule: still there

    state["config"] = WatchlistConfig(frozenset({"KXMID-E1-X"}))  # file edited
    await controller.reconcile()
    assert feed.watching == ["KXMID-E1-X"]  # the dropped manual market left at once


async def test_a_market_added_to_the_file_while_held_by_auto_becomes_manual(
    engine: AsyncEngine,
) -> None:
    await seed_volumes(engine)
    feed, recorder, clock = FakeFeed(), FakeRecorder(), Clock()
    state = {"config": WatchlistConfig(auto_top_n=1)}
    controller = WatchlistController(engine, feed, recorder, lambda: state["config"], clock=clock)
    await controller.reconcile()
    state["config"] = WatchlistConfig(frozenset({"KXBIG-E1-X"}), auto_top_n=0)
    clock.now += timedelta(days=2)
    await controller.reconcile()
    assert feed.watching == ["KXBIG-E1-X"]
    assert controller.current["KXBIG-E1-X"].source == "manual"


async def test_one_bad_ticker_does_not_stop_the_others_being_added(engine: AsyncEngine) -> None:
    feed, recorder = FakeFeed(bad={"KXNOPE-E1-X"}), FakeRecorder()
    config = WatchlistConfig(frozenset({"KXGOOD-E1-X", "KXNOPE-E1-X", "KXFINE-E1-X"}))
    controller = WatchlistController(engine, feed, recorder, lambda: config)
    await controller.reconcile()
    assert sorted(feed.watching) == ["KXFINE-E1-X", "KXGOOD-E1-X"]
    assert controller.failed == {"KXNOPE-E1-X"}
    assert [c[1] for c in recorder.calls] == [["KXFINE-E1-X"], ["KXGOOD-E1-X"]]


async def test_starting_closes_periods_a_previous_run_left_open(engine: AsyncEngine) -> None:
    market_id = await add_market(engine, "KXA-E1-X")
    async with engine.begin() as conn:
        await conn.execute(
            text("insert into watchlist_periods (market_id, source) values (:m, 'auto')"),
            {"m": market_id},
        )
    controller = WatchlistController(engine, FakeFeed(), FakeRecorder(), WatchlistConfig)
    await controller.start()
    async with engine.connect() as conn:
        open_periods = (
            await conn.execute(
                text("select count(*) from watchlist_periods where removed_at is null")
            )
        ).scalar_one()
    assert open_periods == 0
