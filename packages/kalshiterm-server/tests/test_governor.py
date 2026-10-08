import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from kalshi_core.orderbook import BookTracker
from kalshiterm_server import db
from kalshiterm_server.governor import (
    NORMAL,
    SHEDDING,
    TIGHTENED,
    Governor,
    Usage,
    measure_usage,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from test_orderbook_storage import delta_msg, events_for, snap_msg
from test_stream import scalar
from timescale_jobs import quiet_background_jobs

pytestmark = pytest.mark.db

GB = 1024**3
BUDGET = 100 * GB
T0 = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)


@pytest.fixture
async def engine(migrated_db_url: str) -> AsyncIterator[AsyncEngine]:
    engine = db.make_engine(migrated_db_url)
    await quiet_background_jobs(engine)
    yield engine
    await engine.dispose()


class Shed:
    shed_orderbook_deltas = False


class Disk:
    """A simulated disk: tests set how full the budget is, and the state of the drive."""

    def __init__(self) -> None:
        self.percent = 10.0
        self.free_fraction = 0.6
        self.error: str | None = None

    async def __call__(self) -> Usage:
        used = int(BUDGET * self.percent / 100)
        total = 1000 * GB
        return Usage(
            db_bytes=used,
            wal_bytes=0,
            tables={"trades": used},
            disk_total=None if self.error else total,
            disk_free=None if self.error else int(total * self.free_fraction),
            drive_error=self.error,
        )


class Clock:
    def __init__(self) -> None:
        self.now = T0

    def __call__(self) -> datetime:
        return self.now

    def step(self, minutes: float = 10) -> None:
        self.now += timedelta(minutes=minutes)


def make(engine: AsyncEngine, disk: Disk, shed: Shed | None = None) -> tuple[Governor, Clock]:
    clock = Clock()
    return Governor(engine, BUDGET, shedder=shed, measure=disk, clock=clock), clock


async def windows(engine: AsyncEngine) -> dict[str, str]:
    async with engine.connect() as conn:
        found = await conn.execute(
            text(
                "select hypertable_name, config->>'drop_after' from timescaledb_information.jobs "
                "where proc_name = 'policy_retention' and hypertable_name in "
                "('tickers', 'orderbook_deltas', 'trades', 'orderbook_snapshots') order by 1"
            )
        )
        return {name: window for name, window in found}


ORIGINAL = {
    "orderbook_deltas": "14 days",
    "orderbook_snapshots": "365 days",
    "tickers": "14 days",
    "trades": "30 days",
}


async def kinds(engine: AsyncEngine) -> list[str]:
    async with engine.connect() as conn:
        return [
            r[0] for r in await conn.execute(text("select kind from governor_events order by id"))
        ]


async def test_measuring_reports_the_real_footprint_by_table(
    engine: AsyncEngine, tmp_path: Path
) -> None:
    usage = await measure_usage(engine, str(tmp_path))
    actual = await scalar_of(engine, "select pg_database_size(current_database())")
    assert usage.db_bytes == pytest.approx(actual, rel=0.05)
    assert usage.wal_bytes is not None and usage.wal_bytes > 0
    for name in ("trades", "tickers", "orderbook_deltas", "candles_1m", "ticker_1h", "markets"):
        assert usage.tables[name] > 0, name
    assert not any(name.startswith("_materialized") for name in usage.tables)
    assert sum(usage.tables.values()) <= usage.db_bytes  # the rest is catalog and internals
    assert usage.drive_error is None and usage.disk_total and usage.disk_free is not None


async def scalar_of(engine: AsyncEngine, sql: str) -> Any:
    async with engine.connect() as conn:
        return (await conn.execute(text(sql))).scalar_one()


async def test_growing_data_is_visible_in_the_measurement(
    engine: AsyncEngine, tmp_path: Path
) -> None:
    before = await measure_usage(engine, str(tmp_path))
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "insert into tickers (ts, received_at, market_id, price_e6) "
                "select now() - (g || ' seconds')::interval, now(), 1, g from "
                "generate_series(1, 20000) g"
            )
        )
    after = await measure_usage(engine, str(tmp_path))
    assert after.tables["tickers"] > before.tables["tickers"]


async def test_the_full_cycle_normal_tightened_shedding_and_back_with_exact_restore(
    engine: AsyncEngine,
) -> None:
    disk, shed = Disk(), Shed()
    governor, clock = make(engine, disk, shed)

    async def step(percent: float) -> Any:
        disk.percent = percent
        clock.step()
        return await governor.cycle()

    assert (await step(50)).mode == NORMAL
    assert await windows(engine) == ORIGINAL and not shed.shed_orderbook_deltas

    d = await step(82)
    assert d.mode == TIGHTENED and d.previous == NORMAL
    assert await windows(engine) == {
        "orderbook_deltas": "7 days",
        "orderbook_snapshots": "365 days",  # snapshots are history, never tightened
        "tickers": "7 days",
        "trades": "15 days",
    }
    assert not shed.shed_orderbook_deltas

    assert (await step(86)).mode == TIGHTENED  # between 80 and 90: nothing more happens
    d = await step(91)
    assert d.mode == SHEDDING and shed.shed_orderbook_deltas
    assert (
        await scalar_of(engine, "select count(*) from ingest_gaps where reason = 'delta_shed'") == 1
    )

    first_end = await scalar_of(
        engine, "select ended_at from ingest_gaps where reason = 'delta_shed'"
    )
    assert (await step(87)).mode == SHEDDING and shed.shed_orderbook_deltas  # hysteresis
    later_end = await scalar_of(
        engine, "select ended_at from ingest_gaps where reason = 'delta_shed'"
    )
    assert later_end > first_end  # an open shed period keeps reporting "up to at least now"
    d = await step(84)
    assert d.mode == TIGHTENED and not shed.shed_orderbook_deltas
    closed = await scalar_of(engine, "select ended_at > started_at from ingest_gaps")
    assert closed  # the shed period has a start and an end
    assert (await windows(engine))["tickers"] == "7 days"  # still tight until under 70%

    assert (await step(72)).mode == TIGHTENED
    d = await step(65)
    assert d.mode == NORMAL
    assert await windows(engine) == ORIGINAL  # restored exactly
    assert (
        await scalar_of(engine, "select count(*) from governor_state where key like 'retention:%'")
        == 0
    )
    assert await kinds(engine) == ["tighten", "shed_deltas", "resume_deltas", "restore"]


async def test_a_jump_straight_to_shedding_also_tightens_retention(engine: AsyncEngine) -> None:
    disk, shed = Disk(), Shed()
    disk.percent = 95
    governor, _ = make(engine, disk, shed)
    d = await governor.cycle()
    assert d.mode == SHEDDING and shed.shed_orderbook_deltas
    assert (await windows(engine))["tickers"] == "7 days"
    assert await kinds(engine) == ["tighten", "shed_deltas"]


async def test_windows_are_never_tightened_below_their_floor(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                'select alter_job(job_id, config => config || \'{"drop_after": "8 days"}\') '
                "from timescaledb_information.jobs where proc_name = 'policy_retention' "
                "and hypertable_name = 'trades'"
            )
        )
        await conn.execute(
            text(
                'select alter_job(job_id, config => config || \'{"drop_after": "3 days"}\') '
                "from timescaledb_information.jobs where proc_name = 'policy_retention' "
                "and hypertable_name = 'tickers'"
            )
        )
    disk = Disk()
    disk.percent = 85
    governor, clock = make(engine, disk)
    await governor.cycle()
    got = await windows(engine)
    assert got["trades"] == "7 days"  # 8 -> 4 would break the 7-day floor
    assert got["tickers"] == "3 days"  # already at its floor: left alone
    disk.percent = 10
    clock.step()
    await governor.cycle()
    after = await windows(engine)
    assert after["trades"] == "8 days" and after["tickers"] == "3 days"


async def test_a_restart_while_shedding_keeps_shedding(engine: AsyncEngine) -> None:
    disk = Disk()
    disk.percent = 92
    first, _ = make(engine, disk, Shed())
    await first.cycle()

    rebooted = Shed()  # a new process: flag starts off, state is in the database
    second, _ = make(engine, disk, rebooted)
    disk.percent = 88
    d = await second.cycle()
    assert d.mode == SHEDDING and rebooted.shed_orderbook_deltas
    assert (
        await scalar_of(engine, "select count(*) from ingest_gaps where reason = 'delta_shed'") == 1
    )


async def test_samples_are_stored_old_ones_pruned_and_growth_is_projected(
    engine: AsyncEngine,
) -> None:
    disk = Disk()
    governor, clock = make(engine, disk)
    async with engine.begin() as conn:  # a sample from 40 days ago must be pruned
        await conn.execute(
            text(
                "insert into storage_samples (ts, db_bytes, used_bytes, tables) "
                "values (:t, 1, 1, '{}')"
            ),
            {"t": T0 - timedelta(days=40)},
        )
    last = None
    for hour in range(6):  # +1% of budget (1 GB) per hour
        disk.percent = 10 + hour
        last = await governor.cycle()
        clock.step(60)
    assert last is not None and last.projection is not None
    assert last.projection.bytes_per_day == pytest.approx(24 * GB, rel=0.01)
    assert last.projection.days_to_full == pytest.approx((100 - 15) / 24, rel=0.02)
    assert await scalar_of(engine, "select count(*) from storage_samples") == 6


async def test_the_drive_is_watched_for_free_space_and_for_disappearing(
    engine: AsyncEngine,
) -> None:
    disk = Disk()
    governor, clock = make(engine, disk)

    async def step() -> list[str]:
        clock.step()
        return (await governor.cycle()).actions

    assert await step() == []
    disk.free_fraction = 0.10
    assert await step() == ["drive low"]
    assert await step() == []  # reported once, not every cycle
    disk.error = "OSError: [Errno 5] Input/output error"
    assert await step() == ["drive error"]
    disk.error, disk.free_fraction = None, 0.5
    assert await step() == ["drive ok"]
    assert await kinds(engine) == ["drive_low", "drive_error", "drive_ok"]


async def test_the_ingestor_drops_deltas_while_shedding_and_keeps_snapshots(
    migrated_db_url: str,
) -> None:
    tracker = BookTracker()
    tracker.begin_subscription(1, ["KXA-E1-X"])
    batch = events_for(
        tracker,
        [
            snap_msg(1, 1, "KXA-E1-X", [("0.40", "10.00")], []),
            delta_msg(1, 2, "KXA-E1-X", "yes", "0.40", "1.00", offset_ms=100),
            delta_msg(1, 3, "KXA-E1-X", "yes", "0.41", "2.00", offset_ms=200),
        ],
    )

    async def run(shedding: bool) -> Any:
        engine = db.make_engine(migrated_db_url)
        from kalshiterm_server.ingest.stream import StreamIngestor

        async def source() -> AsyncIterator[Any]:
            for event in batch:
                yield event

        ingestor = StreamIngestor(source(), engine)
        ingestor.shed_orderbook_deltas = shedding
        async with asyncio.timeout(30):
            await ingestor.run()
        await engine.dispose()
        return ingestor

    shed = await run(True)
    assert shed.shed["ob_delta"] == 2 and shed.written["orderbook_deltas"] == 0
    assert shed.written["orderbook_snapshots"] == 1  # snapshots are never shed
    assert shed.stats()["shed"] == {"ob_delta": 2}

    await run(False)
    assert await scalar(migrated_db_url, "select count(*) from orderbook_deltas") == 2
