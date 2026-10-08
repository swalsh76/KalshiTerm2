import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from kalshiterm_server import db
from kalshiterm_server.ingest.stream import StreamIngestor
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from test_stream import Gate, rows, scalar, trade_msg, until
from test_stream_schema import query
from timescale_jobs import quiet_background_jobs

pytestmark = pytest.mark.db

TABLES = ["orderbook_deltas", "orderbook_snapshots", "tickers", "trades", "trades_watchlist"]


async def test_the_big_tables_are_compressed_after_one_day_ordered_by_market_then_time(
    migrated_db_url: str,
) -> None:
    settings = await query(
        migrated_db_url,
        "select hypertable::text, coalesce(segmentby, ''), orderby from "
        "timescaledb_information.hypertable_compression_settings "
        "where orderby is not null and hypertable::text = any(:tables) order by 1",
        tables=TABLES,
    )
    assert settings == [(t, "", "market_id,ts DESC") for t in TABLES]
    jobs = await query(
        migrated_db_url,
        "select hypertable_name, config->>'compress_after' from timescaledb_information.jobs "
        "where proc_name = 'policy_compression' and hypertable_name = any(:tables) order by 1",
        tables=TABLES,
    )
    assert jobs == [(t, "1 day") for t in TABLES]


@pytest.fixture
async def engine(migrated_db_url: str) -> Any:
    engine = db.make_engine(migrated_db_url)
    await quiet_background_jobs(engine)
    yield engine
    await engine.dispose()


async def compress_policy(engine: AsyncEngine, table: str) -> None:
    """Run the table's compression policy now (as the background worker would)."""
    async with engine.connect() as conn:
        job = (
            await conn.execute(
                text(
                    "select job_id from timescaledb_information.jobs "
                    "where proc_name = 'policy_compression' and hypertable_name = :t"
                ),
                {"t": table},
            )
        ).scalar_one()
    async with engine.connect() as conn:
        auto = await conn.execution_options(isolation_level="AUTOCOMMIT")
        await auto.execute(text(f"CALL run_job({job})"))  # a procedure that commits itself


async def test_the_policy_compresses_old_chunks_keeps_the_new_one_and_loses_nothing(
    engine: AsyncEngine,
) -> None:
    now = datetime.now(UTC)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "insert into markets (ticker, event_ticker, market_type, status) "
                "values ('KXA-E1-X', 'KXA-E1', 'binary', 'active')"
            )
        )
        for age in (timedelta(days=4), timedelta(days=4, minutes=5), timedelta(minutes=5)):
            await conn.execute(
                text(
                    "insert into trades (ts, received_at, market_id, trade_id, yes_price_e6, "
                    "count_e2, taker_side) select :ts, :ts, id, :t, 123456, 789, 'no' "
                    "from markets"
                ),
                {"ts": now - age, "t": uuid.uuid4()},
            )
    before = await _all_trades(engine)

    await compress_policy(engine, "trades")

    async with engine.connect() as conn:
        state = (
            await conn.execute(
                text(
                    "select is_compressed, range_start < now() - interval '3 days' as old "
                    "from timescaledb_information.chunks where hypertable_name = 'trades' "
                    "order by range_start"
                )
            )
        ).all()
    assert [tuple(r) for r in state] == [(True, True), (False, False)]
    assert await _all_trades(engine) == before  # identical values, nothing lost


async def _all_trades(engine: AsyncEngine) -> list[tuple[Any, ...]]:
    async with engine.connect() as conn:
        found = await conn.execute(
            text("select ts, trade_id, yes_price_e6, count_e2, taker_side from trades order by ts")
        )
        return [tuple(r) for r in found]


async def test_ingest_and_backfill_still_write_into_a_chunk_that_is_already_compressed(
    migrated_db_url: str, engine: AsyncEngine
) -> None:
    gate = Gate()
    ingestor = StreamIngestor(gate.__aiter__(), engine, flush_interval=0.05)
    task = asyncio.create_task(ingestor.run())
    async with asyncio.timeout(30):
        first = trade_msg(offset_ms=0)  # the fixture time is days in the past
        gate.put(first)
        await until(lambda: ingestor.written["trades"] == 1)
        await compress_policy(engine, "trades")
        compressed = await scalar(
            migrated_db_url,
            "select count(*) from timescaledb_information.chunks "
            "where hypertable_name = 'trades' and is_compressed",
        )
        assert compressed == 1

        second = trade_msg(offset_ms=500)
        gate.put(second)
        await until(lambda: ingestor.written["trades"] == 2)
        ingestor.watch(["KXA-E1-X"])  # the history copy also reads the compressed chunk
        await until(lambda: "KXA-E1-X" in ingestor.watched)
        gate.close()
        await task

    assert await scalar(migrated_db_url, "select count(*) from trades") == 2
    ids = await rows(migrated_db_url, "select trade_id::text from trades_watchlist order by ts")
    assert [r[0] for r in ids] == [first.msg["trade_id"], second.msg["trade_id"]]
