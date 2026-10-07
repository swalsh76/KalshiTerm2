import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal as D
from typing import Any

import pytest
from kalshiterm_server import db
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from test_stream import rows

pytestmark = pytest.mark.db

DAY0 = (datetime.now(UTC) - timedelta(days=40)).replace(hour=10, minute=0, second=0, microsecond=0)


@pytest.fixture
async def engine(migrated_db_url: str) -> Any:
    engine = db.make_engine(migrated_db_url)
    async with engine.begin() as conn:
        for ticker in ("KXA-E1-X", "KXB-E1-Y"):
            await conn.execute(
                text(
                    "insert into markets (ticker, event_ticker, market_type, status) "
                    "values (:t, 'KXA-E1', 'binary', 'active')"
                ),
                {"t": ticker},
            )
    yield engine
    await engine.dispose()


async def add_trades(
    engine: AsyncEngine, ticker: str, at: datetime, items: list[tuple[int, int, int]]
) -> None:
    """items: (seconds after ``at``, yes price in millionths, contracts in hundredths)."""
    async with engine.begin() as conn:
        for seconds, price, count in items:
            await conn.execute(
                text(
                    "insert into trades (ts, received_at, market_id, trade_id, yes_price_e6, "
                    "count_e2) select :ts, :ts, id, :t, :p, :c from markets where ticker = :k"
                ),
                {
                    "ts": at + timedelta(seconds=seconds),
                    "t": uuid.uuid4(),
                    "p": price,
                    "c": count,
                    "k": ticker,
                },
            )


async def add_ticks(
    engine: AsyncEngine, ticker: str, at: datetime, items: list[tuple[Any, ...]]
) -> None:
    """items: (seconds, price, yes_bid, yes_ask, volume_e2, open_interest_e2)."""
    async with engine.begin() as conn:
        for seconds, price, bid, ask, volume, oi in items:
            await conn.execute(
                text(
                    "insert into tickers (ts, received_at, market_id, price_e6, yes_bid_e6, "
                    "yes_ask_e6, volume_e2, open_interest_e2) select :ts, :ts, id, :p, :b, :a, "
                    ":v, :o from markets where ticker = :k"
                ),
                {
                    "ts": at + timedelta(seconds=seconds),
                    "p": price,
                    "b": bid,
                    "a": ask,
                    "v": volume,
                    "o": oi,
                    "k": ticker,
                },
            )


async def refresh(engine: AsyncEngine, view: str, start: datetime, end: datetime) -> None:
    async with engine.connect() as conn:
        auto = await conn.execution_options(isolation_level="AUTOCOMMIT")
        await auto.execute(
            text(
                f"CALL refresh_continuous_aggregate('{view}', "
                "CAST(:a AS timestamptz), CAST(:b AS timestamptz))"
            ),
            {"a": start, "b": end},
        )


async def run_policy(engine: AsyncEngine, proc: str, table: str) -> None:
    """Run a policy job now, as the background worker would."""
    async with engine.connect() as conn:
        job = (
            await conn.execute(
                text(
                    "select job_id from timescaledb_information.jobs "
                    "where proc_name = :p and hypertable_name = :t"
                ),
                {"p": proc, "t": table},
            )
        ).scalar_one()
    async with engine.connect() as conn:
        auto = await conn.execution_options(isolation_level="AUTOCOMMIT")
        await auto.execute(text(f"CALL run_job({job})"))


async def scalar(engine: AsyncEngine, sql: str) -> Any:
    async with engine.connect() as conn:
        return (await conn.execute(text(sql))).scalar_one()


async def test_every_aggregate_has_a_refresh_policy_and_the_raw_tables_have_retention(
    migrated_db_url: str,
) -> None:
    refresh_jobs = await rows(
        migrated_db_url,
        "select hypertable_name, config->>'start_offset' from timescaledb_information.jobs "
        "where proc_name = 'policy_refresh_continuous_aggregate' order by 1",
    )
    assert refresh_jobs == [
        ("candles_1h", "2 days"),
        ("candles_1m", "1 day"),
        ("ticker_1h", "2 days"),
        ("ticker_1m", "1 day"),
    ]
    retention = await rows(
        migrated_db_url,
        "select hypertable_name, config->>'drop_after' from timescaledb_information.jobs "
        "where proc_name = 'policy_retention' order by 1",
    )
    assert retention == [
        ("combo_large_trades", "365 days"),
        ("orderbook_deltas", "14 days"),
        ("orderbook_snapshots", "365 days"),
        ("ticker_1m", "30 days"),
        ("tickers", "14 days"),
        ("trades", "30 days"),
    ]


async def test_trade_candles_have_exact_open_high_low_close_volume_and_count(
    engine: AsyncEngine,
) -> None:
    await add_trades(
        engine,
        "KXA-E1-X",
        DAY0,
        [(5, 400000, 1000), (20, 550000, 500), (40, 300000, 250), (50, 450000, 100)],
    )
    await add_trades(engine, "KXA-E1-X", DAY0 + timedelta(minutes=1), [(10, 480000, 700)])
    await add_trades(engine, "KXB-E1-Y", DAY0, [(30, 990000, 5)])
    for view in ("candles_1m", "candles_1h"):
        await refresh(engine, view, DAY0 - timedelta(hours=2), DAY0 + timedelta(hours=4))

    minute = await rows_of(
        engine,
        "select ticker, bucket, open, high, low, close, volume, trades from candles_1m_v "
        "order by ticker, bucket",
    )
    assert minute == [
        ("KXA-E1-X", DAY0, D("0.4"), D("0.55"), D("0.3"), D("0.45"), D("18.5"), 4),
        ("KXA-E1-X", DAY0 + timedelta(minutes=1), D("0.48"), D("0.48"), D("0.48"), D("0.48"),
         D("7"), 1),
        ("KXB-E1-Y", DAY0, D("0.99"), D("0.99"), D("0.99"), D("0.99"), D("0.05"), 1),
    ]  # fmt: skip
    hour = await rows_of(
        engine,
        "select ticker, open, high, low, close, volume, trades from candles_1h_v order by ticker",
    )
    assert hour[0] == ("KXA-E1-X", D("0.4"), D("0.55"), D("0.3"), D("0.48"), D("25.5"), 5)
    assert hour[1][0] == "KXB-E1-Y"


async def rows_of(engine: AsyncEngine, sql: str) -> list[tuple[Any, ...]]:
    async with engine.connect() as conn:
        return [tuple(r) for r in await conn.execute(text(sql))]


async def test_ticker_aggregates_take_the_last_quote_and_ignore_a_missing_price(
    engine: AsyncEngine,
) -> None:
    await add_ticks(
        engine,
        "KXA-E1-X",
        DAY0,
        [
            (1, 500000, 490000, 510000, 10000, 5000),
            (30, 520000, 500000, 530000, 12000, 6000),
            (50, None, 510000, 540000, 12500, 6100),  # a quote update with no trade price
        ],
    )
    await refresh(engine, "ticker_1m", DAY0 - timedelta(hours=2), DAY0 + timedelta(hours=4))
    got = await rows_of(
        engine,
        "select open, high, low, close, yes_bid, yes_ask, volume, open_interest, ticks "
        "from ticker_1m_v",
    )
    assert got == [
        (D("0.5"), D("0.52"), D("0.5"), D("0.52"), D("0.51"), D("0.54"), D("125"), D("61"), 3)
    ]


async def test_candles_survive_the_deletion_of_their_raw_rows(engine: AsyncEngine) -> None:
    recent = datetime.now(UTC) - timedelta(hours=3)
    await add_trades(engine, "KXA-E1-X", DAY0, [(5, 400000, 1000), (20, 550000, 500)])
    await add_trades(engine, "KXA-E1-X", recent, [(0, 600000, 100)])
    await refresh(engine, "candles_1m", DAY0 - timedelta(hours=2), DAY0 + timedelta(hours=4))
    await refresh(engine, "candles_1h", DAY0 - timedelta(hours=2), DAY0 + timedelta(hours=4))
    before = await rows_of(
        engine, "select * from candles_1m_v where bucket < now() - interval '1 day'"
    )
    assert len(before) == 1

    await run_policy(engine, "policy_retention", "trades")

    raw_old = await scalar(
        engine, "select count(*) from trades where ts < now() - interval '30 days'"
    )
    assert raw_old == 0  # the raw rows are gone
    assert await scalar(engine, "select count(*) from trades") == 1  # the recent one stays
    after = await rows_of(
        engine, "select * from candles_1m_v where bucket < now() - interval '1 day'"
    )
    assert after == before  # ...and the candle is untouched
    hour = await rows_of(
        engine,
        "select open, high, low, close, volume, trades from candles_1h_v "
        "where bucket < now() - interval '1 day'",
    )
    assert hour == [(D("0.4"), D("0.55"), D("0.4"), D("0.55"), D("15"), 2)]


async def test_ticker_minutes_expire_after_30_days_but_the_hourly_history_stays(
    engine: AsyncEngine,
) -> None:
    await add_ticks(engine, "KXA-E1-X", DAY0, [(1, 500000, 490000, 510000, 10000, 5000)])
    for view in ("ticker_1m", "ticker_1h"):
        await refresh(engine, view, DAY0 - timedelta(hours=2), DAY0 + timedelta(hours=4))
    assert await scalar(engine, "select count(*) from ticker_1m") == 1

    await run_policy(engine, "policy_retention", "tickers")
    await run_policy(engine, "policy_retention", "ticker_1m")

    assert await scalar(engine, "select count(*) from tickers") == 0
    assert await scalar(engine, "select count(*) from ticker_1m") == 0
    assert await scalar(engine, "select count(*) from ticker_1h") == 1


async def test_old_aggregates_are_compressed_without_changing_a_value(
    engine: AsyncEngine,
) -> None:
    await add_trades(engine, "KXA-E1-X", DAY0, [(5, 400000, 1000), (20, 550000, 500)])
    await refresh(engine, "candles_1m", DAY0 - timedelta(hours=2), DAY0 + timedelta(hours=4))
    before = await rows_of(engine, "select * from candles_1m order by bucket")

    await run_policy(engine, "policy_compression", "candles_1m")

    compressed = await scalar(
        engine,
        "select count(*) from timescaledb_information.chunks c join "
        "timescaledb_information.continuous_aggregates a on a.materialization_hypertable_name "
        "= c.hypertable_name where a.view_name = 'candles_1m' and c.is_compressed",
    )
    assert compressed >= 1
    assert await rows_of(engine, "select * from candles_1m order by bucket") == before
