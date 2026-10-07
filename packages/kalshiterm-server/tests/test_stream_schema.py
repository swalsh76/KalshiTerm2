import pytest
from kalshiterm_server import db
from kalshiterm_server.ingest.combos import LARGE_TRADE_COLUMNS
from kalshiterm_server.ingest.stream import (
    LIFECYCLE_COLUMNS,
    TICKER_COLUMNS,
    TRADE_COLUMNS,
)
from sqlalchemy import text

pytestmark = pytest.mark.db


async def query(url: str, sql: str, **params: object) -> list[tuple[object, ...]]:
    engine = db.make_engine(url)
    try:
        async with engine.connect() as conn:
            return [tuple(r) for r in await conn.execute(text(sql), params)]
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    ("table", "columns"),
    [
        ("tickers", TICKER_COLUMNS),
        ("trades", TRADE_COLUMNS),
        ("market_lifecycle", LIFECYCLE_COLUMNS),
        ("combo_large_trades", LARGE_TRADE_COLUMNS),
    ],
)
async def test_copy_column_lists_match_the_migrated_tables(
    migrated_db_url: str, table: str, columns: list[str]
) -> None:
    """The ingestor's column lists are hand-written; this stops them drifting from the DDL."""
    rows = await query(
        migrated_db_url,
        "select column_name from information_schema.columns where table_name = :t "
        "order by ordinal_position",
        t=table,
    )
    assert [name for (name,) in rows] == columns


async def test_streaming_tables_are_hypertables_with_the_planned_chunk_sizes(
    migrated_db_url: str,
) -> None:
    rows = await query(
        migrated_db_url,
        "select hypertable_name, d.time_interval::text from timescaledb_information.hypertables h "
        "join timescaledb_information.dimensions d using (hypertable_name) order by 1",
    )
    assert dict(rows) == {  # type: ignore[arg-type]
        "combo_large_trades": "7 days",
        "market_lifecycle": "7 days",
        "tickers": "1 day",
        "trades": "1 day",
    }


async def test_views_present_readable_values_and_the_ticker(migrated_db_url: str) -> None:
    engine = db.make_engine(migrated_db_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "insert into markets (ticker, event_ticker, market_type, status) "
                    "values ('KXA-E1-X', 'KXA-E1', 'binary', 'active')"
                )
            )
            await conn.execute(
                text(
                    "insert into tickers (ts, received_at, market_id, price_e6, yes_bid_size_e2) "
                    "select now(), now(), id, 560000, 12000 from markets"
                )
            )
            await conn.execute(
                text(
                    "insert into trades (ts, received_at, market_id, trade_id, yes_price_e6, "
                    "count_e2) select now(), now(), id, gen_random_uuid(), 410000, 304 "
                    "from markets"
                )
            )
            ticker = (
                await conn.execute(text("select ticker, price, yes_bid_size from tickers_v"))
            ).one()
            trade = (
                await conn.execute(text("select yes_price, no_price, count from trades_v"))
            ).one()
    finally:
        await engine.dispose()
    assert (ticker[0], str(ticker[1]), str(ticker[2])) == ("KXA-E1-X", "0.560000", "120.00")
    assert tuple(str(v) for v in trade) == ("0.410000", "0.590000", "3.04")
