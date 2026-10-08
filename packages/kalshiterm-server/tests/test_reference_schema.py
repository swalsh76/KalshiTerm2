import pytest
from kalshiterm_server import db
from kalshiterm_server.storage import tables
from sqlalchemy import text
from timescale_jobs import quiet_database

pytestmark = pytest.mark.db


async def column_names(url: str, table: str) -> set[str]:
    engine = db.make_engine(url)
    try:
        async with engine.connect() as conn:
            rows = await conn.execute(
                text("select column_name from information_schema.columns where table_name = :t"),
                {"t": table},
            )
            return {name for (name,) in rows}
    finally:
        await engine.dispose()


async def test_core_definitions_match_the_migrated_database(migrated_db_url: str) -> None:
    """The SQLAlchemy Core tables are written by hand; this stops them drifting from the DDL."""
    for table in tables.metadata.sorted_tables:
        assert {c.name for c in table.columns} == await column_names(migrated_db_url, table.name)


async def test_views_present_readable_decimals(migrated_db_url: str) -> None:
    engine = db.make_engine(migrated_db_url)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "insert into series (ticker, title, frequency, category, fee_multiplier_e6) "
                    "values ('S', 't', 'daily', 'c', 70000)"
                )
            )
            await conn.execute(
                text(
                    "insert into markets (ticker, event_ticker, market_type, status, "
                    "settlement_value_e6, floor_strike_e6) "
                    "values ('M', 'E', 'binary', 'finalized', 1000000, 118749900)"
                )
            )
            fee = (await conn.execute(text("select fee_multiplier from series_v"))).scalar_one()
            row = (
                await conn.execute(text("select settlement_value, floor_strike from markets_v"))
            ).one()
    finally:
        await engine.dispose()
    assert str(fee) == "0.070000"
    assert (str(row[0]), str(row[1])) == ("1.000000", "118.749900")


async def test_downgrade_removes_the_reference_tables(migrated_db_url: str) -> None:
    await quiet_database(migrated_db_url)
    await db.downgrade_async(migrated_db_url, "0001")
    for name in ("series", "events", "markets", "discovery_state"):
        assert await column_names(migrated_db_url, name) == set()
