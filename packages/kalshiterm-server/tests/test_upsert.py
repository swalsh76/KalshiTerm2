from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from kalshi_core.models import Market
from kalshiterm_server import db
from kalshiterm_server.fixedpoint import PrecisionError
from kalshiterm_server.storage import reference, tables
from sqlalchemy import text

pytestmark = pytest.mark.db


def market(ticker: str = "M1", **overrides: object) -> dict[str, Any]:
    base: dict[str, Any] = {
        "ticker": ticker,
        "event_ticker": "E1",
        "market_type": "binary",
        "status": "open",
        "yes_sub_title": "Yes",
        "no_sub_title": "No",
    }
    return {**base, **overrides}


async def fetch(url: str, sql: str) -> list[tuple[Any, ...]]:
    engine = db.make_engine(url)
    try:
        async with engine.connect() as conn:
            return [tuple(r) for r in await conn.execute(text(sql))]
    finally:
        await engine.dispose()


async def fetch_map(url: str, sql: str) -> dict[Any, Any]:
    """Two-column query as ``{first: second}``."""
    return {row[0]: row[1] for row in await fetch(url, sql)}


async def test_insert_then_identical_rerun_rewrites_nothing(migrated_db_url: str) -> None:
    engine = db.make_engine(migrated_db_url)
    rows = [market(f"M{i}") for i in range(50)]
    sql = "select ticker, xmin::text from markets"
    try:
        first = await reference.upsert(engine, tables.markets, rows, "ticker")
        before = await fetch_map(migrated_db_url, sql)
        second = await reference.upsert(engine, tables.markets, rows, "ticker")
        after = await fetch_map(migrated_db_url, sql)
    finally:
        await engine.dispose()
    assert (first.inserted, first.updated, first.unchanged) == (50, 0, 0)
    assert (second.inserted, second.updated, second.unchanged) == (0, 0, 50)
    assert before == after  # same tuple versions: no dead rows were created


async def test_only_changed_rows_are_updated(migrated_db_url: str) -> None:
    engine = db.make_engine(migrated_db_url)
    sql = "select ticker, updated_at from markets"
    try:
        await reference.upsert(engine, tables.markets, [market("A"), market("B")], "ticker")
        before = await fetch_map(migrated_db_url, sql)
        result = await reference.upsert(
            engine, tables.markets, [market("A"), market("B", status="closed")], "ticker"
        )
        after = await fetch_map(migrated_db_url, sql)
        status = await fetch_map(migrated_db_url, "select ticker, status from markets")
    finally:
        await engine.dispose()
    assert (result.inserted, result.updated, result.unchanged) == (0, 1, 1)
    assert after["A"] == before["A"] and after["B"] > before["B"]
    assert status["B"] == "closed"


async def test_market_ids_are_stable_and_never_reused(migrated_db_url: str) -> None:
    engine = db.make_engine(migrated_db_url)
    sql = "select ticker, id from markets"
    try:
        await reference.upsert(engine, tables.markets, [market("A"), market("B")], "ticker")
        ids = await fetch_map(migrated_db_url, sql)
        await reference.upsert(
            engine, tables.markets, [market("A", status="closed"), market("C")], "ticker"
        )
        ids2 = await fetch_map(migrated_db_url, sql)
    finally:
        await engine.dispose()
    assert ids2["A"] == ids["A"] and ids2["B"] == ids["B"]  # updates keep the id
    assert ids2["C"] not in ids.values()


async def test_duplicate_keys_in_one_call_collapse_to_the_last(migrated_db_url: str) -> None:
    engine = db.make_engine(migrated_db_url)
    rows = [market("A", status="open"), market("A", status="closed")]
    try:
        result = await reference.upsert(engine, tables.markets, rows, "ticker")
        stored = await fetch(migrated_db_url, "select status from markets")
    finally:
        await engine.dispose()
    assert result.inserted == 1 and stored == [("closed",)]


async def test_large_batches_are_split_to_respect_the_parameter_limit(
    migrated_db_url: str,
) -> None:
    engine = db.make_engine(migrated_db_url)
    # Wide rows: 5,000 rows x many columns would exceed 32,767 bind parameters in one statement.
    wide = [
        market(
            f"BIG{i}",
            rules_primary="p",
            rules_secondary="s",
            strike_type="t",
            result="",
            exchange_index=1,
            floor_strike_e6=1,
            cap_strike_e6=2,
            settlement_value_e6=3,
        )
        for i in range(5000)
    ]
    try:
        result = await reference.upsert(engine, tables.markets, wide, "ticker")
        count = await fetch(migrated_db_url, "select count(*) from markets")
    finally:
        await engine.dispose()
    assert result.inserted == 5000 and count == [(5000,)]


def test_market_row_converts_to_fixed_point() -> None:
    m = Market.model_validate(
        {
            "ticker": "T",
            "event_ticker": "E",
            "market_type": "binary",
            "status": "finalized",
            "settlement_value_dollars": "1.0000",
            "settlement_ts": "2026-10-06T21:00:00Z",
            "floor_strike": 118.7499,
            "cap_strike": 120,
        }
    )
    row = reference.market_row(m)
    assert row["settlement_value_e6"] == 1_000_000
    assert row["floor_strike_e6"] == 118_749_900 and row["cap_strike_e6"] == 120_000_000
    assert row["settlement_ts"] == datetime(2026, 10, 6, 21, 0, tzinfo=UTC)


def test_market_row_refuses_values_it_cannot_store_exactly() -> None:
    m = Market.model_validate(
        {
            "ticker": "T",
            "event_ticker": "E",
            "market_type": "binary",
            "status": "x",
            "settlement_value_dollars": Decimal("0.1234567"),
        }
    )
    with pytest.raises(PrecisionError):
        reference.market_row(m)
