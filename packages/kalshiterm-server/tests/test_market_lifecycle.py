import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from kalshiterm_server import db
from kalshiterm_server.storage import reference, tables
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine
from timescale_jobs import quiet_background_jobs, run_job_named

pytestmark = pytest.mark.db

NOW = datetime.now(UTC)


@pytest.fixture
async def engine(migrated_db_url: str) -> AsyncIterator[AsyncEngine]:
    engine = db.make_engine(migrated_db_url)
    await quiet_background_jobs(engine)
    yield engine
    await engine.dispose()


async def add_market(
    engine: AsyncEngine, ticker: str, event: str, settled_days_ago: int | None, **extra: Any
) -> int:
    """A fully described market, settled ``settled_days_ago`` days ago (None = unsettled)."""
    values = {
        "ticker": ticker,
        "event": event,
        "status": "finalized" if settled_days_ago is not None else "active",
        "settled": None if settled_days_ago is None else NOW - timedelta(days=settled_days_ago),
        "created": NOW - timedelta(days=200),
        **extra,
    }
    async with engine.begin() as conn:
        market_id: int = (
            await conn.execute(
                text(
                    "insert into markets (ticker, event_ticker, market_type, status, "
                    "yes_sub_title, no_sub_title, created_time, updated_time, open_time, "
                    "close_time, latest_expiration_time, result, settlement_value_e6, "
                    "settlement_ts, rules_primary, rules_secondary, strike_type, "
                    "floor_strike_e6, cap_strike_e6, exchange_index) values (:ticker, :event, "
                    "'binary', :status, 'Yes sub', 'No sub', :created, :created, :created, "
                    ":created, :created, 'yes', 1000000, :settled, 'Rules one', 'Rules two', "
                    "'between', 10000000, 20000000, 3) returning id"
                ),
                values,
            )
        ).scalar_one()
        await conn.execute(
            text(
                "insert into events (event_ticker, series_ticker, title, mutually_exclusive) "
                "values (:e, 'KXS', 'Event title', false) on conflict do nothing"
            ),
            {"e": event},
        )
    return market_id


async def market(engine: AsyncEngine, ticker: str) -> dict[str, Any]:
    async with engine.connect() as conn:
        row = (
            await conn.execute(text("select * from markets where ticker = :t"), {"t": ticker})
        ).one()
        return dict(row._mapping)


async def events(engine: AsyncEngine) -> list[str]:
    async with engine.connect() as conn:
        found = await conn.execute(text("select event_ticker from events order by 1"))
        return [r[0] for r in found]


def is_full(row: dict[str, Any]) -> bool:
    return bool(row["rules_primary"] == "Rules one" and row["strike_type"] == "between")


def is_slim(row: dict[str, Any]) -> bool:
    blank = ("rules_primary", "rules_secondary", "yes_sub_title", "no_sub_title")
    return bool(
        all(row[c] == "" for c in blank)
        and row["strike_type"] == "between"  # strikes, times kept in the 30-90 day stage
        and row["floor_strike_e6"] == 10_000_000
        and row["created_time"] is not None
    )


def is_tombstone(row: dict[str, Any]) -> bool:
    gone = (
        "created_time", "updated_time", "open_time", "close_time", "latest_expiration_time",
        "strike_type", "floor_strike_e6", "cap_strike_e6", "exchange_index",
    )  # fmt: skip
    return (
        all(row[c] is None for c in gone)
        and all(row[c] == "" for c in ("rules_primary", "yes_sub_title"))
        and row["result"] == "yes"  # the outcome survives
        and row["settlement_value_e6"] == 1_000_000
        and row["settlement_ts"] is not None
        and row["event_ticker"] != ""
    )


async def test_the_job_is_registered_daily_with_its_windows_in_the_config(
    engine: AsyncEngine,
) -> None:
    async with engine.connect() as conn:
        job = (
            await conn.execute(
                text(
                    "select schedule_interval::text, config from timescaledb_information.jobs "
                    "where proc_name = 'expire_markets'"
                )
            )
        ).one()
    assert job[0] == "1 day"
    assert job[1] == {"slim_after": "30 days", "delete_after": "90 days"}


async def test_markets_move_through_full_then_slim_then_tombstone_by_age(
    engine: AsyncEngine,
) -> None:
    await add_market(engine, "KXF-E1-FRESH", "KXF-E1", 10)
    await add_market(engine, "KXF-E2-MID", "KXF-E2", 40)
    await add_market(engine, "KXF-E3-OLD", "KXF-E3", 100)
    await add_market(engine, "KXF-E4-OPEN", "KXF-E4", None)  # created 200 days ago, unsettled

    state = await run_job_named(engine, "expire_markets")

    assert is_full(await market(engine, "KXF-E1-FRESH")), state
    assert is_slim(await market(engine, "KXF-E2-MID")), state
    old = await market(engine, "KXF-E3-OLD")
    assert is_tombstone(old) and old["ticker"] == "KXF-E3-OLD"
    assert is_full(await market(engine, "KXF-E4-OPEN")), state  # unsettled markets never expire


async def test_watched_and_pinned_markets_are_slimmed_but_never_tombstoned(
    engine: AsyncEngine,
) -> None:
    watched = await add_market(engine, "KXW-E1-A", "KXW-E1", 100)
    pinned = await add_market(engine, "KXW-E2-B", "KXW-E2", 100)
    async with engine.begin() as conn:
        await conn.execute(
            text("insert into watchlist_periods (market_id, source) values (:m, 'auto')"),
            {"m": watched},
        )
        await conn.execute(
            text("insert into pins (market_id, note) values (:m, 'keep')"), {"m": pinned}
        )

    state = await run_job_named(engine, "expire_markets")

    assert is_slim(await market(engine, "KXW-E1-A")), state  # a closed watch period still counts
    assert is_slim(await market(engine, "KXW-E2-B")), state
    assert await events(engine) == ["KXW-E1", "KXW-E2"]


async def test_events_are_deleted_only_when_all_their_markets_have_expired(
    engine: AsyncEngine,
) -> None:
    await add_market(engine, "KXE-1-A", "KXE-1", 100)  # event 1: every market expired
    await add_market(engine, "KXE-1-B", "KXE-1", 120)
    await add_market(engine, "KXE-2-A", "KXE-2", 100)  # event 2: one still recent
    await add_market(engine, "KXE-2-B", "KXE-2", 5)
    pinned = await add_market(engine, "KXE-3-A", "KXE-3", 100)  # event 3: one pinned
    await add_market(engine, "KXE-3-B", "KXE-3", 100)
    async with engine.begin() as conn:
        await conn.execute(text("insert into pins (market_id) values (:m)"), {"m": pinned})
        await conn.execute(  # event 4 has no markets at all (e.g. not yet discovered)
            text(
                "insert into events (event_ticker, series_ticker, title, mutually_exclusive) "
                "values ('KXE-4', 'KXS', 'Empty', false)"
            )
        )

    state = await run_job_named(engine, "expire_markets")

    assert await events(engine) == ["KXE-2", "KXE-3", "KXE-4"]
    assert is_tombstone(await market(engine, "KXE-1-A")), state  # the tombstones remain


async def test_running_twice_changes_nothing_the_second_time(engine: AsyncEngine) -> None:
    await add_market(engine, "KXI-E1-A", "KXI-E1", 40)
    await add_market(engine, "KXI-E2-B", "KXI-E2", 100)
    await run_job_named(engine, "expire_markets")
    first = [await market(engine, t) for t in ("KXI-E1-A", "KXI-E2-B")]
    state = await run_job_named(engine, "expire_markets")
    assert [await market(engine, t) for t in ("KXI-E1-A", "KXI-E2-B")] == first, state


async def test_a_tombstone_keeps_its_id_so_old_data_still_joins_to_its_ticker(
    engine: AsyncEngine,
) -> None:
    market_id = await add_market(engine, "KXT-E1-A", "KXT-E1", 100)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "insert into trades (ts, received_at, market_id, trade_id, yes_price_e6, "
                "count_e2) values (now(), now(), :m, :t, 500000, 100)"
            ),
            {"m": market_id, "t": uuid.uuid4()},
        )
    await run_job_named(engine, "expire_markets")
    async with engine.connect() as conn:
        seen = (await conn.execute(text("select ticker, market_id from trades_v"))).all()
        again = (
            await conn.execute(text("select id from markets where ticker = 'KXT-E1-A'"))
        ).scalar_one()
    assert [tuple(r) for r in seen] == [("KXT-E1-A", market_id)]
    assert again == market_id


async def test_a_tombstone_that_discovery_refills_is_reduced_again_by_the_next_run(
    engine: AsyncEngine,
) -> None:
    await add_market(engine, "KXR-E1-A", "KXR-E1", 100)
    state = await run_job_named(engine, "expire_markets")
    assert is_tombstone(await market(engine, "KXR-E1-A")), state

    await reference.upsert(  # Kalshi touched the old market, so discovery rewrote it in full
        engine,
        tables.markets,
        [
            {
                "ticker": "KXR-E1-A",
                "event_ticker": "KXR-E1",
                "market_type": "binary",
                "status": "finalized",
                "yes_sub_title": "Yes sub",
                "rules_primary": "Rules one",
                "result": "yes",
                "settlement_value_e6": 1_000_000,
                "settlement_ts": NOW - timedelta(days=100),
                "strike_type": "between",
                "created_time": NOW - timedelta(days=200),
            }
        ],
        "ticker",
    )
    assert (await market(engine, "KXR-E1-A"))["rules_primary"] == "Rules one"

    state = await run_job_named(engine, "expire_markets")
    assert is_tombstone(await market(engine, "KXR-E1-A")), state


async def test_the_windows_come_from_the_job_config(engine: AsyncEngine) -> None:
    await add_market(engine, "KXC-E1-A", "KXC-E1", 20)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "select alter_job(job_id, config => "
                """'{"slim_after": "7 days", "delete_after": "15 days"}'::jsonb) """
                "from timescaledb_information.jobs where proc_name = 'expire_markets'"
            )
        )
    state = await run_job_named(engine, "expire_markets")
    assert is_tombstone(await market(engine, "KXC-E1-A")), state


async def test_a_pin_must_point_at_a_real_market(engine: AsyncEngine) -> None:
    with pytest.raises(DBAPIError, match="foreign key"):
        async with engine.begin() as conn:
            await conn.execute(text("insert into pins (market_id) values (999999)"))
