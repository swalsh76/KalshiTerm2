"""Row mapping and change-aware upserts for the reference tables."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from kalshi_core.models import Event, Market, Series
from sqlalchemy import Table, func, literal_column, select, tuple_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine

from kalshiterm_server.fixedpoint import to_e6
from kalshiterm_server.storage import tables

MAX_BIND_PARAMS = 30_000  # PostgreSQL/asyncpg allow 32,767 per statement


@dataclass
class UpsertResult:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    rejected: int = 0  # rows skipped because a value could not be stored exactly

    def add(self, other: "UpsertResult") -> None:
        self.inserted += other.inserted
        self.updated += other.updated
        self.unchanged += other.unchanged
        self.rejected += other.rejected

    @property
    def total(self) -> int:
        return self.inserted + self.updated + self.unchanged


def series_row(s: Series) -> dict[str, Any]:
    return {
        "ticker": s.ticker,
        "title": s.title,
        "frequency": s.frequency,
        "category": s.category,
        "tags": s.tags,
        "fee_type": s.fee_type,
        "fee_multiplier_e6": to_e6(s.fee_multiplier),
        "source_updated_at": s.last_updated_ts,
    }


def event_row(e: Event) -> dict[str, Any]:
    return {
        "event_ticker": e.event_ticker,
        "series_ticker": e.series_ticker,
        "title": e.title,
        "sub_title": e.sub_title,
        "mutually_exclusive": e.mutually_exclusive,
        "strike_date": e.strike_date,
        "strike_period": e.strike_period,
        "source_updated_at": e.last_updated_ts,
    }


def market_row(m: Market) -> dict[str, Any]:
    return {
        "ticker": m.ticker,
        "event_ticker": m.event_ticker,
        "market_type": m.market_type,
        "status": m.status,
        "yes_sub_title": m.yes_sub_title,
        "no_sub_title": m.no_sub_title,
        "created_time": m.created_time,
        "updated_time": m.updated_time,
        "open_time": m.open_time,
        "close_time": m.close_time,
        "latest_expiration_time": m.latest_expiration_time,
        "result": m.result,
        "settlement_value_e6": to_e6(m.settlement_value_dollars),
        "settlement_ts": m.settlement_ts,
        "rules_primary": m.rules_primary,
        "rules_secondary": m.rules_secondary,
        "strike_type": m.strike_type,
        "floor_strike_e6": to_e6(m.floor_strike),
        "cap_strike_e6": to_e6(m.cap_strike),
        "exchange_index": m.exchange_index,
    }


async def upsert(
    engine: AsyncEngine, table: Table, rows: Sequence[dict[str, Any]], key: str
) -> UpsertResult:
    """Insert new rows and update only rows whose values changed.

    Unchanged rows are not rewritten, so refreshing 100k+ unchanged markets leaves no dead
    tuples behind. Duplicate keys within ``rows`` collapse to the last one.
    """
    result = UpsertResult()
    if not rows:
        return result
    unique = list({row[key]: row for row in rows}.values())
    columns = list(unique[0])
    compare = [c for c in columns if c != key]
    batch = max(1, MAX_BIND_PARAMS // len(columns))
    for start in range(0, len(unique), batch):
        chunk = unique[start : start + batch]
        insert_stmt = pg_insert(table).values(chunk)
        excluded = insert_stmt.excluded
        upsert_stmt: Any = insert_stmt.on_conflict_do_update(
            index_elements=[key],
            set_={**{c: excluded[c] for c in compare}, "updated_at": func.now()},
            where=tuple_(*[table.c[c] for c in compare]).is_distinct_from(
                tuple_(*[excluded[c] for c in compare])
            ),
        ).returning(literal_column("(xmax = 0)").label("inserted"))
        async with engine.begin() as conn:
            returned = (await conn.execute(upsert_stmt)).all()
        inserted = sum(1 for (was_inserted,) in returned if was_inserted)
        result.inserted += inserted
        result.updated += len(returned) - inserted
        result.unchanged += len(chunk) - len(returned)
    return result


async def unknown_tickers(engine: AsyncEngine, limit: int = 2_000) -> list[str]:
    """Tickers the stream created as placeholders and discovery has not described yet."""
    query = (
        select(tables.markets.c.ticker)
        .where(tables.markets.c.status == "unknown")
        .order_by(tables.markets.c.id)
        .limit(limit)
    )
    async with engine.connect() as conn:
        return [ticker for (ticker,) in await conn.execute(query)]


async def get_state(engine: AsyncEngine, key: str) -> str | None:
    query = select(tables.discovery_state.c.value).where(tables.discovery_state.c.key == key)
    async with engine.connect() as conn:
        return (await conn.execute(query)).scalar_one_or_none()


async def set_state(engine: AsyncEngine, key: str, value: str) -> None:
    stmt = pg_insert(tables.discovery_state).values(key=key, value=value)
    stmt = stmt.on_conflict_do_update(
        index_elements=["key"], set_={"value": value, "updated_at": func.now()}
    )
    async with engine.begin() as conn:
        await conn.execute(stmt)


def epoch(moment: datetime) -> int:
    return int(moment.timestamp())
