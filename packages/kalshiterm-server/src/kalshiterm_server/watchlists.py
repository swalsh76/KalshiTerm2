"""Per-user watchlists: what each user wants the server to capture.

The limits exist because every watched market costs orderbook storage (measured: one fast
crypto book can be a third of all orderbook rows). Both are checked inside one transaction
under a lock, so two simultaneous requests cannot both slip under a cap.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

COMBO_PREFIX = "KXMVE"  # combos have no stored orderbooks (PLAN decision 17)
LOCK_KEY = 7_001  # advisory lock serialising watchlist additions


class WatchlistError(Exception):
    def __init__(self, status: int, code: str, **extra: Any) -> None:
        super().__init__(code)
        self.status = status
        self.code = code
        self.extra = extra


@dataclass(frozen=True, slots=True)
class WatchedMarket:
    ticker: str
    event_title: str | None
    status: str
    added_at: datetime
    capturing: bool  # the ingest process is storing this market's orderbook right now


@dataclass(slots=True)
class Listing:
    markets: list[WatchedMarket] = field(default_factory=list)


async def list_watched(engine: AsyncEngine, user_id: int) -> list[WatchedMarket]:
    async with engine.connect() as conn:
        rows = await conn.execute(
            text(
                "SELECT m.ticker, e.title AS event_title, m.status, w.added_at, "
                "EXISTS (SELECT 1 FROM watchlist_periods p WHERE p.market_id = m.id "
                "        AND p.removed_at IS NULL) AS capturing "
                "FROM user_watchlists w JOIN markets m ON m.id = w.market_id "
                "LEFT JOIN events e ON e.event_ticker = m.event_ticker "
                "WHERE w.user_id = :u ORDER BY m.ticker"
            ),
            {"u": user_id},
        )
        return [WatchedMarket(*r) for r in rows]


async def add(
    engine: AsyncEngine, user_id: int, ticker: str, max_per_user: int, max_total: int
) -> bool:
    """Put a market on the user's list. True if added, False if it was already there."""
    async with engine.begin() as conn:
        await conn.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": LOCK_KEY})
        market = (
            await conn.execute(
                text(
                    "SELECT id, settlement_ts IS NOT NULL AS settled FROM markets WHERE ticker = :t"
                ),
                {"t": ticker},
            )
        ).first()
        if market is None:
            raise WatchlistError(404, "not_found")
        if ticker.startswith(COMBO_PREFIX):
            raise WatchlistError(422, "not_watchable", reason="combo_market")
        if market.settled:
            raise WatchlistError(422, "not_watchable", reason="settled")
        already = (
            await conn.execute(
                text("SELECT 1 FROM user_watchlists WHERE user_id = :u AND market_id = :m"),
                {"u": user_id, "m": market.id},
            )
        ).first()
        if already:
            return False
        mine = (
            await conn.execute(
                text("SELECT count(*) FROM user_watchlists WHERE user_id = :u"), {"u": user_id}
            )
        ).scalar_one()
        if mine >= max_per_user:
            raise WatchlistError(409, "watchlist_limit", limit=max_per_user)
        wanted_by_anyone = (
            await conn.execute(
                text("SELECT 1 FROM user_watchlists WHERE market_id = :m LIMIT 1"),
                {"m": market.id},
            )
        ).first()
        if not wanted_by_anyone:  # a market someone already wants costs nothing extra
            distinct = (
                await conn.execute(text("SELECT count(DISTINCT market_id) FROM user_watchlists"))
            ).scalar_one()
            if distinct >= max_total:
                raise WatchlistError(409, "server_watchlist_full", limit=max_total)
        await conn.execute(
            text("INSERT INTO user_watchlists (user_id, market_id) VALUES (:u, :m)"),
            {"u": user_id, "m": market.id},
        )
    return True


async def remove(engine: AsyncEngine, user_id: int, ticker: str) -> bool:
    """Take a market off the user's list. True if it was on it."""
    async with engine.begin() as conn:
        market_id = (
            await conn.execute(text("SELECT id FROM markets WHERE ticker = :t"), {"t": ticker})
        ).scalar_one_or_none()
        if market_id is None:
            raise WatchlistError(404, "not_found")
        result = await conn.execute(
            text("DELETE FROM user_watchlists WHERE user_id = :u AND market_id = :m"),
            {"u": user_id, "m": market_id},
        )
    return bool(result.rowcount)
