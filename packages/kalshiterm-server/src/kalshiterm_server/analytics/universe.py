"""Which markets analyzers look at: the ones somebody is watching.

Analyzing every market that ticks (about 35,000 in an hour, measured 2026-10-12) at one-minute
cadence would store ~250 million rows a day, so analytics covers the *watch union*: every market
with an open watchlist period (file, user and automatic entries all open one) plus pinned
markets. That is the same set the ingestor keeps orderbooks for, and the set per-user alerts
(slice 4.4) can refer to.
"""

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


async def watched_market_ids(engine: AsyncEngine) -> list[int]:
    async with engine.connect() as conn:
        rows = await conn.execute(
            text(
                "SELECT market_id FROM watchlist_periods WHERE removed_at IS NULL "
                "UNION SELECT market_id FROM pins ORDER BY 1"
            )
        )
        return [row[0] for row in rows]


async def covered_since(engine: AsyncEngine) -> datetime | None:
    """The moment from which ticks have been recorded without interruption.

    A market that is quiet and a market we were not recording look the same in ``tickers``:
    neither has ticks. So a window metric ("change over 24 hours") is only honest if the whole
    window lies after the last time recording resumed. That moment is the newest ``ended_at`` in
    the ingest gap log (never ``started_at``: a long outage is truncated to the backfill window
    there). The governor's ``delta_shed`` rows concern orderbook deltas only and are ignored.
    None means the log is empty, i.e. nothing is known to be missing.
    """
    async with engine.connect() as conn:
        return (  # type: ignore[no-any-return]
            await conn.execute(
                text("SELECT max(ended_at) FROM ingest_gaps WHERE reason <> 'delta_shed'")
            )
        ).scalar_one()
