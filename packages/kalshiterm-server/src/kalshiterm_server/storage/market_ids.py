"""Ticker -> integer market id, creating a placeholder row for tickers not seen yet.

The streaming tables reference ``markets.id`` (4 bytes) instead of the ticker text. Discovery
refreshes ``markets`` only periodically, so a stream message can name a market that is not
there yet; dropping it would lose data, so a placeholder row (``status='unknown'``) is
inserted and discovery fills it in on its next pass.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

PLACEHOLDER_STATUS = "unknown"


def guess_event_ticker(ticker: str) -> str:
    """Kalshi tickers are ``EVENT-SUFFIX``; good enough until discovery supplies the truth."""
    head, sep, _ = ticker.rpartition("-")
    return head if sep else ticker


class MarketIds:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine
        self._ids: dict[str, int] = {}
        self.placeholders_created = 0

    def __len__(self) -> int:
        return len(self._ids)

    async def resolve(self, tickers: set[str]) -> dict[str, int]:
        """Ids for every ticker, creating placeholder rows where needed."""
        missing = [t for t in tickers if t not in self._ids]
        if missing:
            async with self._engine.begin() as conn:
                found = await conn.execute(
                    text("SELECT ticker, id FROM markets WHERE ticker = ANY(:t)"), {"t": missing}
                )
                for ticker, market_id in found:
                    self._ids[ticker] = market_id
                still = [t for t in missing if t not in self._ids]
                if still:
                    created = await conn.execute(
                        text(
                            "INSERT INTO markets (ticker, event_ticker, market_type, status) "
                            "SELECT t, e, 'binary', :status "
                            "FROM unnest(CAST(:tickers AS text[]), CAST(:events AS text[])) "
                            "AS u(t, e) "
                            "ON CONFLICT (ticker) DO NOTHING RETURNING ticker, id"
                        ),
                        {
                            "tickers": still,
                            "events": [guess_event_ticker(t) for t in still],
                            "status": PLACEHOLDER_STATUS,
                        },
                    )
                    for ticker, market_id in created:
                        self._ids[ticker] = market_id
                        self.placeholders_created += 1
                    # A concurrent writer (e.g. discovery) may have inserted some first.
                    raced = [t for t in still if t not in self._ids]
                    if raced:
                        found = await conn.execute(
                            text("SELECT ticker, id FROM markets WHERE ticker = ANY(:t)"),
                            {"t": raced},
                        )
                        for ticker, market_id in found:
                            self._ids[ticker] = market_id
        return {t: self._ids[t] for t in tickers}
