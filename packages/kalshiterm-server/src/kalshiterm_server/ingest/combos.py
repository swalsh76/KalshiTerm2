"""Combo (multivariate) markets: per-minute universe counters and rows for traded combos.

About 6 M combos are created per day and only ~12 % ever show activity, so a per-market row is
kept only for combos that have *traded*; ``combo_stats_1m`` counts the whole universe.
"""

import logging
from collections import OrderedDict
from datetime import datetime
from typing import Any

from kalshi_core.models import Market
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from kalshiterm_server.fixedpoint import to_e6
from kalshiterm_server.storage.market_ids import MarketIds

log = logging.getLogger("kalshiterm_server")

AGG_FIELDS = (
    "created",
    "determined",
    "settled",
    "close_updated",
    "ticker_msgs",
    "trades",
    "contracts_e2",
)
_AGG_INDEX = {name: i for i, name in enumerate(AGG_FIELDS)}

UPSERT_STATS = """
INSERT INTO combo_stats_1m
    (minute, family, created, determined, settled, close_updated, ticker_msgs, trades, contracts_e2)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (minute, family) DO UPDATE SET
    created = combo_stats_1m.created + EXCLUDED.created,
    determined = combo_stats_1m.determined + EXCLUDED.determined,
    settled = combo_stats_1m.settled + EXCLUDED.settled,
    close_updated = combo_stats_1m.close_updated + EXCLUDED.close_updated,
    ticker_msgs = combo_stats_1m.ticker_msgs + EXCLUDED.ticker_msgs,
    trades = combo_stats_1m.trades + EXCLUDED.trades,
    contracts_e2 = combo_stats_1m.contracts_e2 + EXCLUDED.contracts_e2
"""

UPDATE_DETERMINED = """
UPDATE combo_markets c SET result = u.result, settlement_value_e6 = u.value,
       status = 'determined', updated_at = now()
FROM unnest($1::text[], $2::text[], $3::bigint[]) AS u(ticker, result, value)
WHERE c.ticker = u.ticker
"""
UPDATE_SETTLED = """
UPDATE combo_markets c SET settled_at = u.at, status = 'finalized', updated_at = now()
FROM unnest($1::text[], $2::timestamptz[]) AS u(ticker, at) WHERE c.ticker = u.ticker
"""
UPDATE_CLOSE = """
UPDATE combo_markets c SET close_time = u.at, updated_at = now()
FROM unnest($1::text[], $2::timestamptz[]) AS u(ticker, at) WHERE c.ticker = u.ticker
"""


def family_of(ticker: str) -> str:
    """Ticker family used to key the universe counters (e.g. ``KXMVECROSSCATEGORY``)."""
    return ticker.split("-", 1)[0]


class Aggregates:
    """In-memory per-(minute, family) counters, flushed together with a batch."""

    def __init__(self) -> None:
        self._rows: dict[tuple[datetime, str], list[int]] = {}

    def add(self, moment: datetime, family: str, field: str, amount: int = 1) -> None:
        key = (moment.replace(second=0, microsecond=0), family)
        self._rows.setdefault(key, [0] * len(AGG_FIELDS))[_AGG_INDEX[field]] += amount

    def __bool__(self) -> bool:
        return bool(self._rows)

    def merge(self, other: "Aggregates") -> None:
        """Fold ``other`` into this one (used to put counters back after a failed write)."""
        for key, values in other._rows.items():
            mine = self._rows.setdefault(key, [0] * len(AGG_FIELDS))
            for i, amount in enumerate(values):
                mine[i] += amount

    def records(self) -> list[tuple[Any, ...]]:
        return [(minute, family, *values) for (minute, family), values in self._rows.items()]


class ComboStore:
    """Rows for traded combo markets, with a bounded id cache."""

    def __init__(self, engine: AsyncEngine, market_ids: MarketIds, cache_size: int = 500_000):
        self._engine = engine
        self._market_ids = market_ids
        self._cache: OrderedDict[str, int] = OrderedDict()
        self._cache_size = cache_size
        self.created = 0

    def _remember(self, ticker: str, combo_id: int) -> None:
        self._cache[ticker] = combo_id
        self._cache.move_to_end(ticker)
        while len(self._cache) > self._cache_size:
            self._cache.popitem(last=False)

    async def known_ids(self, tickers: set[str]) -> dict[str, int]:
        """Ids of the combos that already have a row (a miss is simply absent)."""
        found = {t: self._cache[t] for t in tickers if t in self._cache}
        missing = [t for t in tickers if t not in found]
        if missing:
            async with self._engine.connect() as conn:
                rows = await conn.execute(
                    text("SELECT ticker, id FROM combo_markets WHERE ticker = ANY(:t)"),
                    {"t": missing},
                )
                for ticker, combo_id in rows:
                    found[ticker] = combo_id
                    self._remember(ticker, combo_id)
        return found

    async def create(self, markets: list[Market], first_trade: dict[str, datetime]) -> None:
        """Insert rows for combos that just traded; legs become ids of ordinary markets."""
        if not markets:
            return
        leg_tickers = {leg.market_ticker for m in markets for leg in m.mve_selected_legs or []}
        leg_ids = await self._market_ids.resolve(leg_tickers) if leg_tickers else {}
        params = []
        for m in markets:
            legs = m.mve_selected_legs or []
            params.append(
                {
                    "ticker": m.ticker,
                    "collection": m.mve_collection_ticker or "",
                    "event": m.event_ticker,
                    "status": m.status,
                    "created": m.created_time,
                    "opened": m.open_time,
                    "closed": m.close_time,
                    "first_trade": first_trade[m.ticker],
                    "result": m.result,
                    "settlement": to_e6(m.settlement_value_dollars),
                    "settled": m.settlement_ts,
                    "leg_ids": [leg_ids[leg.market_ticker] for leg in legs],
                    "leg_yes": [leg.side == "yes" for leg in legs],
                }
            )
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO combo_markets (ticker, collection_ticker, event_ticker, status, "
                    "created_time, open_time, close_time, first_trade_at, result, "
                    "settlement_value_e6, settled_at, leg_market_ids, leg_yes) VALUES "
                    "(:ticker, :collection, :event, :status, :created, :opened, :closed, "
                    ":first_trade, :result, :settlement, :settled, CAST(:leg_ids AS integer[]), "
                    "CAST(:leg_yes AS boolean[])) ON CONFLICT (ticker) DO NOTHING"
                ),
                params,
            )
        self.created += len(params)
