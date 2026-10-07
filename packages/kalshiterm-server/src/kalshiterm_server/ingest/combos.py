"""Combo (multivariate) markets: per-minute universe counters and a large-trade log.

Measurements (see PLAN decision 17) showed per-combo rows are not worth their cost: combo
outcomes are fully determined by their legs and ~6 % of traded combos hold ~73 % of the dollars.
So every combo message is *counted* per minute and ticker family, and only individual trades
above a dollar threshold are stored (by ticker text, no per-market row).
"""

from datetime import datetime
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Any

AGG_FIELDS = (
    "created",
    "determined",
    "settled",
    "close_updated",
    "ticker_msgs",
    "trades",
    "contracts_e2",
    "notional_e6",
)
_AGG_INDEX = {name: i for i, name in enumerate(AGG_FIELDS)}

# Taker dollars at risk in a combo trade of this size or more are logged individually.
LARGE_TRADE_E6 = 500 * 1_000_000

UPSERT_STATS = """
INSERT INTO combo_stats_1m
    (minute, family, created, determined, settled, close_updated, ticker_msgs, trades,
     contracts_e2, notional_e6)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (minute, family) DO UPDATE SET
    created = combo_stats_1m.created + EXCLUDED.created,
    determined = combo_stats_1m.determined + EXCLUDED.determined,
    settled = combo_stats_1m.settled + EXCLUDED.settled,
    close_updated = combo_stats_1m.close_updated + EXCLUDED.close_updated,
    ticker_msgs = combo_stats_1m.ticker_msgs + EXCLUDED.ticker_msgs,
    trades = combo_stats_1m.trades + EXCLUDED.trades,
    contracts_e2 = combo_stats_1m.contracts_e2 + EXCLUDED.contracts_e2,
    notional_e6 = combo_stats_1m.notional_e6 + EXCLUDED.notional_e6
"""

LARGE_TRADE_COLUMNS = [
    "ts", "received_at", "ticker", "trade_id", "yes_price_e6", "count_e2", "taker_side",
    "notional_e6",
]  # fmt: skip


def family_of(ticker: str) -> str:
    """Ticker family used to key the universe counters (e.g. ``KXMVECROSSCATEGORY``)."""
    return ticker.split("-", 1)[0]


def notional_e6(count: Decimal, price_paid: Decimal) -> int:
    """Taker dollars at risk (contracts x price paid) in millionths.

    A derived statistic, not a stored fact: price (up to 6 decimals) times count (2 decimals)
    can have 8, so this rounds (half-even) instead of raising like the fixed-point converters.
    """
    return int((count * price_paid * 1_000_000).quantize(Decimal(1), rounding=ROUND_HALF_EVEN))


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
