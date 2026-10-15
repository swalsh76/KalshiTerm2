"""Cross-market arbitrage, bid side (slice 4.3).

In an event whose markets are *mutually exclusive*, at most one market can resolve YES. Selling
YES in a set S of its markets (buying NO) at their best bids collects the sum of those bids and
pays out $1 on the single market that wins, so the worst case is ``sum(bids in S) - 1`` per
set of contracts, before fees. That is a real arbitrage whenever it is positive, and it holds
for any subset of the markets, so a market with no usable bid is simply left out; leaving legs
out can only lower the sum. (The mirror image, buying YES everywhere when the asks sum to less
than $1, would also need the event to be *exhaustive*, which Kalshi does not flag; parked.)

What the numbers mean: ``edge`` is per contract set; ``size`` is the contracts every used leg
can take at its best bid (the smallest top-of-book size); ``fees`` are Kalshi's taker fees for
that size on every leg (see ``fees``), each leg's order rounded up to ``fee_round``; ``profit``
is ``size * edge - fees``, which can be negative when fees eat the edge. Quotes come from the
newest ticker per market, which measured (2026-10-12, against Kalshi's order book) as the best
available view but not exact: this is an *indication*, to be confirmed against the live book
before anything is traded. A leg counts only if its newest tick is after recording last resumed
(``covered_since``) and no older than ``max_quote_age``.
"""

from collections.abc import Sequence
from datetime import timedelta
from typing import Any

from kalshi_core.fees import round_up, taker_fee_e6, taker_multiplier
from kalshi_core.fixedpoint import E2_TO_E6, E6
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import text

from kalshiterm_server.analytics.base import AnalysisContext, Result
from kalshiterm_server.analytics.universe import covered_since


class ArbitrageSettings(BaseSettings):
    """``KTERM_ARBITRAGE_*`` environment variables."""

    model_config = SettingsConfigDict(env_prefix="KTERM_ARBITRAGE_", extra="ignore")

    # Each leg's fee is rounded up to this unit: $0.01 is what a non-direct member's balance is
    # aligned to (the conservative reading of Kalshi's rounding); 100 would be $0.0001.
    fee_round_e6: int = 10_000
    min_edge_e6: int = 0  # report only events whose edge exceeds this (millionths of a dollar)
    max_quote_age_seconds: int = 600


class ArbitrageBid:
    name = "arbitrage_bid"
    interval = timedelta(seconds=60)
    timeout = timedelta(seconds=45)

    SQL = text(
        """
        WITH fresh AS (
            SELECT DISTINCT market_id FROM tickers WHERE ts > :since AND ts <= :now),
        legs AS (
            SELECT m.event_ticker, m.id AS market_id, q.ts, q.yes_bid_e6, q.yes_bid_size_e2
            FROM fresh f
            JOIN markets m ON m.id = f.market_id AND m.status = 'active'
            JOIN events e ON e.event_ticker = m.event_ticker AND e.mutually_exclusive
            CROSS JOIN LATERAL (
                SELECT ts, yes_bid_e6, yes_bid_size_e2 FROM tickers t
                WHERE t.market_id = m.id AND t.ts > :since AND t.ts <= :now
                ORDER BY t.ts DESC LIMIT 1) q
            WHERE q.yes_bid_e6 > 0 AND q.yes_bid_size_e2 > 0),
        candidates AS (
            SELECT event_ticker FROM legs GROUP BY event_ticker
            HAVING count(*) >= 2 AND sum(yes_bid_e6) > :threshold)
        SELECT l.event_ticker, l.market_id, l.ts, l.yes_bid_e6, l.yes_bid_size_e2,
               s.fee_type, s.fee_multiplier_e6,
               (SELECT count(*) FROM markets x
                WHERE x.event_ticker = l.event_ticker AND x.status = 'active') AS legs_total
        FROM legs l
        JOIN candidates c USING (event_ticker)
        JOIN events e ON e.event_ticker = l.event_ticker
        JOIN series s ON s.ticker = e.series_ticker
        ORDER BY l.event_ticker, l.market_id
        """
    )

    def __init__(
        self,
        fee_round_e6: int | None = None,
        min_edge_e6: int | None = None,
        max_quote_age_seconds: int | None = None,
    ) -> None:
        env = ArbitrageSettings()
        self.fee_round_e6 = env.fee_round_e6 if fee_round_e6 is None else fee_round_e6
        self.min_edge_e6 = env.min_edge_e6 if min_edge_e6 is None else min_edge_e6
        age = env.max_quote_age_seconds if max_quote_age_seconds is None else max_quote_age_seconds
        self.max_quote_age = timedelta(seconds=age)

    async def run(self, ctx: AnalysisContext) -> Sequence[Result]:
        since = ctx.now - self.max_quote_age
        covered = await covered_since(ctx.engine)
        if covered is not None:
            since = max(since, covered)
        async with ctx.engine.connect() as conn:
            rows = (
                await conn.execute(
                    self.SQL,
                    {
                        "since": since,
                        "now": ctx.now,
                        "threshold": E6 + self.min_edge_e6,
                    },
                )
            ).all()
        events: dict[str, list[Any]] = {}
        for row in rows:
            events.setdefault(row.event_ticker, []).append(row)
        out: list[Result] = []
        for event, legs in events.items():
            out += self._evaluate(event, legs, ctx)
        return out

    def _evaluate(self, event: str, legs: list[Any], ctx: AnalysisContext) -> list[Result]:
        multiplier = taker_multiplier(legs[0].fee_type, legs[0].fee_multiplier_e6)
        if multiplier is None:
            return []  # a fee schedule we do not model: say nothing rather than guess
        bid_sum = sum(leg.yes_bid_e6 for leg in legs)
        edge = bid_sum - E6
        if edge <= self.min_edge_e6:
            return []
        size_e2 = min(leg.yes_bid_size_e2 for leg in legs)
        fees = sum(
            round_up(taker_fee_e6(multiplier, size_e2, leg.yes_bid_e6), self.fee_round_e6)
            for leg in legs
        )
        profit = size_e2 * edge // 100 - fees
        oldest = max((ctx.now - leg.ts).total_seconds() for leg in legs)

        def result(metric: str, value: int) -> Result:
            return Result(metric, value, event_ticker=event)

        return [
            result("bid_sum", bid_sum),
            result("edge", edge),
            result("size", size_e2 * E2_TO_E6),
            result("fees", fees),
            result("profit", profit),
            result("legs", len(legs) * E6),
            result("legs_total", legs[0].legs_total * E6),
            result("oldest_quote_age", round(oldest * E6)),
        ]
