"""Quote, move and volatility metrics for the watched markets (slice 4.2).

Units: every ``value_e6`` is millionths of its natural unit. Prices, spreads, changes and
volatility are in dollars (a probability point is $0.01 = 10000); sizes and volumes are in
contracts; ages are in seconds. Source facts are exact (a size of 12.34 contracts is stored as
12340000); *derived* numbers (a mid price, an average, a square root) are rounded to the nearest
millionth, and say so below. A metric is simply absent when it cannot be computed honestly (no
quote in the window, no tick as old as the window start): nothing is invented or carried over.

Read from the raw ``tickers`` table, not ``ticker_1m``: the aggregate is materialized-only and
measured ~3 minutes behind the newest tick, too stale for a 15-minute move.
"""

from collections.abc import Sequence
from datetime import timedelta
from typing import Any

from kalshi_core.fixedpoint import E2_TO_E6, E6
from sqlalchemy import text

from kalshiterm_server.analytics.base import AnalysisContext, Result
from kalshiterm_server.analytics.universe import covered_since, watched_market_ids


class MarketQuotes:
    """Latest bid, ask, mid, spread and top-of-book sizes, plus the 15-minute average spread."""

    name = "market_quotes"
    interval = timedelta(seconds=60)
    timeout = timedelta(seconds=45)
    window = timedelta(minutes=15)  # a quote older than this is not reported

    SQL = text(
        """
        SELECT m.id AS market_id, q.ts, q.yes_bid_e6, q.yes_ask_e6, q.yes_bid_size_e2,
               q.yes_ask_size_e2, a.avg_spread_e6
        FROM unnest(CAST(:ids AS integer[])) AS m(id)
        CROSS JOIN LATERAL (
            SELECT ts, yes_bid_e6, yes_ask_e6, yes_bid_size_e2, yes_ask_size_e2
            FROM tickers t
            WHERE t.market_id = m.id AND t.ts > :since AND t.ts <= :now
              AND t.yes_bid_e6 IS NOT NULL AND t.yes_ask_e6 IS NOT NULL
            ORDER BY t.ts DESC LIMIT 1) q
        CROSS JOIN LATERAL (
            SELECT round(avg(yes_ask_e6 - yes_bid_e6))::bigint AS avg_spread_e6
            FROM tickers t
            WHERE t.market_id = m.id AND t.ts > :since AND t.ts <= :now
              AND t.yes_bid_e6 IS NOT NULL AND t.yes_ask_e6 IS NOT NULL) a
        """
    )

    async def run(self, ctx: AnalysisContext) -> Sequence[Result]:
        ids = await watched_market_ids(ctx.engine)
        if not ids:
            return []
        async with ctx.engine.connect() as conn:
            rows = (
                await conn.execute(
                    self.SQL, {"ids": ids, "since": ctx.now - self.window, "now": ctx.now}
                )
            ).all()
        out: list[Result] = []
        for r in rows:
            m = r.market_id
            bid, ask = r.yes_bid_e6, r.yes_ask_e6
            out += [
                Result("bid", bid, market_id=m),
                Result("ask", ask, market_id=m),
                Result("mid", (bid + ask + 1) // 2, market_id=m),  # rounded half up
                Result("spread", ask - bid, market_id=m),
                Result("spread_avg_15m", r.avg_spread_e6, market_id=m),
                Result("quote_age", round((ctx.now - r.ts).total_seconds() * E6), market_id=m),
            ]
            if r.yes_bid_size_e2 is not None:
                out.append(Result("bid_size", r.yes_bid_size_e2 * E2_TO_E6, market_id=m))
            if r.yes_ask_size_e2 is not None:
                out.append(Result("ask_size", r.yes_ask_size_e2 * E2_TO_E6, market_id=m))
        return out


class MarketMoves:
    """Last price, and its change and the traded volume over 15 minutes, 1 hour and 24 hours.

    The baseline for a window is the last tick at or before the window start, so a market that
    simply did not trade in the window has a change of exactly zero. A window is skipped when it
    starts before recording last resumed (see ``covered_since``: after a restart the 24-hour
    metrics return 24 hours later), and for a market with no tick that old (new, or older than
    the 14-day raw retention).
    """

    name = "market_moves"
    interval = timedelta(seconds=60)
    timeout = timedelta(seconds=45)
    windows = (
        ("15m", timedelta(minutes=15)),
        ("1h", timedelta(hours=1)),
        ("24h", timedelta(hours=24)),
    )

    LATEST = text(
        """
        SELECT m.id AS market_id, p.price_e6, v.volume_e2
        FROM unnest(CAST(:ids AS integer[])) AS m(id)
        CROSS JOIN LATERAL (
            SELECT price_e6 FROM tickers t
            WHERE t.market_id = m.id AND t.price_e6 IS NOT NULL AND t.ts <= :now AND t.ts > :since
            ORDER BY t.ts DESC LIMIT 1) p
        LEFT JOIN LATERAL (
            SELECT volume_e2 FROM tickers t
            WHERE t.market_id = m.id AND t.volume_e2 IS NOT NULL AND t.ts <= :now
              AND t.ts > :since
            ORDER BY t.ts DESC LIMIT 1) v ON true
        """
    )
    BASELINE = text(
        """
        SELECT m.id AS market_id, p.price_e6, v.volume_e2
        FROM unnest(CAST(:ids AS integer[])) AS m(id)
        LEFT JOIN LATERAL (
            SELECT price_e6 FROM tickers t
            WHERE t.market_id = m.id AND t.price_e6 IS NOT NULL AND t.ts <= :at
            ORDER BY t.ts DESC LIMIT 1) p ON true
        LEFT JOIN LATERAL (
            SELECT volume_e2 FROM tickers t
            WHERE t.market_id = m.id AND t.volume_e2 IS NOT NULL AND t.ts <= :at
            ORDER BY t.ts DESC LIMIT 1) v ON true
        """
    )

    async def run(self, ctx: AnalysisContext) -> Sequence[Result]:
        ids = await watched_market_ids(ctx.engine)
        if not ids:
            return []
        async with ctx.engine.connect() as conn:
            latest = {
                r.market_id: r
                for r in await conn.execute(
                    self.LATEST,
                    {"ids": ids, "now": ctx.now, "since": ctx.now - self.windows[-1][1]},
                )
            }
            baselines: dict[str, dict[int, Any]] = {}
            covered = await covered_since(ctx.engine)
            for label, span in self.windows:
                if covered is not None and ctx.now - span < covered:
                    continue  # recording was interrupted inside this window
                baselines[label] = {
                    r.market_id: r
                    for r in await conn.execute(self.BASELINE, {"ids": ids, "at": ctx.now - span})
                }
        out: list[Result] = []
        for market_id, now in latest.items():
            out.append(Result("last", now.price_e6, market_id=market_id))
            for label, _ in self.windows:
                then = baselines.get(label, {}).get(market_id)
                if then is None:
                    continue
                if then.price_e6 is not None:
                    out.append(
                        Result(f"change_{label}", now.price_e6 - then.price_e6, market_id=market_id)
                    )
                if now.volume_e2 is not None and then.volume_e2 is not None and label != "15m":
                    out.append(
                        Result(
                            f"volume_{label}",
                            (now.volume_e2 - then.volume_e2) * E2_TO_E6,
                            market_id=market_id,
                        )
                    )
        return out


class MarketVolatility:
    """Realized volatility over the last hour: the per-minute root-mean-square change of the
    last trade price, in dollars (rounded to a millionth).

    Prices are sampled once per minute that has a tick. A step across minutes without ticks is
    scaled as a random walk (its squared change is divided by the minutes it spans), and a
    market needs at least 20 such steps in the hour, so one that barely trades is not reported
    as "calm". The hour is cut short where recording was interrupted (``covered_since``).
    """

    name = "market_volatility"
    interval = timedelta(seconds=300)
    timeout = timedelta(seconds=120)
    min_active_minutes = 20

    SQL = text(
        """
        WITH minutes AS (
            SELECT market_id, time_bucket('1 minute', ts) AS bucket,
                   last(price_e6, ts) FILTER (WHERE price_e6 IS NOT NULL) AS p
            FROM tickers
            WHERE market_id = ANY(CAST(:ids AS integer[])) AND ts > :since AND ts <= :now
            GROUP BY 1, 2),
        steps AS (
            SELECT market_id, bucket, p,
                   p - lag(p) OVER w AS dp,
                   extract(epoch FROM bucket - lag(bucket) OVER w) / 60 AS gap
            FROM minutes WHERE p IS NOT NULL
            WINDOW w AS (PARTITION BY market_id ORDER BY bucket))
        SELECT market_id, count(*) AS active_minutes,
               -- random-walk scaling: variance grows with elapsed time, so the per-minute variance
               -- is the total squared change over the total minutes the steps cover
               sqrt(sum(dp::float8 * dp) / sum(gap)) AS rms
        FROM steps WHERE dp IS NOT NULL
        GROUP BY market_id
        """
    )

    async def run(self, ctx: AnalysisContext) -> Sequence[Result]:
        ids = await watched_market_ids(ctx.engine)
        if not ids:
            return []
        since = ctx.now - timedelta(hours=1)
        covered = await covered_since(ctx.engine)
        if covered is not None:
            since = max(since, covered)
        async with ctx.engine.connect() as conn:
            rows = (
                await conn.execute(self.SQL, {"ids": ids, "since": since, "now": ctx.now})
            ).all()
        return [
            Result("volatility_1h", round(r.rms), market_id=r.market_id)
            for r in rows
            if r.active_minutes >= self.min_active_minutes and r.rms is not None
        ]
