"""The built-in market analyzers, on crafted ticks and a fixed clock."""

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from kalshiterm_server import db
from kalshiterm_server.analytics.base import AnalysisContext, Result, check_result
from kalshiterm_server.analytics.builtin.market_metrics import (
    MarketMoves,
    MarketQuotes,
    MarketVolatility,
)
from kalshiterm_server.analytics.universe import covered_since, watched_market_ids
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from timescale_jobs import quiet_background_jobs

pytestmark = pytest.mark.db

NOW = datetime(2026, 10, 12, 12, 0, tzinfo=UTC)


@pytest.fixture
async def engine(migrated_db_url: str) -> AsyncIterator[AsyncEngine]:
    engine = db.make_engine(migrated_db_url)
    await quiet_background_jobs(engine)
    yield engine
    await engine.dispose()


async def add_market(engine: AsyncEngine, ticker: str, watched: bool = True) -> int:
    async with engine.begin() as conn:
        market_id: int = (
            await conn.execute(
                text(
                    "insert into markets (ticker, event_ticker, market_type, status) "
                    "values (:t, 'E', 'binary', 'active') returning id"
                ),
                {"t": ticker},
            )
        ).scalar_one()
        if watched:
            await conn.execute(
                text("insert into watchlist_periods (market_id, source) values (:m, 'auto')"),
                {"m": market_id},
            )
    return market_id


async def ticks(engine: AsyncEngine, market_id: int, rows: list[dict[str, Any]]) -> None:
    """rows: ts plus any of price, bid, ask (dollars e6), sizes and volume (hundredths)."""
    columns = ("price", "bid", "ask", "bid_size", "ask_size", "volume")
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "insert into tickers (ts, received_at, market_id, price_e6, yes_bid_e6, "
                "yes_ask_e6, yes_bid_size_e2, yes_ask_size_e2, volume_e2) values "
                "(:ts, :ts, :m, :price, :bid, :ask, :bid_size, :ask_size, :volume)"
            ),
            [{"m": market_id, **{c: None for c in columns}, **r} for r in rows],
        )


async def gap_ended(engine: AsyncEngine, at: datetime, reason: str = "startup") -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "insert into ingest_gaps (started_at, ended_at, reason, status) "
                "values (:s, :e, :r, 'done')"
            ),
            {"s": at - timedelta(minutes=1), "e": at, "r": reason},
        )


async def run(analyzer: Any, engine: AsyncEngine) -> dict[tuple[int, str], int]:
    results = await analyzer.run(AnalysisContext(engine, NOW))
    for r in results:
        check_result(analyzer.name, r)  # everything an analyzer returns must be storable
    return {(r.market_id, r.metric): r.value_e6 for r in results}


def ago(**kw: float) -> datetime:
    return NOW - timedelta(**kw)


# ------------------------------------------------------------------ the universe


async def test_the_universe_is_open_watch_periods_plus_pins(engine: AsyncEngine) -> None:
    watched = await add_market(engine, "W")
    closed = await add_market(engine, "C")
    pinned = await add_market(engine, "P", watched=False)
    await add_market(engine, "U", watched=False)
    async with engine.begin() as conn:
        await conn.execute(
            text("update watchlist_periods set removed_at = now() where market_id = :m"),
            {"m": closed},
        )
        await conn.execute(text("insert into pins (market_id) values (:m)"), {"m": pinned})
    assert await watched_market_ids(engine) == sorted([watched, pinned])


async def test_recording_coverage_ignores_orderbook_shedding_rows(engine: AsyncEngine) -> None:
    assert await covered_since(engine) is None
    await gap_ended(engine, ago(hours=30))
    await gap_ended(engine, ago(hours=2), reason="delta_shed")  # not about tickers
    assert await covered_since(engine) == ago(hours=30)
    await gap_ended(engine, ago(minutes=10), reason="reconnect")
    assert await covered_since(engine) == ago(minutes=10)


# ------------------------------------------------------------------ quotes


async def test_quotes_report_the_latest_two_sided_quote_exactly(engine: AsyncEngine) -> None:
    m = await add_market(engine, "Q")
    await ticks(engine, m, [
        {"ts": ago(minutes=10), "bid": 400_000, "ask": 440_000},
        {"ts": ago(minutes=5), "bid": 410_000, "ask": 430_000, "bid_size": 1234, "ask_size": 99},
        {"ts": ago(minutes=2), "price": 420_000},  # a trade-only tick: no quote on it
        {"ts": ago(seconds=30), "bid": 415_000, "ask": 425_001, "bid_size": 5050},  # no ask size
    ])  # fmt: skip
    got = await run(MarketQuotes(), engine)
    assert got[(m, "bid")] == 415_000 and got[(m, "ask")] == 425_001
    assert got[(m, "spread")] == 10_001
    assert got[(m, "mid")] == 420_001  # (415000 + 425001) / 2 = 420000.5, rounded half up
    assert got[(m, "bid_size")] == 50_500_000  # 50.50 contracts, exactly, in millionths
    assert (m, "ask_size") not in got  # unknown stays absent; nothing is carried over
    assert got[(m, "quote_age")] == 30_000_000
    # average over the three two-sided ticks in the window: (40000 + 20000 + 10001) / 3
    assert got[(m, "spread_avg_15m")] == 23_334


async def test_a_quote_older_than_the_window_or_an_unwatched_market_is_not_reported(
    engine: AsyncEngine,
) -> None:
    stale = await add_market(engine, "STALE")
    other = await add_market(engine, "OTHER", watched=False)
    await ticks(engine, stale, [{"ts": ago(minutes=16), "bid": 1, "ask": 2}])
    await ticks(engine, other, [{"ts": ago(seconds=5), "bid": 1, "ask": 2}])
    assert await run(MarketQuotes(), engine) == {}


async def test_no_watched_markets_means_no_results_and_no_queries_that_fail(
    engine: AsyncEngine,
) -> None:
    for analyzer in (MarketQuotes(), MarketMoves(), MarketVolatility()):
        assert await run(analyzer, engine) == {}


# ------------------------------------------------------------------ moves


async def test_changes_use_the_last_tick_at_or_before_each_window_start(
    engine: AsyncEngine,
) -> None:
    m = await add_market(engine, "MOVE")
    await ticks(engine, m, [
        {"ts": ago(hours=30), "price": 300_000, "volume": 1_000},
        {"ts": ago(hours=25), "price": 350_000, "volume": 1_500},   # the 24 h baseline
        {"ts": ago(hours=2), "price": 400_000, "volume": 2_000},    # the 1 h baseline
        {"ts": ago(minutes=40), "price": 500_000, "volume": 2_600},
        {"ts": ago(minutes=20), "price": 520_000, "volume": 2_900},  # the 15 m baseline
        {"ts": ago(minutes=1), "price": 560_000, "volume": 3_100},
    ])  # fmt: skip
    got = await run(MarketMoves(), engine)
    assert got[(m, "last")] == 560_000
    assert got[(m, "change_24h")] == 560_000 - 350_000
    assert got[(m, "change_1h")] == 560_000 - 400_000
    assert got[(m, "change_15m")] == 560_000 - 520_000
    assert got[(m, "volume_24h")] == (3_100 - 1_500) * 10_000  # hundredths -> millionths
    assert got[(m, "volume_1h")] == (3_100 - 2_000) * 10_000
    assert (m, "volume_15m") not in got  # volume is reported for 1 h and 24 h only


async def test_a_market_whose_last_tick_is_over_a_day_old_has_no_current_price(
    engine: AsyncEngine,
) -> None:
    m = await add_market(engine, "DORMANT")
    await ticks(engine, m, [{"ts": ago(hours=30), "price": 640_000, "volume": 7_000}])
    assert await run(MarketMoves(), engine) == {}


async def test_a_quiet_market_with_a_recent_tick_reports_zero_change_and_zero_volume(
    engine: AsyncEngine,
) -> None:
    m = await add_market(engine, "QUIET2")
    await ticks(engine, m, [
        {"ts": ago(hours=30), "price": 640_000, "volume": 7_000},
        {"ts": ago(hours=3), "price": 640_000, "volume": 7_000},  # same price, same volume
    ])  # fmt: skip
    got = await run(MarketMoves(), engine)
    assert [got[(m, k)] for k in ("last", "change_15m", "change_1h", "change_24h")] == [
        640_000, 0, 0, 0,
    ]  # fmt: skip
    assert got[(m, "volume_1h")] == 0 and got[(m, "volume_24h")] == 0


async def test_a_window_with_no_baseline_tick_is_left_out_not_guessed(
    engine: AsyncEngine,
) -> None:
    m = await add_market(engine, "NEW")
    await ticks(engine, m, [
        {"ts": ago(minutes=50), "price": 500_000, "volume": 100},
        {"ts": ago(minutes=1), "price": 530_000, "volume": 400},
    ])  # fmt: skip
    got = await run(MarketMoves(), engine)
    assert got[(m, "change_15m")] == 30_000
    assert (m, "change_1h") not in got and (m, "change_24h") not in got
    assert (m, "volume_1h") not in got and (m, "volume_24h") not in got


async def test_windows_that_reach_back_before_recording_resumed_are_left_out(
    engine: AsyncEngine,
) -> None:
    m = await add_market(engine, "GAP")
    await ticks(engine, m, [
        {"ts": ago(hours=30), "price": 100_000, "volume": 1},
        {"ts": ago(minutes=1), "price": 200_000, "volume": 9},
    ])  # fmt: skip
    assert (m, "change_24h") in await run(MarketMoves(), engine)  # no gap known: all windows
    await gap_ended(engine, ago(minutes=30))  # recording resumed 30 minutes ago
    got = await run(MarketMoves(), engine)
    assert (m, "change_15m") in got  # entirely after the resume
    assert (m, "change_1h") not in got and (m, "change_24h") not in got
    assert (m, "volume_1h") not in got and (m, "last") in got


async def test_moves_read_compressed_history_correctly(engine: AsyncEngine) -> None:
    m = await add_market(engine, "COLD")
    await ticks(engine, m, [
        {"ts": ago(hours=26), "price": 111_000, "volume": 10},
        {"ts": ago(hours=25), "price": 222_000, "volume": 20},
        {"ts": ago(minutes=1), "price": 333_000, "volume": 50},
    ])  # fmt: skip
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "select compress_chunk(c, true) from show_chunks('tickers', "
                "older_than => CAST(:t AS timestamptz)) c"
            ),
            {"t": ago(hours=12)},
        )
    got = await run(MarketMoves(), engine)
    assert got[(m, "change_24h")] == 333_000 - 222_000 and got[(m, "volume_24h")] == 300_000


# ------------------------------------------------------------------ volatility


async def test_a_price_that_alternates_by_a_cent_has_a_volatility_of_one_cent(
    engine: AsyncEngine,
) -> None:
    m = await add_market(engine, "VOL")
    await ticks(engine, m, [
        {"ts": ago(minutes=40 - i, seconds=5), "price": 500_000 + (i % 2) * 10_000}
        for i in range(31)
    ])  # fmt: skip
    got = await run(MarketVolatility(), engine)
    assert got[(m, "volatility_1h")] == 10_000  # exactly $0.01 per minute


async def test_several_ticks_in_a_minute_count_once_by_their_last_price(
    engine: AsyncEngine,
) -> None:
    m = await add_market(engine, "BURST")
    rows = []
    for i in range(30):
        base = ago(minutes=45 - i)
        rows.append({"ts": base + timedelta(seconds=5), "price": 999_000})  # overwritten
        rows.append({"ts": base + timedelta(seconds=50), "price": 500_000 + (i % 2) * 10_000})
    await ticks(engine, m, rows)
    assert (await run(MarketVolatility(), engine))[(m, "volatility_1h")] == 10_000


async def test_a_step_across_silent_minutes_is_scaled_as_a_random_walk(
    engine: AsyncEngine,
) -> None:
    m = await add_market(engine, "SCALE")
    rows = [{"ts": ago(minutes=50 - i), "price": 500_000 + (i % 2) * 10_000} for i in range(21)]
    # then a 4-minute silence ending in a $0.02 move: counts as 4 minutes of 0.02/sqrt(4) each
    rows.append({"ts": ago(minutes=26), "price": 520_000})
    await ticks(engine, m, rows)
    got = await run(MarketVolatility(), engine)
    # 20 steps of 10000 (1 minute each), the move from 500000 to 520000 over 6 minutes
    # (the last alternating tick is at minute 30 ago, price 500000): total d2 = 20*1e8 + 4e8
    expected = round(((20 * 10_000**2 + 20_000**2) / (20 + 4)) ** 0.5)
    assert got[(m, "volatility_1h")] == expected


async def test_too_few_active_minutes_is_not_reported_as_calm(engine: AsyncEngine) -> None:
    m = await add_market(engine, "THIN")
    await ticks(
        engine,
        m,
        [{"ts": ago(minutes=30 - i), "price": 500_000 + (i % 2) * 10_000} for i in range(15)],
    )
    assert await run(MarketVolatility(), engine) == {}


async def test_volatility_uses_only_the_time_since_recording_resumed(engine: AsyncEngine) -> None:
    m = await add_market(engine, "RESUME")
    await ticks(
        engine,
        m,
        [{"ts": ago(minutes=55 - i), "price": 500_000 + (i % 2) * 10_000} for i in range(50)],
    )
    assert (m, "volatility_1h") in await run(MarketVolatility(), engine)
    await gap_ended(engine, ago(minutes=12))  # only 12 minutes of trustworthy data remain
    assert await run(MarketVolatility(), engine) == {}


async def test_every_result_is_storable_and_integer_valued(engine: AsyncEngine) -> None:
    m = await add_market(engine, "ALL")
    await ticks(engine, m, [
        {"ts": ago(hours=26), "price": 1, "bid": 1, "ask": 3, "bid_size": 1, "volume": 1},
        *[{"ts": ago(minutes=40 - i), "price": 500_000 + (i % 2) * 10_000, "bid": 499_999,
           "ask": 510_001, "bid_size": 100, "ask_size": 200, "volume": 5 + i} for i in range(35)],
    ])  # fmt: skip
    for analyzer in (MarketQuotes(), MarketMoves(), MarketVolatility()):
        results: list[Result] = list(await analyzer.run(AnalysisContext(engine, NOW)))
        assert results
        for r in results:
            check_result(analyzer.name, r)
            assert isinstance(r.value_e6, int) and not isinstance(r.value_e6, bool)
