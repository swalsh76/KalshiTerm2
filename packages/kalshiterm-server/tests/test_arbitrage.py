"""The bid-side arbitrage analyzer on crafted events and a fixed clock."""

from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from kalshiterm_server import db
from kalshiterm_server.analytics.base import AnalysisContext, check_result
from kalshiterm_server.analytics.builtin.arbitrage import ArbitrageBid
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


def ago(**kw: float) -> datetime:
    return NOW - timedelta(**kw)


async def make_event(
    engine: AsyncEngine,
    event: str,
    legs: Sequence[tuple[int | None, int | None]],  # (bid_e6, bid_size_e2) per market
    *,
    fee_type: str = "quadratic",
    multiplier_e6: int = 1_000_000,
    exclusive: bool = True,
    age: timedelta = timedelta(seconds=30),
    status: str = "active",
) -> list[int]:
    ids = []
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "insert into series (ticker, title, frequency, category, fee_type, "
                "fee_multiplier_e6) values (:s, 't', 'daily', 'c', :ft, :fm) "
                "on conflict (ticker) do nothing"
            ),
            {"s": f"S-{fee_type}-{multiplier_e6}", "ft": fee_type, "fm": multiplier_e6},
        )
        await conn.execute(
            text(
                "insert into events (event_ticker, series_ticker, title, mutually_exclusive) "
                "values (:e, :s, 't', :x)"
            ),
            {"e": event, "s": f"S-{fee_type}-{multiplier_e6}", "x": exclusive},
        )
        for n, (bid, size) in enumerate(legs):
            market_id = (
                await conn.execute(
                    text(
                        "insert into markets (ticker, event_ticker, market_type, status) "
                        "values (:t, :e, 'binary', :st) returning id"
                    ),
                    {"t": f"{event}-{n}", "e": event, "st": status},
                )
            ).scalar_one()
            ids.append(market_id)
            if bid is not None:
                await conn.execute(
                    text(
                        "insert into tickers (ts, received_at, market_id, price_e6, yes_bid_e6, "
                        "yes_ask_e6, yes_bid_size_e2, yes_ask_size_e2) values "
                        "(:ts, :ts, :m, :b, :b, :a, :sz, 100)"
                    ),
                    {"ts": NOW - age, "m": market_id, "b": bid, "a": bid + 10_000, "sz": size},
                )
    return ids


async def run(engine: AsyncEngine, **settings: Any) -> dict[tuple[str, str], int]:
    analyzer = ArbitrageBid(**settings)
    results = await analyzer.run(AnalysisContext(engine, NOW))
    for r in results:
        check_result(analyzer.name, r)
        assert r.market_id is None and r.event_ticker  # the subject is the event
    return {(r.event_ticker or "", r.metric): r.value_e6 for r in results}


async def test_a_profitable_set_is_priced_exactly_including_fees(engine: AsyncEngine) -> None:
    # bids 40 + 35 + 30 = 105 cents; the smallest top-of-book size is 10 contracts.
    await make_event(engine, "E1", [(400_000, 1_000), (350_000, 2_000), (300_000, 1_500)])
    got = await run(engine)
    e = {metric: v for (ev, metric), v in got.items() if ev == "E1"}
    assert e["bid_sum"] == 1_050_000 and e["edge"] == 50_000
    assert e["size"] == 10_000_000  # 10 contracts, in millionths
    # taker fee per leg for 10 contracts, each rounded up to a cent:
    #   0.07 x 10 x 0.40 x 0.60 = 0.168 -> 0.17 ; x 0.35 x 0.65 = 0.15925 -> 0.16 ;
    #   x 0.30 x 0.70 = 0.147 -> 0.15      (the three sum to 0.48)
    assert e["fees"] == 480_000
    assert e["profit"] == 10 * 50_000 - 480_000 == 20_000  # $0.50 gross - $0.48 fees
    assert e["legs"] == 3_000_000 and e["legs_total"] == 3_000_000
    assert e["oldest_quote_age"] == 30_000_000


async def test_the_fee_rounding_unit_is_configurable(engine: AsyncEngine) -> None:
    await make_event(engine, "E1", [(400_000, 1_000), (350_000, 2_000), (300_000, 1_500)])
    exact = await run(engine, fee_round_e6=1)
    # unrounded to a millionth: 168000 + 159250 + 147000
    assert exact[("E1", "fees")] == 474_250 and exact[("E1", "profit")] == 25_750
    cents = await run(engine, fee_round_e6=10_000)
    assert cents[("E1", "fees")] == 480_000


async def test_a_thin_edge_is_reported_with_its_negative_profit(engine: AsyncEngine) -> None:
    # the common real-world case: a one-cent edge that the fees more than eat
    await make_event(engine, "THIN", [(510_000, 1_000), (500_000, 1_000)])
    got = await run(engine)
    assert got[("THIN", "edge")] == 10_000
    # fees: 0.07 x 10 x 0.51 x 0.49 = 0.17493 -> 0.18 ; 0.07 x 10 x 0.25 = 0.175 -> 0.18
    assert got[("THIN", "fees")] == 360_000
    assert got[("THIN", "profit")] == 10 * 10_000 - 360_000 == -260_000


async def test_no_edge_one_leg_or_a_threshold_means_no_result(engine: AsyncEngine) -> None:
    await make_event(engine, "FAIR", [(600_000, 100), (400_000, 100)])  # sums to exactly 1
    await make_event(engine, "UNDER", [(500_000, 100), (400_000, 100)])
    await make_event(engine, "ONE", [(990_000, 100), (None, None)])  # only one quoted leg
    await make_event(engine, "SMALL", [(510_000, 100), (500_000, 100)])
    assert {ev for ev, _ in await run(engine)} == {"SMALL"}
    assert await run(engine, min_edge_e6=10_000) == {}  # edge must exceed the threshold


async def test_legs_without_a_usable_bid_are_left_out_but_counted_in_the_total(
    engine: AsyncEngine,
) -> None:
    await make_event(engine, "PART", [(600_000, 500), (450_000, 300), (0, 100), (None, None)])
    got = await run(engine)
    assert got[("PART", "bid_sum")] == 1_050_000  # a zero bid and a market with no tick add nothing
    assert got[("PART", "legs")] == 2_000_000 and got[("PART", "legs_total")] == 4_000_000
    assert got[("PART", "size")] == 3_000_000  # the smaller of the two used top-of-book sizes


async def test_quotes_that_may_be_stale_are_not_used(engine: AsyncEngine) -> None:
    await make_event(engine, "OLD", [(600_000, 100), (450_000, 100)], age=timedelta(minutes=11))
    assert await run(engine) == {}  # older than the 10-minute limit
    assert ("OLD", "edge") in await run(engine, max_quote_age_seconds=900)
    async with engine.begin() as conn:  # recording resumed 5 minutes ago: older ticks prove nothing
        await conn.execute(
            text(
                "insert into ingest_gaps (started_at, ended_at, reason, status) "
                "values (:s, :e, 'startup', 'done')"
            ),
            {"s": ago(minutes=6), "e": ago(minutes=5)},
        )
    await make_event(engine, "RECENT", [(600_000, 100), (450_000, 100)], age=timedelta(minutes=1))
    got = await run(engine, max_quote_age_seconds=3600)
    assert ("RECENT", "edge") in got and ("OLD", "edge") not in got


async def test_only_active_markets_of_mutually_exclusive_events_count(
    engine: AsyncEngine,
) -> None:
    await make_event(engine, "NOTEXCL", [(600_000, 100), (450_000, 100)], exclusive=False)
    await make_event(engine, "SETTLED", [(600_000, 100), (450_000, 100)], status="finalized")
    assert await run(engine) == {}


async def test_fee_schedules_are_modelled_or_the_event_is_skipped(engine: AsyncEngine) -> None:
    legs = [(600_000, 1_000), (450_000, 1_000)]
    await make_event(engine, "PLAIN", legs)
    await make_event(engine, "MAKER", legs, fee_type="quadratic_with_maker_fees")
    await make_event(engine, "FLAT", legs, fee_type="flat")
    await make_event(engine, "FREE", legs, multiplier_e6=0)
    await make_event(engine, "HALF", legs, multiplier_e6=500_000)
    got = await run(engine, fee_round_e6=1)
    assert not any(ev == "FLAT" for ev, _ in got)  # a fee table we do not model: no guess
    assert got[("PLAIN", "fees")] == got[("MAKER", "fees")]  # taker orders pay the same fee
    assert got[("FREE", "fees")] == 0
    assert got[("HALF", "fees")] == got[("PLAIN", "fees")] // 2
    assert got[("FREE", "profit")] == 10 * 50_000


async def test_the_environment_sets_the_defaults(
    engine: AsyncEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("KTERM_ARBITRAGE_FEE_ROUND_E6", "1")
    monkeypatch.setenv("KTERM_ARBITRAGE_MAX_QUOTE_AGE_SECONDS", "5")
    analyzer = ArbitrageBid()
    assert analyzer.fee_round_e6 == 1 and analyzer.max_quote_age == timedelta(seconds=5)
    assert ArbitrageBid(fee_round_e6=77).fee_round_e6 == 77  # arguments win
