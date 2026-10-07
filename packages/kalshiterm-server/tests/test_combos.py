import asyncio
from decimal import Decimal
from typing import Any

import pytest
from kalshi_core.ws_models import WsMessage
from kalshiterm_server import db
from kalshiterm_server.ingest.combos import notional_e6
from kalshiterm_server.ingest.stream import StreamIngestor
from test_stream import (
    RECEIVE_DELAY,
    T_MS,
    Gate,
    rows,
    run,
    scalar,
    ticker_msg,
    trade_msg,
    until,
)

pytestmark = pytest.mark.db

FAMILY_A = "KXMVECROSSCATEGORY"
FAMILY_B = "KXMVECROSSCATEGORY0"
COMBO_A = f"{FAMILY_A}-S2026ABC-111"
COMBO_B = f"{FAMILY_B}-S2026XYZ-222"


def lifecycle(ticker: str, event_type: str, offset_ms: int = 0, **msg: Any) -> WsMessage:
    return WsMessage(
        type="multivariate_market_lifecycle",
        sid=5,
        msg={"market_ticker": ticker, "event_type": event_type, **msg},
        sending_ts_ms=T_MS + offset_ms,
        received_at=(T_MS + offset_ms) / 1000 + RECEIVE_DELAY,
    )


async def stats_rows(url: str) -> dict[str, list[Any]]:
    result = await rows(
        url,
        "select family, created, determined, settled, close_updated, ticker_msgs, trades, "
        "contracts_e2, notional_e6 from combo_stats_1m order by family",
    )
    return {family: list(rest) for family, *rest in result}


def test_notional_is_contracts_times_price_paid_rounded_to_a_millionth() -> None:
    assert notional_e6(Decimal("2.50"), Decimal("0.56")) == 1_400_000
    assert notional_e6(Decimal("1000"), Decimal("0.6")) == 600_000_000
    # price with 6 decimals x count with 2 decimals has 8: rounded, not an error
    assert notional_e6(Decimal("0.01"), Decimal("0.123456")) == 1235  # 1234.56 -> 1235


async def test_counters_cover_every_combo_message_with_exact_dollar_value(
    migrated_db_url: str,
) -> None:
    messages = [ticker_msg(COMBO_B, offset_ms=i) for i in range(5)]
    messages += [
        lifecycle(COMBO_B, "created", 10),
        lifecycle(COMBO_B, "created", 11),
        lifecycle(COMBO_B, "determined", 12, result="no", settlement_value="0.0000"),
        lifecycle(COMBO_B, "settled", 13, settled_ts=1_791_000_100),
        lifecycle(COMBO_B, "close_date_updated", 14, close_ts=1_791_000_200),
        lifecycle(COMBO_B, "activated", 15),  # not counted
        # yes taker pays the yes price; no taker pays the no price
        trade_msg(COMBO_A, offset_ms=20, count_fp="2.50", taker_side="yes"),
        trade_msg(COMBO_A, offset_ms=21, count_fp="1.00", taker_side="no"),
    ]
    ingestor = await run(migrated_db_url, messages)
    stats = await stats_rows(migrated_db_url)
    assert stats[FAMILY_B] == [2, 1, 1, 1, 5, 0, 0, 0]
    # 2.50 x $0.56 + 1.00 x $0.44 = $1.84 ; 3.50 contracts
    assert stats[FAMILY_A] == [0, 0, 0, 0, 0, 2, 350, 1_840_000]
    assert ingestor.combo_counted == {"ticker": 5, "lifecycle": 5, "trade": 2}
    assert await scalar(migrated_db_url, "select count(*) from combo_large_trades") == 0


async def test_only_trades_at_or_above_the_dollar_threshold_are_logged_individually(
    migrated_db_url: str,
) -> None:
    exactly = trade_msg(COMBO_A, offset_ms=1, count_fp="1000.00", yes_price_dollars="0.5000")
    just_under = trade_msg(COMBO_A, offset_ms=2, count_fp="999.98", yes_price_dollars="0.5000")
    big_no = trade_msg(
        COMBO_B, offset_ms=3, count_fp="2000.00", taker_side="no", no_price_dollars="0.3000",
        yes_price_dollars="0.7000",
    )  # fmt: skip
    small = trade_msg(COMBO_A, offset_ms=4, count_fp="3.00")
    ingestor = await run(migrated_db_url, [exactly, just_under, big_no, small])
    logged = await rows(
        migrated_db_url,
        "select ticker, trade_id::text, yes_price_e6, count_e2, taker_side, notional_e6 "
        "from combo_large_trades order by ts",
    )
    assert logged == [
        (COMBO_A, exactly.msg["trade_id"], 500_000, 100_000, "yes", 500_000_000),
        (COMBO_B, big_no.msg["trade_id"], 700_000, 200_000, "no", 600_000_000),
    ]
    assert ingestor.written["combo_large_trades"] == 2
    assert (
        await scalar(migrated_db_url, "select sum(trades) from combo_stats_1m") == 4
    )  # all counted
    view = await rows(
        migrated_db_url, "select notional, count from combo_large_trades_v order by ts"
    )
    assert [(str(n), str(c)) for n, c in view] == [("500.00", "1000.00"), ("600.00", "2000.00")]


async def test_the_threshold_is_configurable(migrated_db_url: str) -> None:
    messages = [trade_msg(COMBO_A, offset_ms=i, count_fp="10.00") for i in range(4)]  # $5.60 each
    await run(migrated_db_url, messages, large_trade_e6=5_000_000)
    assert await scalar(migrated_db_url, "select count(*) from combo_large_trades") == 4


async def test_counters_are_exact_after_failed_writes_and_retries(migrated_db_url: str) -> None:
    gate, failures = Gate(), [2]
    engine = db.make_engine(migrated_db_url)

    async def fast(_: float) -> None:
        await asyncio.sleep(0.01)

    try:
        ingestor = StreamIngestor(
            gate.__aiter__(), engine, flush_interval=0.05, sleep=fast, retry_base=0.01
        )
        real_write = ingestor._write

        async def flaky(batch: Any, agg: Any) -> None:
            if failures[0] > 0:
                failures[0] -= 1
                raise OSError("database unreachable")
            await real_write(batch, agg)

        ingestor._write = flaky  # type: ignore[method-assign]
        task = asyncio.create_task(ingestor.run())
        gate.put(*[ticker_msg(COMBO_A, offset_ms=i) for i in range(10)])
        await asyncio.sleep(0.02)  # arrives while the first write is failing
        gate.put(*[ticker_msg(COMBO_A, offset_ms=100 + i) for i in range(7)])
        await until(lambda: ingestor.retries >= 2 and ingestor.stats()["buffered"] == 0)
        gate.close()
        await asyncio.wait_for(task, 10)
    finally:
        await engine.dispose()
    assert await scalar(migrated_db_url, "select sum(ticker_msgs) from combo_stats_1m") == 17


async def test_batches_that_contain_only_counters_are_still_flushed(
    migrated_db_url: str,
) -> None:
    ingestor = await run(migrated_db_url, [lifecycle(COMBO_A, "created", i) for i in range(3)])
    assert ingestor.flushes >= 1
    assert (await stats_rows(migrated_db_url))[FAMILY_A][0] == 3
    assert await scalar(migrated_db_url, "select count(*) from combo_large_trades") == 0


async def test_a_malformed_combo_trade_is_still_counted_without_breaking_anything(
    migrated_db_url: str,
) -> None:
    odd = trade_msg(COMBO_A, count_fp="1.234")  # three decimals: no exact count representation
    ok = trade_msg(COMBO_A, offset_ms=1, count_fp="2.00")
    ingestor = await run(migrated_db_url, [odd, ok])
    stats = (await stats_rows(migrated_db_url))[FAMILY_A]
    assert stats[5] == 2  # both counted as trades
    assert stats[6] == 200  # only the representable contract count was added
    assert ingestor.rejected == 0


async def test_event_lifecycle_messages_and_unknown_types_are_ignored(
    migrated_db_url: str,
) -> None:
    ignored = [
        WsMessage(type="event_lifecycle", sid=5, msg={"event_ticker": "KXMVE-1", "title": "x"}),
        WsMessage(type="orderbook_delta", sid=1, seq=1, msg={}),
    ]
    ingestor = await run(migrated_db_url, ignored)
    assert (
        not ingestor.seen
        and await scalar(migrated_db_url, "select count(*) from combo_stats_1m") == 0
    )


async def test_migration_0005_drops_the_per_combo_tables_and_downgrade_restores_them(
    fresh_db_url: str,
) -> None:
    async def tables() -> set[str]:
        found = await rows(
            fresh_db_url,
            "select table_name from information_schema.tables where table_schema = 'public'",
        )
        return {name for (name,) in found}

    await db.upgrade_async(fresh_db_url, "0004")
    before = await tables()
    assert {"combo_markets", "combo_tickers", "combo_trades"} <= before
    await db.upgrade_async(fresh_db_url, "0005")
    after = await tables()
    assert not {"combo_markets", "combo_tickers", "combo_trades"} & after
    assert {"combo_large_trades", "combo_stats_1m"} <= after
    await db.downgrade_async(fresh_db_url, "0004")
    assert await tables() >= {"combo_markets", "combo_tickers", "combo_trades"}
