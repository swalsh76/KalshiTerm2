import asyncio
import time
import uuid
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from typing import Any

import pytest
from kalshi_core.ws import RECONNECTED
from kalshi_core.ws_models import WsMessage
from kalshiterm_server import db
from kalshiterm_server.ingest.stream import StreamIngestor
from kalshiterm_server.storage import reference, tables
from sqlalchemy import text

pytestmark = pytest.mark.db

T_MS = 1_791_000_000_000  # an arbitrary exchange time
RECEIVE_DELAY = 0.25  # seconds between "exchange time" and "received"


def ticker_msg(ticker: str = "KXA-E1-X", offset_ms: int = 0, **fields: Any) -> WsMessage:
    msg = {
        "market_ticker": ticker,
        "price_dollars": "0.5600",
        "yes_bid_dollars": "0.5500",
        "yes_ask_dollars": "0.5700",
        "yes_bid_size_fp": "120.00",
        "yes_ask_size_fp": "75.00",
        "volume_fp": "18234.00",
        "open_interest_fp": "9120.00",
        "last_trade_size_fp": "3.00",
        "ts_ms": T_MS + offset_ms,
        **fields,
    }
    received = (T_MS + offset_ms) / 1000 + RECEIVE_DELAY
    return WsMessage(type="ticker", sid=1, msg=msg, received_at=received)


def trade_msg(ticker: str = "KXA-E1-X", offset_ms: int = 0, **fields: Any) -> WsMessage:
    msg = {
        "trade_id": str(uuid.uuid4()),
        "market_ticker": ticker,
        "yes_price_dollars": "0.5600",
        "no_price_dollars": "0.4400",
        "count_fp": "3.00",
        "taker_side": "yes",
        "is_block_trade": False,
        "ts_ms": T_MS + offset_ms,
        **fields,
    }
    received = (T_MS + offset_ms) / 1000 + RECEIVE_DELAY
    return WsMessage(type="trade", sid=2, msg=msg, received_at=received)


def lifecycle_msg(ticker: str = "KXA-E1-X", offset_ms: int = 0) -> WsMessage:
    msg = {
        "market_ticker": ticker,
        "event_type": "determined",
        "determination_ts": 1_791_000_100,
        "result": "yes",
        "settlement_value": "1.0000",
    }
    return WsMessage(
        type="market_lifecycle_v2",
        sid=3,
        msg=msg,
        sending_ts_ms=T_MS + offset_ms,
        received_at=(T_MS + offset_ms) / 1000 + RECEIVE_DELAY,
    )


async def stream(messages: list[WsMessage]) -> AsyncIterator[WsMessage]:
    for message in messages:
        yield message


class Gate:
    """A message source the test can feed and pause, to observe the ingestor mid-run."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[WsMessage | None] = asyncio.Queue()
        self.pulled = 0

    def put(self, *messages: WsMessage) -> None:
        for message in messages:
            self._queue.put_nowait(message)

    def close(self) -> None:
        self._queue.put_nowait(None)

    async def __aiter__(self) -> AsyncIterator[WsMessage]:
        while (message := await self._queue.get()) is not None:
            self.pulled += 1
            yield message


async def scalar(url: str, sql: str) -> Any:
    engine = db.make_engine(url)
    try:
        async with engine.connect() as conn:
            return (await conn.execute(text(sql))).scalar_one()
    finally:
        await engine.dispose()


async def rows(url: str, sql: str) -> list[tuple[Any, ...]]:
    engine = db.make_engine(url)
    try:
        async with engine.connect() as conn:
            return [tuple(r) for r in await conn.execute(text(sql))]
    finally:
        await engine.dispose()


async def until(predicate: Callable[[], bool], seconds: float = 10.0) -> None:
    async with asyncio.timeout(seconds):
        while not predicate():  # noqa: ASYNC110 - polling a plain predicate
            await asyncio.sleep(0.01)


async def run(url: str, messages: list[WsMessage], **kwargs: Any) -> StreamIngestor:
    engine = db.make_engine(url)
    try:
        ingestor = StreamIngestor(stream(messages), engine, **kwargs)
        async with asyncio.timeout(30):
            await ingestor.run()
        return ingestor
    finally:
        await engine.dispose()


async def test_rows_land_with_exact_fixed_point_values_and_both_timestamps(
    migrated_db_url: str,
) -> None:
    trade = trade_msg(offset_ms=500)
    ingestor = await run(migrated_db_url, [ticker_msg(), trade, lifecycle_msg(offset_ms=900)])
    assert ingestor.written == {"tickers": 1, "trades": 1, "market_lifecycle": 1}

    [t] = await rows(
        migrated_db_url,
        "select ts, received_at, price_e6, yes_bid_e6, yes_ask_e6, yes_bid_size_e2, "
        "yes_ask_size_e2, volume_e2, open_interest_e2, last_trade_size_e2 from tickers",
    )
    assert t[0] == datetime.fromtimestamp(T_MS / 1000, UTC)
    assert (t[1] - t[0]).total_seconds() == pytest.approx(RECEIVE_DELAY)
    assert t[2:] == (560_000, 550_000, 570_000, 12_000, 7_500, 1_823_400, 912_000, 300)

    [tr] = await rows(
        migrated_db_url,
        "select ts, trade_id::text, yes_price_e6, count_e2, taker_side, is_block_trade from trades",
    )
    assert tr[0] == datetime.fromtimestamp((T_MS + 500) / 1000, UTC)
    assert tr[1:] == (trade.msg["trade_id"], 560_000, 300, "yes", False)

    [lc] = await rows(
        migrated_db_url,
        "select ts, event_type, determination_ts, result, settlement_value_e6 "
        "from market_lifecycle",
    )
    assert lc[0] == datetime.fromtimestamp((T_MS + 900) / 1000, UTC)  # sending_ts_ms
    assert lc[1] == "determined" and lc[3] == "yes" and lc[4] == 1_000_000
    assert lc[2] == datetime.fromtimestamp(1_791_000_100, UTC)


async def test_known_markets_keep_their_id_and_unknown_ones_get_placeholders(
    migrated_db_url: str,
) -> None:
    engine = db.make_engine(migrated_db_url)
    known = {
        "ticker": "KXA-E1-X",
        "event_ticker": "KXA-E1",
        "market_type": "binary",
        "status": "active",
    }
    try:
        await reference.upsert(engine, tables.markets, [known], "ticker")
    finally:
        await engine.dispose()
    original_id = await scalar(migrated_db_url, "select id from markets where ticker = 'KXA-E1-X'")

    ingestor = await run(migrated_db_url, [ticker_msg("KXA-E1-X"), ticker_msg("KXNEW-26OCT07-T1")])
    assert ingestor.stats()["placeholders_created"] == 1
    placeholder = (
        await rows(
            migrated_db_url,
            "select event_ticker, status from markets where ticker = 'KXNEW-26OCT07-T1'",
        )
    )[0]
    assert placeholder == ("KXNEW-26OCT07", "unknown")
    assert (
        await scalar(migrated_db_url, "select status from markets where ticker = 'KXA-E1-X'")
        == "active"
    )
    stored = await rows(migrated_db_url, "select market_id from tickers order by market_id")
    assert len(stored) == 2 and (original_id,) in stored

    # Discovery later fills the placeholder in without changing its id.
    placeholder_id = await scalar(
        migrated_db_url, "select id from markets where ticker = 'KXNEW-26OCT07-T1'"
    )
    engine = db.make_engine(migrated_db_url)
    try:
        result = await reference.upsert(
            engine,
            tables.markets,
            [
                {
                    **known,
                    "ticker": "KXNEW-26OCT07-T1",
                    "event_ticker": "KXNEW-26OCT07",
                    "status": "active",
                }
            ],
            "ticker",
        )
    finally:
        await engine.dispose()
    assert result.updated == 1
    assert (
        await scalar(migrated_db_url, "select id from markets where ticker = 'KXNEW-26OCT07-T1'")
        == placeholder_id
    )


async def test_combo_market_messages_are_skipped_and_counted(migrated_db_url: str) -> None:
    ingestor = await run(
        migrated_db_url,
        [
            ticker_msg("KXMVECROSSCATEGORY-S2026ABC-123"),
            trade_msg("KXMVECROSSCATEGORY-S2026ABC-123"),
            ticker_msg("KXA-E1-X"),
        ],
    )
    assert ingestor.skipped_mve == 2
    assert ingestor.written == {"tickers": 1, "trades": 0, "market_lifecycle": 0}
    assert (
        await scalar(migrated_db_url, "select count(*) from markets where ticker like 'KXMVE%'")
        == 0
    )


async def test_unrepresentable_or_malformed_rows_are_rejected_without_blocking_the_rest(
    migrated_db_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    ingestor = await run(
        migrated_db_url,
        [
            ticker_msg(price_dollars="0.1234567"),  # finer than 1e-6
            trade_msg(trade_id="not-a-uuid"),
            WsMessage(type="ticker", sid=1, msg={"price_dollars": "0.5"}),  # no ticker at all
            ticker_msg(offset_ms=1),
            trade_msg(offset_ms=1),
        ],
    )
    assert ingestor.rejected == 3
    assert ingestor.written == {"tickers": 1, "trades": 1, "market_lifecycle": 0}
    assert "rejected" in caplog.text


async def test_flushes_when_the_batch_is_full_and_also_on_the_interval(
    migrated_db_url: str,
) -> None:
    gate = Gate()
    engine = db.make_engine(migrated_db_url)
    try:
        ingestor = StreamIngestor(gate.__aiter__(), engine, batch_size=10, flush_interval=0.15)
        task = asyncio.create_task(ingestor.run())
        gate.put(*[ticker_msg(offset_ms=i) for i in range(10)])  # exactly one full batch
        await until(lambda: ingestor.written["tickers"] == 10)
        assert ingestor.flushes == 1
        gate.put(*[ticker_msg(offset_ms=100 + i) for i in range(3)])  # too few for a batch...
        await until(lambda: ingestor.written["tickers"] == 13)  # ...but the interval flushes them
        gate.close()
        await asyncio.wait_for(task, 10)
    finally:
        await engine.dispose()
    assert await scalar(migrated_db_url, "select count(*) from tickers") == 13


async def test_database_errors_are_retried_without_loss_duplication_or_reordering(
    migrated_db_url: str,
) -> None:
    engine = db.make_engine(migrated_db_url)
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        await asyncio.sleep(0)

    gate = Gate()
    try:
        ingestor = StreamIngestor(
            gate.__aiter__(),
            engine,
            batch_size=10,
            flush_interval=0.05,
            retry_base=0.5,
            sleep=fake_sleep,
        )
        real_write, failures = ingestor._write, [3]

        async def flaky(batch: Any) -> None:
            if failures[0] > 0:
                failures[0] -= 1
                raise OSError("database unreachable")
            await real_write(batch)

        ingestor._write = flaky  # type: ignore[method-assign]
        task = asyncio.create_task(ingestor.run())
        gate.put(*[ticker_msg(offset_ms=i) for i in range(30)])
        await until(lambda: ingestor.written["tickers"] == 30)
        gate.close()
        await asyncio.wait_for(task, 10)
    finally:
        await engine.dispose()
    stored = await rows(migrated_db_url, "select ts from tickers order by ts")
    expected = [datetime.fromtimestamp((T_MS + i) / 1000, UTC) for i in range(30)]
    assert [r[0] for r in stored] == expected  # nothing lost, duplicated or reordered
    assert ingestor.retries == 3
    assert sleeps[:3] == [0.5, 1.0, 2.0]  # exponential backoff


async def test_shutdown_gives_up_after_a_few_attempts_instead_of_hanging(
    migrated_db_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    async def instant(_: float) -> None:
        await asyncio.sleep(0)

    engine = db.make_engine(migrated_db_url)
    try:
        ingestor = StreamIngestor(
            stream([ticker_msg(offset_ms=i) for i in range(30)]), engine, sleep=instant
        )

        async def always_down(batch: Any) -> None:
            raise OSError("database unreachable")

        ingestor._write = always_down  # type: ignore[method-assign]
        async with asyncio.timeout(10):
            await ingestor.run()  # the source ends at once; the drain must not retry forever
    finally:
        await engine.dispose()
    assert ingestor.retries == 3 and ingestor.written["tickers"] == 0
    assert "dropping 30 unwritten items at shutdown" in caplog.text


async def test_a_stalled_database_pauses_intake_instead_of_buffering_without_limit(
    migrated_db_url: str,
) -> None:
    gate, database_up = Gate(), [False]
    engine = db.make_engine(migrated_db_url)

    async def fast_sleep(_: float) -> None:
        await asyncio.sleep(0.01)

    try:
        ingestor = StreamIngestor(
            gate.__aiter__(),
            engine,
            batch_size=10,
            max_buffered=50,
            flush_interval=0.05,
            sleep=fast_sleep,
        )
        real_write = ingestor._write

        async def maybe_down(batch: Any) -> None:
            if not database_up[0]:
                raise OSError("database unreachable")
            await real_write(batch)

        ingestor._write = maybe_down  # type: ignore[method-assign]
        task = asyncio.create_task(ingestor.run())
        gate.put(*[ticker_msg(offset_ms=i) for i in range(500)])
        await asyncio.sleep(0.5)
        assert gate.pulled <= 51  # buffer limit plus the one message waiting at the door
        assert ingestor.stats()["buffered"] <= 50
        database_up[0] = True  # the database comes back
        await until(lambda: gate.pulled == 500 and ingestor.written["tickers"] == 500)
        gate.close()
        await asyncio.wait_for(task, 10)
    finally:
        await engine.dispose()
    assert await scalar(migrated_db_url, "select count(*) from tickers") == 500


async def test_a_reconnect_event_is_counted_and_gets_earlier_data_onto_disk(
    migrated_db_url: str,
) -> None:
    gate = Gate()
    engine = db.make_engine(migrated_db_url)
    try:
        ingestor = StreamIngestor(gate.__aiter__(), engine, batch_size=1000, flush_interval=60)
        task = asyncio.create_task(ingestor.run())
        gate.put(*[ticker_msg(offset_ms=i) for i in range(5)])
        gate.put(WsMessage(type=RECONNECTED, msg={"reason": "connection_lost"}))
        await until(lambda: ingestor.written["tickers"] == 5)  # flushed by the event, not the timer
        assert ingestor.reconnects == 1
        gate.close()
        await asyncio.wait_for(task, 10)
    finally:
        await engine.dispose()


async def test_lag_statistics_separate_network_delay_from_our_own_buffering(
    migrated_db_url: str,
) -> None:
    written_at = T_MS / 1000 + RECEIVE_DELAY + 1.0  # one second after receipt
    ingestor = await run(
        migrated_db_url, [ticker_msg(offset_ms=i) for i in range(20)], clock=lambda: written_at
    )
    stats = ingestor.stats()
    assert stats["lag_ms"]["p50"] == pytest.approx(RECEIVE_DELAY * 1000, abs=25)
    assert stats["write_delay_ms"]["p50"] == pytest.approx(1000 - 19 / 2, abs=25)


async def test_throughput_is_far_above_the_measured_live_peak(migrated_db_url: str) -> None:
    """Live peak was ~3,300 msgs/s (mean ~1,550). Demand at least 5,000/s sustained."""
    tickers = [f"KXLOAD-26OCT07-M{i}" for i in range(300)]
    messages: list[WsMessage] = []
    for i in range(30_000):
        name = tickers[i % 300]
        if i % 10 < 6:
            messages.append(ticker_msg(name, offset_ms=i))
        elif i % 10 < 9:
            messages.append(trade_msg(name, offset_ms=i))
        else:
            messages.append(lifecycle_msg(name, offset_ms=i))
    started = time.monotonic()
    ingestor = await run(migrated_db_url, messages)
    rate = len(messages) / (time.monotonic() - started)
    print(f"\n  ingested {len(messages):,} messages at {rate:,.0f}/s; {ingestor.flushes} flushes")
    assert sum(ingestor.written.values()) == 30_000
    assert rate >= 5_000
