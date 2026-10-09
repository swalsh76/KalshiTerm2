"""The write-order cursor (migration 0016): what a push client can rely on."""

import asyncio
from typing import Any

import asyncpg  # type: ignore[import-untyped]
import pytest
from kalshiterm_server import db
from kalshiterm_server.ingest import stream as stream_module
from kalshiterm_server.ingest.stream import StreamIngestor
from sqlalchemy import text
from test_orderbook_storage import delta_msg, events_for, snap_msg
from test_stream import Gate, rows, scalar, ticker_msg, trade_msg, until

pytestmark = pytest.mark.db

TABLES = ("trades", "tickers", "orderbook_snapshots", "orderbook_deltas")


def book_events(count: int) -> list[Any]:
    from kalshi_core.orderbook import BookTracker

    tracker = BookTracker()
    tracker.begin_subscription(1, ["KXA-E1-X"])
    messages = [snap_msg(1, 1, "KXA-E1-X", [("0.40", "10.00")], [])]
    messages += [
        delta_msg(1, 2 + i, "KXA-E1-X", "yes", "0.40", "1.00", offset_ms=i) for i in range(count)
    ]
    return events_for(tracker, messages)


async def feed_everything(url: str, **kwargs: Any) -> StreamIngestor:
    """Trades, tickers and orderbook events through one ingestor, in several batches."""
    engine = db.make_engine(url)
    gate = Gate()
    ingestor = StreamIngestor(gate.__aiter__(), engine, flush_interval=0.02, **kwargs)
    task = asyncio.create_task(ingestor.run())
    async with asyncio.timeout(60):
        for round_ in range(6):
            for i in range(20):
                gate.put(
                    trade_msg(offset_ms=round_ * 1000 + i), ticker_msg(offset_ms=round_ * 1000 + i)
                )
            await asyncio.sleep(0.05)
        gate.close()
        await task
    await engine.dispose()
    return ingestor


async def test_every_streamed_row_gets_a_unique_increasing_sequence_number(
    migrated_db_url: str,
) -> None:
    await feed_everything(migrated_db_url)
    for table in ("trades", "tickers"):
        found = await rows(
            migrated_db_url, f"select ingest_seq from {table} order by tableoid, ctid"
        )
        seqs = [r[0] for r in found]
        assert len(seqs) == 120 and None not in seqs
        assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)  # in write order, no repeats
    both = await scalar(
        migrated_db_url,
        "select count(*) - count(distinct ingest_seq) from "
        "(select ingest_seq from trades union all select ingest_seq from tickers) u",
    )
    assert both == 0  # one sequence shared by the tables: a single total order


async def test_the_watermark_is_the_highest_sequence_number_after_every_batch(
    migrated_db_url: str,
) -> None:
    await feed_everything(migrated_db_url)
    highest = await scalar(
        migrated_db_url,
        "select max(s) from (select max(ingest_seq) s from trades union all "
        "select max(ingest_seq) from tickers) u",
    )
    assert await scalar(migrated_db_url, "select last_seq from ingest_progress") == highest > 0


async def test_a_reader_never_sees_a_row_beyond_the_watermark_while_batches_commit(
    migrated_db_url: str,
) -> None:
    """The invariant push relies on, checked by a reader running flat out during real writes."""
    engine = db.make_engine(migrated_db_url)
    violations: list[tuple[int, int]] = []
    done = asyncio.Event()

    async def reader() -> int:
        looks = 0
        while not done.is_set():
            async with engine.connect() as conn:
                # one statement = one snapshot, so the pair is consistent
                mark, top = (
                    await conn.execute(
                        text(
                            "select p.last_seq, coalesce(greatest("
                            "(select max(ingest_seq) from trades), "
                            "(select max(ingest_seq) from tickers)), 0) from ingest_progress p"
                        )
                    )
                ).one()
            looks += 1
            if top > mark:
                violations.append((mark, top))
        return looks

    watcher = asyncio.create_task(reader())
    await feed_everything(migrated_db_url)
    done.set()
    looks = await watcher
    await engine.dispose()
    assert looks > 20  # it really was watching while the batches landed
    assert violations == []


async def test_a_batch_that_fails_to_commit_leaves_neither_rows_nor_a_moved_watermark(
    migrated_db_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = stream_module.PUBLISH
    calls = {"n": 0}

    async def instant(_: float) -> None:
        return

    engine = db.make_engine(migrated_db_url)
    gate = Gate()
    ingestor = StreamIngestor(gate.__aiter__(), engine, flush_interval=0.02, sleep=instant)
    original = ingestor._write  # noqa: SLF001

    async def fail_once_after_the_rows_are_in(batch: Any, agg: Any) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            monkeypatch.setattr(stream_module, "PUBLISH", "SELECT 1/0")  # dies inside the txn
        else:
            monkeypatch.setattr(stream_module, "PUBLISH", real)
        await original(batch, agg)

    ingestor._write = fail_once_after_the_rows_are_in  # type: ignore[method-assign]
    task = asyncio.create_task(ingestor.run())
    async with asyncio.timeout(30):
        gate.put(*[trade_msg(offset_ms=i) for i in range(10)])
        await until(lambda: ingestor.written["trades"] == 10)
        gate.close()
        await task
    await engine.dispose()
    assert calls["n"] >= 2  # the first attempt failed and was retried
    assert await scalar(migrated_db_url, "select count(*) from trades") == 10  # once, not twice
    top = await scalar(migrated_db_url, "select max(ingest_seq) from trades")
    assert await scalar(migrated_db_url, "select last_seq from ingest_progress") == top
    # the failed attempt consumed sequence numbers, which is harmless: a cursor tolerates gaps
    assert top > 10


async def test_listeners_are_told_the_new_watermark_at_commit(migrated_db_url: str) -> None:
    dsn = migrated_db_url.replace("postgresql+asyncpg://", "postgresql://")
    listener = await asyncpg.connect(dsn)
    heard: asyncio.Queue[str] = asyncio.Queue()
    await listener.add_listener(
        "kterm_ingest", lambda _c, _p, _ch, payload: heard.put_nowait(payload)
    )
    try:
        await feed_everything(migrated_db_url)
        async with asyncio.timeout(10):
            payloads = [await heard.get()]
            while not heard.empty():
                payloads.append(heard.get_nowait())
    finally:
        await listener.close()
    final = str(await scalar(migrated_db_url, "select last_seq from ingest_progress"))
    assert payloads[-1] == final and len(payloads) >= 2  # one per committed batch
    numbers = [int(p) for p in payloads]
    assert numbers == sorted(numbers)  # never goes backwards


async def test_orderbook_rows_share_the_same_sequence(migrated_db_url: str) -> None:
    engine = db.make_engine(migrated_db_url)
    events = book_events(30)

    async def source() -> Any:
        for event in events:
            yield event

    ingestor = StreamIngestor(source(), engine)
    await ingestor.run()
    await engine.dispose()
    snaps = [
        r[0] for r in await rows(migrated_db_url, "select ingest_seq from orderbook_snapshots")
    ]
    deltas = [
        r[0]
        for r in await rows(migrated_db_url, "select ingest_seq from orderbook_deltas order by seq")
    ]
    assert len(snaps) == 1 and len(deltas) == 30
    assert deltas == sorted(deltas) and snaps[0] < deltas[0]  # the snapshot came first


async def test_cursor_order_is_arrival_order_across_tables_not_table_write_order(
    migrated_db_url: str,
) -> None:
    """Found live: tables are COPYed one after another within a batch, so a column default
    numbered a snapshot BEFORE the deltas that had arrived ahead of it."""
    from kalshi_core.orderbook import BookTracker

    tracker = BookTracker()
    tracker.begin_subscription(1, ["KXA-E1-X"])
    arrival = events_for(
        tracker,
        [
            snap_msg(1, 1, "KXA-E1-X", [("0.40", "10.00")], []),
            delta_msg(1, 2, "KXA-E1-X", "yes", "0.40", "1.00", offset_ms=10),
            delta_msg(1, 3, "KXA-E1-X", "yes", "0.40", "1.00", offset_ms=20),
            snap_msg(1, 4, "KXA-E1-X", [("0.40", "12.00")], [], offset_ms=30),  # a later snapshot
            delta_msg(1, 5, "KXA-E1-X", "yes", "0.40", "1.00", offset_ms=40),
        ],
    )
    messages = [
        trade_msg(offset_ms=1),
        ticker_msg(offset_ms=2),
        trade_msg(offset_ms=3),
    ]
    mixed: list[Any] = [messages[0], arrival[0], arrival[1], messages[1], arrival[2], arrival[3]]
    mixed += [messages[2], arrival[4]]
    engine = db.make_engine(migrated_db_url)

    async def source() -> Any:
        for item in mixed:
            yield item

    ingestor = StreamIngestor(source(), engine, flush_interval=60, batch_size=1000)
    await ingestor.run()  # everything arrives before the first flush: one batch
    await engine.dispose()
    assert ingestor.flushes == 1
    rows_ = await rows(
        migrated_db_url,
        "select 'trade' k, ingest_seq from trades "
        "union all select 'ticker', ingest_seq from tickers "
        "union all select 'snapshot' || seq, ingest_seq from orderbook_snapshots "
        "union all select 'delta' || seq, ingest_seq from orderbook_deltas order by 2",
    )
    assert [r[0] for r in rows_] == [
        "trade", "snapshot1", "delta2", "ticker", "delta3", "snapshot4", "trade", "delta5",
    ]  # fmt: skip
    numbers = [r[1] for r in rows_]
    assert numbers == list(range(numbers[0], numbers[0] + len(numbers)))  # one gap-free block
    assert await scalar(migrated_db_url, "select last_seq from ingest_progress") == numbers[-1]
