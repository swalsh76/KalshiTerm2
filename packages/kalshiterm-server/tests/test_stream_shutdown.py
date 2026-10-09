"""A stop request arriving mid-write must neither lose nor duplicate the batch.

Found live (2026-10-09): every graceful stop wrote the last second of every table twice,
because a cancelled write was assumed not to have committed when the database often had.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from kalshiterm_server import db
from kalshiterm_server.ingest.stream import StreamIngestor
from test_stream import Gate, scalar, trade_msg, until

pytestmark = pytest.mark.db

Write = Callable[[Any, Any], Awaitable[None]]


async def stop_during_a_write(
    url: str, wrap: Callable[[Write, asyncio.Event], Write], **kwargs: Any
) -> StreamIngestor:
    """Feed three trades, stop the ingestor while the (wrapped) write is in progress."""
    engine = db.make_engine(url)
    try:
        gate = Gate()
        ingestor = StreamIngestor(gate.__aiter__(), engine, flush_interval=0.05, **kwargs)
        writing = asyncio.Event()
        ingestor._write = wrap(ingestor._write, writing)  # type: ignore[method-assign,assignment]  # noqa: SLF001
        task = asyncio.create_task(ingestor.run())
        async with asyncio.timeout(30):
            gate.put(trade_msg(offset_ms=0), trade_msg(offset_ms=1), trade_msg(offset_ms=2))
            await writing.wait()
            task.cancel()  # what `docker stop` and --seconds both do
            await asyncio.gather(task, return_exceptions=True)
        return ingestor
    finally:
        await engine.dispose()


async def rows_and_distinct(url: str) -> tuple[int, int]:
    return (
        await scalar(url, "select count(*) from trades"),
        await scalar(url, "select count(distinct trade_id) from trades"),
    )


async def test_a_stop_after_the_database_committed_does_not_write_the_batch_again(
    migrated_db_url: str,
) -> None:
    def wrap(real: Write, writing: asyncio.Event) -> Write:
        async def commit_then_linger(batch: Any, agg: Any) -> None:
            await real(batch, agg)  # the transaction commits...
            writing.set()
            await asyncio.sleep(0.5)  # ...and the stop request lands before the call returns

        return commit_then_linger

    await stop_during_a_write(migrated_db_url, wrap)
    assert await rows_and_distinct(migrated_db_url) == (3, 3)  # once each, not 6


async def test_a_stop_before_the_commit_still_writes_the_batch_exactly_once(
    migrated_db_url: str,
) -> None:
    def wrap(real: Write, writing: asyncio.Event) -> Write:
        async def delay_then_commit(batch: Any, agg: Any) -> None:
            writing.set()
            await asyncio.sleep(0.3)  # the stop request lands first
            await real(batch, agg)

        return delay_then_commit

    await stop_during_a_write(migrated_db_url, wrap)
    assert await rows_and_distinct(migrated_db_url) == (3, 3)  # neither lost nor repeated


async def test_a_write_that_failed_during_the_stop_is_kept_and_written_by_the_drain(
    migrated_db_url: str,
) -> None:
    calls = {"n": 0}

    def wrap(real: Write, writing: asyncio.Event) -> Write:
        async def fail_first(batch: Any, agg: Any) -> None:
            calls["n"] += 1
            if calls["n"] == 1:
                writing.set()
                await asyncio.sleep(0.2)
                raise ConnectionError("database went away")
            await real(batch, agg)

        return fail_first

    ingestor = await stop_during_a_write(migrated_db_url, wrap)
    assert await rows_and_distinct(migrated_db_url) == (3, 3)  # the failed batch was not lost
    assert calls["n"] == 2 and ingestor.stats()["buffered"] == 0


async def test_a_write_that_never_finishes_does_not_hang_the_shutdown_or_get_retried(
    migrated_db_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR)

    def wrap(real: Write, writing: asyncio.Event) -> Write:
        async def hang(batch: Any, agg: Any) -> None:
            writing.set()
            await asyncio.sleep(3600)

        return hang

    started = asyncio.get_running_loop().time()
    ingestor = await stop_during_a_write(migrated_db_url, wrap, settle_timeout=0.3)
    assert asyncio.get_running_loop().time() - started < 10  # bounded, not an hour
    assert "did not finish" in caplog.text and "not retrying" in caplog.text
    assert await rows_and_distinct(migrated_db_url) == (0, 0)
    assert ingestor.stats()["buffered"] == 0  # not left to be written twice


async def test_an_ordinary_run_and_a_clean_finish_write_each_row_once(
    migrated_db_url: str,
) -> None:
    engine = db.make_engine(migrated_db_url)
    gate = Gate()
    ingestor = StreamIngestor(gate.__aiter__(), engine, flush_interval=0.05)
    task = asyncio.create_task(ingestor.run())
    async with asyncio.timeout(30):
        gate.put(*[trade_msg(offset_ms=i) for i in range(50)])
        await until(lambda: ingestor.written["trades"] == 50)
        gate.close()
        await task
    await engine.dispose()
    assert await rows_and_distinct(migrated_db_url) == (50, 50)
