"""Gap log and trade backfill.

Kalshi does not replay what a dropped connection missed. Every outage (a reconnect, a
receive-queue overflow, or a restart of this process) becomes a row in ``ingest_gaps``, and the
trades of that window are fetched from REST and fed through the ingestor's normal write path.

Trades are the only stream that can be recovered: tickers, lifecycle events and orderbook
deltas cannot be replayed, so the gap row is the permanent record that they are missing for
that window. Combo (``KXMVE``) trades are backfilled only into the large-trade log, because
their per-minute counters cannot be de-duplicated against what the live stream already counted.

The fetch window is padded on both sides (clock skew, ordering at the edges) and every trade is
checked against the database and the ingestor's recent-id memory, so overlap never duplicates.
Re-running a window is therefore always safe, which is what makes retry simple.
"""

import asyncio
import logging
import math
from collections import Counter
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

from kalshi_core.rest import KalshiRestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from kalshiterm_server.ingest.stream import GapInfo, StreamIngestor

log = logging.getLogger("kalshiterm_server")

MARGIN = timedelta(seconds=10)
MAX_WINDOW = timedelta(hours=6)

KNOWN_IDS = """
SELECT trade_id::text FROM trades WHERE ts BETWEEN :a AND :b
UNION
SELECT trade_id::text FROM combo_large_trades WHERE ts BETWEEN :a AND :b
"""


class GapBackfiller:
    def __init__(
        self,
        rest: KalshiRestClient,
        engine: AsyncEngine,
        ingestor: StreamIngestor,
        *,
        max_window: timedelta = MAX_WINDOW,
        margin: timedelta = MARGIN,
        attempts: int = 3,
        retry_base: float = 2.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._rest = rest
        self._engine = engine
        self._ingestor = ingestor
        self._max_window = max_window
        self._margin = margin
        self._attempts = attempts
        self._retry_base = retry_base
        self._sleep = sleep
        self._clock = clock
        self._queue: asyncio.Queue[GapInfo] = asyncio.Queue()
        self.processed = 0
        ingestor.on_gap = self.report

    def report(self, gap: GapInfo) -> None:
        """Called by the ingestor (synchronously) when a connection comes back."""
        self._queue.put_nowait(gap)

    async def start(self) -> None:
        """Mark gaps a previous run never finished, and queue the gap since it stopped."""
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "UPDATE ingest_gaps SET status = 'interrupted', finished_at = now() "
                    "WHERE status IN ('pending', 'running')"
                )
            )
            now = self._clock()
            newest = None
            if (await conn.execute(text("SELECT EXISTS (SELECT 1 FROM trades)"))).scalar_one():
                # Bounded to the window we would backfill anyway: a hypertable has no cheap
                # global max(ts), and anything older is clamped regardless.
                newest = (
                    await conn.execute(
                        text("SELECT max(ts) FROM trades WHERE ts >= :since"),
                        {"since": now - self._max_window},
                    )
                ).scalar_one()
                newest = newest or now - self._max_window
        if newest is not None:
            self.report(GapInfo(started=newest, ended=now, reason="startup", dropped=0))

    async def run(self) -> None:
        while True:
            gap = await self._queue.get()
            try:
                await self.process(gap)
            except Exception:
                log.exception("gap backfill crashed for %s", gap)
            self.processed += 1

    async def process(self, gap: GapInfo) -> None:
        window_start = gap.started - self._margin
        note = ""
        earliest = gap.ended - self._max_window
        if window_start < earliest:
            window_start = earliest
            note = f"truncated to the last {self._max_window}"
        window_end = gap.ended + self._margin
        gap_id = await self._open(gap, window_start, window_end, note)
        log.warning(
            "gap %d (%s): %s -> %s, backfilling trades", gap_id, gap.reason, gap.started, gap.ended
        )
        counts: Counter[str] = Counter()
        error = ""
        attempts = 0
        for attempt in range(1, self._attempts + 1):
            attempts = attempt
            try:
                await self._fetch(window_start, window_end, counts)
                error = ""
                break
            except Exception as exc:  # a retry re-reads the window; duplicates are skipped
                error = f"{type(exc).__name__}: {exc}"
                log.warning("gap %d backfill attempt %d failed: %s", gap_id, attempt, error)
                if attempt < self._attempts:
                    await self._sleep(self._retry_base * 2 ** (attempt - 1))
        if attempts > 1:  # counts accumulate across attempts, so repeats show as duplicates
            note = "; ".join(x for x in (note, f"{attempts} attempts") if x)
        await self._close(gap_id, counts, error, note)
        log.warning(
            "gap %d %s: found %d, added %d, duplicates %d, combo large %d, combos skipped %d",
            gap_id,
            "failed" if error else "done",
            sum(counts.values()),
            counts["added"],
            counts["duplicate"],
            counts["combo_large"],
            counts["combo_skipped"],
        )

    async def _fetch(self, start: datetime, end: datetime, counts: Counter[str]) -> None:
        # REST takes whole seconds, so it returns trades from the rounded-off sliver before
        # ``start``. Look up what is already stored over that same rounded range, plus a margin
        # (the live stream stamps milliseconds, REST microseconds), or those trades would be
        # fetched but not recognised and stored twice (found live, 2026-10-09).
        low, high = math.floor(start.timestamp()), math.ceil(end.timestamp())
        pad = timedelta(seconds=2)
        async with self._engine.connect() as conn:
            found = await conn.execute(
                text(KNOWN_IDS),
                {
                    "a": datetime.fromtimestamp(low, UTC) - pad,
                    "b": datetime.fromtimestamp(high, UTC) + pad,
                },
            )
            known = {row[0] for row in found}
        async for trade in self._rest.iter_trades(limit=1000, min_ts=low, max_ts=high):
            counts[await self._ingestor.backfill_trade(trade, known)] += 1

    async def _open(self, gap: GapInfo, start: datetime, end: datetime, note: str) -> int:
        async with self._engine.begin() as conn:
            gap_id: int = (
                await conn.execute(
                    text(
                        "INSERT INTO ingest_gaps (started_at, ended_at, reason, dropped, status, "
                        "window_start, window_end, note) VALUES (:s, :e, :r, :d, 'running', "
                        ":ws, :we, :n) RETURNING id"
                    ),
                    {
                        "s": gap.started,
                        "e": gap.ended,
                        "r": gap.reason,
                        "d": gap.dropped,
                        "ws": start,
                        "we": end,
                        "n": note,
                    },
                )
            ).scalar_one()
            return gap_id

    async def _close(self, gap_id: int, counts: Counter[str], error: str, note: str) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "UPDATE ingest_gaps SET status = :status, finished_at = now(), "
                    "trades_found = :found, trades_added = :added, duplicates = :dup, "
                    "combo_large_added = :cl, combo_skipped = :cs, note = :note WHERE id = :id"
                ),
                {
                    "status": "failed" if error else "done",
                    "found": sum(counts.values()),
                    "added": counts["added"],
                    "dup": counts["duplicate"],
                    "cl": counts["combo_large"],
                    "cs": counts["combo_skipped"],
                    "note": "; ".join(x for x in (note, error) if x),
                    "id": gap_id,
                },
            )
