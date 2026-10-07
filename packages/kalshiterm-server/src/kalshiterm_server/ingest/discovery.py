"""Discovery poller: keeps ``series``, ``events`` and ``markets`` in step with Kalshi.

Two modes. A **full** refresh (first run, at most once a day after that, or on demand) reads
every ``unopened`` / ``open`` / ``closed`` event and market plus markets settled since the
bookmark. An **incremental** cycle asks only for events and markets *updated* since the
bookmark (``min_updated_ts``); live probing showed that covers new markets, status changes and
settlements, and costs seconds instead of a minute. Multivariate (combo) markets are excluded:
they arrive through lifecycle events and are handled separately.
"""

import logging
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from kalshi_core.models import Event, Market
from kalshi_core.rest import KalshiRestClient
from sqlalchemy import Table
from sqlalchemy.ext.asyncio import AsyncEngine

from kalshiterm_server.fixedpoint import PrecisionError
from kalshiterm_server.storage import reference, tables

log = logging.getLogger("kalshiterm_server")

OPEN_STATUSES = ("unopened", "open", "closed")
UPDATED_KEY = "updated_since"  # epoch seconds: re-read everything updated after this
FULL_KEY = "last_full_at"  # epoch seconds of the last full refresh
CHUNK = 1000
LOOKUP_BATCH = 100  # tickers per direct lookup
OVERLAP = timedelta(minutes=2)  # re-read a little to be safe against clock/ordering edges


@dataclass
class DiscoveryReport:
    mode: str = "full"
    series: reference.UpsertResult = field(default_factory=reference.UpsertResult)
    events: reference.UpsertResult = field(default_factory=reference.UpsertResult)
    markets: reference.UpsertResult = field(default_factory=reference.UpsertResult)
    resolved: int = 0  # stream-created placeholders described by a direct ticker lookup
    unresolved: int = 0  # placeholders Kalshi did not return (retried next cycle)
    seconds: float = 0.0

    def lines(self) -> list[str]:
        rows = [("series", self.series), ("events", self.events), ("markets", self.markets)]
        return (
            [f"mode: {self.mode}"]
            + [
                f"{name:8} fetched {r.total:>8,}  new {r.inserted:>8,}  "
                f"changed {r.updated:>8,}  unchanged {r.unchanged:>8,}  rejected {r.rejected:>4,}"
                for name, r in rows
            ]
            + [
                f"placeholders: resolved {self.resolved}, still unknown {self.unresolved}",
                f"took {self.seconds:.1f}s",
            ]
        )


async def _stream(
    engine: AsyncEngine,
    table: Table,
    key: str,
    items: AsyncIterator[Any],
    to_row: Callable[[Any], dict[str, Any]],
    into: reference.UpsertResult,
) -> None:
    chunk: list[dict[str, Any]] = []
    async for item in items:
        try:
            chunk.append(to_row(item))
        except PrecisionError as exc:
            # Loud but not fatal: one odd row must not stop ingestion of the rest.
            into.rejected += 1
            log.error("rejected %s row: %s", table.name, exc)
            continue
        if len(chunk) >= CHUNK:
            into.add(await reference.upsert(engine, table, chunk, key))
            chunk = []
    into.add(await reference.upsert(engine, table, chunk, key))


async def _int_state(engine: AsyncEngine, key: str) -> int | None:
    value = await reference.get_state(engine, key)
    return int(value) if value else None


async def _resolve_placeholders(
    rest: KalshiRestClient, engine: AsyncEngine, report: DiscoveryReport
) -> None:
    pending = await reference.unknown_tickers(engine)
    resolved = 0
    for start in range(0, len(pending), LOOKUP_BATCH):
        wanted = pending[start : start + LOOKUP_BATCH]
        page = await rest.markets_page(tickers=",".join(wanted), limit=LOOKUP_BATCH)
        rows: list[dict[str, Any]] = []
        for market in page.markets:
            try:
                rows.append(reference.market_row(market))
            except PrecisionError as exc:
                report.markets.rejected += 1
                log.error("rejected markets row: %s", exc)
        report.markets.add(await reference.upsert(engine, tables.markets, rows, "ticker"))
        resolved += len(rows)
    report.resolved = resolved
    report.unresolved = len(pending) - resolved


async def discover(
    rest: KalshiRestClient,
    engine: AsyncEngine,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    first_run_lookback: timedelta = timedelta(days=7),
    full: bool = False,
    full_refresh_every: timedelta = timedelta(hours=24),
) -> DiscoveryReport:
    started = time.monotonic()
    cycle_start = now()
    since = await _int_state(engine, UPDATED_KEY)
    last_full = await _int_state(engine, FULL_KEY)
    do_full = (
        full
        or since is None
        or last_full is None
        or cycle_start - datetime.fromtimestamp(last_full, UTC) >= full_refresh_every
    )
    report = DiscoveryReport(mode="full" if do_full else "incremental")

    series = await rest.series_list()  # one request; cheap enough for every cycle
    series_rows = [reference.series_row(s) for s in series]
    report.series.add(await reference.upsert(engine, tables.series, series_rows, "ticker"))

    events: AsyncIterator[Event]
    markets: AsyncIterator[Market]
    if do_full:
        for status in OPEN_STATUSES:
            events = rest.iter_events(status=status, limit=200)
            await _stream(
                engine, tables.events, "event_ticker", events, reference.event_row, report.events
            )
        for status in OPEN_STATUSES:
            markets = rest.iter_markets(status=status, mve_filter="exclude", limit=1000)
            await _stream(
                engine, tables.markets, "ticker", markets, reference.market_row, report.markets
            )
        settled_since = since or reference.epoch(cycle_start - first_run_lookback)
        markets = rest.iter_markets(
            status="settled", min_settled_ts=settled_since, mve_filter="exclude", limit=1000
        )
        await _stream(
            engine, tables.markets, "ticker", markets, reference.market_row, report.markets
        )
    else:
        assert since is not None
        events = rest.iter_events(min_updated_ts=since, limit=200)
        await _stream(
            engine, tables.events, "event_ticker", events, reference.event_row, report.events
        )
        markets = rest.iter_markets(min_updated_ts=since, mve_filter="exclude", limit=1000)
        await _stream(
            engine, tables.markets, "ticker", markets, reference.market_row, report.markets
        )

    # Markets the stream saw before discovery did: look them up directly by ticker, so a
    # placeholder never depends on falling inside the incremental window.
    await _resolve_placeholders(rest, engine, report)

    # Only advance the bookmarks once everything above succeeded.
    await reference.set_state(engine, UPDATED_KEY, str(reference.epoch(cycle_start - OVERLAP)))
    if do_full:
        await reference.set_state(engine, FULL_KEY, str(reference.epoch(cycle_start)))

    report.seconds = time.monotonic() - started
    log.info("discovery (%s) finished in %.1fs", report.mode, report.seconds)
    return report
