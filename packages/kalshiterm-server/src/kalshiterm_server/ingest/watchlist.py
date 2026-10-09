"""Watchlist controller: decides which markets get orderbook capture and the trade copy.

Three sources feed one set. **Manual** markets come from a TOML file that is re-read every cycle,
so edits apply without a restart; they leave only when removed from the file. **User** markets
are those on any user's watchlist (the API); **auto** markets are the busiest by traded
contracts over a recent window. A market is watched while *anyone* wants it. User and auto
entries stay at least ``dwell`` (decision 13: 12 hours) once added, so neither a market near
the top-N cut-off nor a user adding and removing one repeatedly makes the feed churn.

Applying a change touches two places in a safe order: the orderbook feed (live add/remove),
then the ingestor, which opens/closes the coverage period and starts/stops the permanent
trade copy inside its next write transaction.
"""

import asyncio
import logging
import time
import tomllib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

log = logging.getLogger("kalshiterm_server")

COMBO_PREFIX = "KXMVE"
DWELL = timedelta(hours=12)


class Feed(Protocol):
    async def add_markets(self, tickers: list[str]) -> None: ...
    async def remove_markets(self, tickers: list[str]) -> None: ...


class Recorder(Protocol):
    def watch(self, tickers: list[str], source: str = "manual") -> None: ...
    def unwatch(self, tickers: list[str]) -> None: ...


@dataclass(frozen=True, slots=True)
class WatchlistConfig:
    markets: frozenset[str] = frozenset()
    auto_top_n: int = 0
    auto_window: timedelta = timedelta(hours=1)


def _whole(value: object, minimum: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= minimum


def load_config(path: Path) -> WatchlistConfig:
    """Parse the TOML file; a mistake raises ``ValueError`` naming the problem."""
    try:
        data = tomllib.loads(path.read_text())
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(f"{path}: {exc}") from exc
    section = data.get("watchlist", {})
    markets = section.get("markets", [])
    top_n = section.get("auto_top_n", 0)
    window_minutes = section.get("auto_window_minutes", 60)
    if not isinstance(markets, list) or not all(isinstance(m, str) for m in markets):
        raise ValueError(f"{path}: watchlist.markets must be a list of tickers")
    if not _whole(top_n, 0):
        raise ValueError(f"{path}: watchlist.auto_top_n must be a whole number >= 0")
    if not _whole(window_minutes, 1):
        raise ValueError(f"{path}: watchlist.auto_window_minutes must be a whole number >= 1")
    return WatchlistConfig(
        frozenset(m.strip() for m in markets if m.strip()),
        top_n,
        timedelta(minutes=window_minutes),
    )


@dataclass(slots=True)
class Entry:
    source: str  # "manual" | "user" | "auto": who currently holds the market (best claim wins)
    added_at: datetime


@dataclass(slots=True)
class Plan:
    add: dict[str, str]  # ticker -> source
    remove: list[str]


def plan(
    manual: frozenset[str],
    top: list[str],
    current: dict[str, Entry],
    now: datetime,
    dwell: timedelta = DWELL,
    users: frozenset[str] = frozenset(),
) -> Plan:
    """Pure reconcile step: what to add and remove.

    A market is kept while anyone wants it (the file, a user, the top-N). One nobody wants is
    removed at once if the file was its holder (an edit takes effect immediately), otherwise
    once it has been watched for at least ``dwell``.
    """
    add = {t: "manual" for t in sorted(manual) if t not in current}
    add.update({t: "user" for t in sorted(users - manual) if t not in current})
    add.update({t: "auto" for t in top if t not in manual and t not in users and t not in current})
    wanted = manual | users | set(top)
    remove: list[str] = []
    for ticker, entry in current.items():
        if ticker in wanted:
            continue
        if entry.source == "manual" or now - entry.added_at >= dwell:
            remove.append(ticker)
    return Plan(add, sorted(remove))


TOP_BY_VOLUME = """
SELECT m.ticker
FROM trades t JOIN markets m ON m.id = t.market_id
WHERE t.ts >= :since AND m.ticker NOT LIKE :combo AND m.status IN ('open', 'active', 'unknown')
GROUP BY m.ticker
ORDER BY sum(t.count_e2) DESC, m.ticker
LIMIT :n
"""


async def top_by_volume(engine: AsyncEngine, n: int, window: timedelta, now: datetime) -> list[str]:
    """The ``n`` ordinary markets with the most contracts traded in the window."""
    if n <= 0:
        return []
    async with engine.connect() as conn:
        rows = await conn.execute(
            text(TOP_BY_VOLUME), {"since": now - window, "combo": COMBO_PREFIX + "%", "n": n}
        )
        return [ticker for (ticker,) in rows]


USERS_WANT = """
SELECT DISTINCT m.ticker
FROM user_watchlists w JOIN markets m ON m.id = w.market_id
WHERE m.settlement_ts IS NULL AND m.ticker NOT LIKE :combo
"""
USERS_SIGNATURE = """
SELECT count(*)::text || ':' || coalesce(max(added_at)::text, '') FROM user_watchlists
"""


class WatchlistController:
    def __init__(
        self,
        engine: AsyncEngine,
        feed: Feed,
        recorder: Recorder,
        config: Callable[[], WatchlistConfig],
        *,
        extra_manual: frozenset[str] = frozenset(),
        interval: float = 300.0,
        poll_interval: float = 15.0,
        dwell: timedelta = DWELL,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._engine = engine
        self._feed = feed
        self._recorder = recorder
        self._config = config
        self._extra = extra_manual
        self._interval = interval
        self._poll_interval = poll_interval
        self._dwell = dwell
        self._clock = clock
        self._sleep = sleep
        self._monotonic = monotonic
        self._signature: str | None = None  # of the users' lists at the last reconcile
        self.current: dict[str, Entry] = {}
        self.failed: set[str] = set()  # markets the feed refused (e.g. unknown ticker)

    async def start(self) -> None:
        """Close periods left open by a previous run (they did not cover the downtime)."""
        async with self._engine.begin() as conn:
            await conn.execute(
                text("UPDATE watchlist_periods SET removed_at = now() WHERE removed_at IS NULL")
            )
        await self.reconcile()

    async def run(self) -> None:
        """Full reconcile every ``interval``; in between, a cheap poll that reconciles early
        when a user's list changed (so a new entry is captured within seconds, not minutes)."""
        last_full = self._monotonic()
        while True:
            await self._sleep(self._poll_interval)
            try:
                if self._monotonic() - last_full >= self._interval:
                    await self.reconcile()
                    last_full = self._monotonic()
                else:
                    await self.poll()
            except Exception:
                log.exception("watchlist reconcile failed; keeping the current list")

    async def poll(self) -> bool:
        """Reconcile now if any user's list changed since the last time; True if it did."""
        async with self._engine.connect() as conn:
            signature = (await conn.execute(text(USERS_SIGNATURE))).scalar_one()
        if signature == self._signature:
            return False
        await self.reconcile()
        return True

    async def reconcile(self) -> Plan:
        config = self._config()
        now = self._clock()
        manual = config.markets | self._extra
        async with self._engine.connect() as conn:
            # the signature first: a change landing while we work is seen by the next poll
            self._signature = (await conn.execute(text(USERS_SIGNATURE))).scalar_one()
            users = frozenset(
                ticker
                for (ticker,) in await conn.execute(text(USERS_WANT), {"combo": COMBO_PREFIX + "%"})
            )
        top = await top_by_volume(self._engine, config.auto_top_n, config.auto_window, now)
        for ticker, entry in self.current.items():  # the best claim holds a market
            if ticker in manual:
                entry.source = "manual"
            elif ticker in users:
                entry.source = "user"
            elif ticker in top:
                entry.source = "auto"
        todo = plan(manual, top, self.current, now, self._dwell, users)
        if todo.remove:
            await self._feed.remove_markets(todo.remove)
            self._recorder.unwatch(todo.remove)
            for ticker in todo.remove:
                del self.current[ticker]
            log.info("watchlist: removed %d (%s)", len(todo.remove), ", ".join(todo.remove[:5]))
        if todo.add:
            await self._add(todo.add, now)
        return todo

    async def _add(self, add: dict[str, str], now: datetime) -> None:
        try:
            await self._feed.add_markets(list(add))
        except Exception as exc:
            # One bad ticker must not block the rest: fall back to one at a time.
            log.warning("watchlist: batch add failed (%s); retrying individually", exc)
            for ticker, source in add.items():
                try:
                    await self._feed.add_markets([ticker])
                except Exception as single:
                    self.failed.add(ticker)
                    log.error("watchlist: cannot watch %s: %s", ticker, single)
                    continue
                self._admit(ticker, source, now)
            return
        for ticker, source in add.items():
            self._admit(ticker, source, now)
        log.info("watchlist: added %d (%s)", len(add), ", ".join(list(add)[:5]))

    def _admit(self, ticker: str, source: str, now: datetime) -> None:
        self.current[ticker] = Entry(source, now)
        self._recorder.watch([ticker], source)
        self.failed.discard(ticker)
