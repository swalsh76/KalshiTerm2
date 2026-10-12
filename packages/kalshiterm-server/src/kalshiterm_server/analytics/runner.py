"""Running analyzers on their schedules and recording what happened.

Each analyzer has its own loop, so a slow or failing one never delays the others. A run is
recorded in ``analyzer_runs`` (running, then ok / failed / timeout), its results are written in
ONE transaction with the NOTIFY and the final status (all or nothing), and a Postgres advisory
lock per analyzer keeps two analytics processes from running the same analyzer at once.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from kalshiterm_server.analytics.base import (
    AnalysisContext,
    Analyzer,
    AnalyzerError,
    Result,
    check_result,
)

log = logging.getLogger(__name__)
CHANNEL = "kterm_analysis"
KEEP_RUNS = timedelta(days=7)

LOCK = "SELECT pg_try_advisory_lock(hashtextextended(:key, 0))"
UNLOCK = "SELECT pg_advisory_unlock(hashtextextended(:key, 0))"
INSERT = (
    "INSERT INTO analysis_results (ts, analyzer, metric, market_id, event_ticker, value_e6) "
    "VALUES (:ts, :analyzer, :metric, :market_id, :event_ticker, :value_e6)"
)


@dataclass(frozen=True)
class RunRecord:
    analyzer: str
    status: str  # ok | failed | timeout
    results: int
    error: str


class AnalysisRunner:
    def __init__(
        self,
        engine: AsyncEngine,
        analyzers: dict[str, Analyzer],
        *,
        max_results: int = 50_000,
        retry: timedelta = timedelta(minutes=5),
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
    ) -> None:
        self._engine = engine
        self._analyzers = analyzers
        self._max = max_results
        self._retry = retry
        self._clock = clock
        self._sleep = sleep

    # ------------------------------------------------------------------ one run

    async def run_once(self, analyzer: Analyzer) -> RunRecord | None:
        """Run ``analyzer`` now. None when another process is already running it."""
        key = f"kterm_analyzer:{analyzer.name}"
        async with self._engine.connect() as holder:
            auto = await holder.execution_options(isolation_level="AUTOCOMMIT")
            if not (await auto.execute(text(LOCK), {"key": key})).scalar_one():
                log.info("analyzer %s is running elsewhere; skipped", analyzer.name)
                return None
            try:
                return await self._run_locked(analyzer)
            finally:
                try:
                    await auto.execute(text(UNLOCK), {"key": key})
                except Exception:  # the lock dies with the connection anyway
                    log.warning("could not release the lock for %s", analyzer.name)

    async def _run_locked(self, analyzer: Analyzer) -> RunRecord:
        started = self._clock()
        async with self._engine.begin() as conn:
            await conn.execute(  # a previous process died mid-run: say so instead of 'running'
                text(
                    "UPDATE analyzer_runs SET status = 'failed', finished_at = :now, "
                    "error = 'interrupted: the process stopped during the run' "
                    "WHERE analyzer = :a AND status = 'running'"
                ),
                {"a": analyzer.name, "now": started},
            )
            run_id = (
                await conn.execute(
                    text(
                        "INSERT INTO analyzer_runs (analyzer, started_at, interval_seconds) "
                        "VALUES (:a, :t, :i) RETURNING id"
                    ),
                    {"a": analyzer.name, "t": started, "i": int(analyzer.interval.total_seconds())},
                )
            ).scalar_one()
        status, error, count = "ok", "", 0
        try:
            async with asyncio.timeout(analyzer.timeout.total_seconds()):
                produced = await analyzer.run(AnalysisContext(self._engine, started))
            rows = self._validate(analyzer.name, produced, started)
            count = len(rows)
        except TimeoutError:
            status, error = "timeout", f"no answer within {analyzer.timeout}"
        except Exception as exc:
            status, error = "failed", f"{exc.__class__.__name__}: {exc}"
            if not isinstance(exc, AnalyzerError):
                log.exception("analyzer %s failed", analyzer.name)
        await self._finish(run_id, analyzer, started, status, error, rows if status == "ok" else [])
        return RunRecord(analyzer.name, status, count if status == "ok" else 0, error)

    def _validate(
        self, name: str, produced: Sequence[Result], started: datetime
    ) -> list[dict[str, Any]]:
        if not isinstance(produced, Sequence) or isinstance(produced, str | bytes):
            raise AnalyzerError(
                f"{name}: run must return a list of Result, got {type(produced).__name__}"
            )
        if len(produced) > self._max:
            raise AnalyzerError(f"{name}: {len(produced)} results in one run (limit {self._max})")
        rows = []
        for item in produced:
            result = check_result(name, item)
            rows.append(
                {
                    "ts": result.ts or started,
                    "analyzer": name,
                    "metric": result.metric,
                    "market_id": result.market_id,
                    "event_ticker": result.event_ticker,
                    "value_e6": result.value_e6,
                }
            )
        return rows

    async def _finish(
        self,
        run_id: int,
        analyzer: Analyzer,
        started: datetime,
        status: str,
        error: str,
        rows: list[dict[str, Any]],
    ) -> None:
        async with self._engine.begin() as conn:
            if rows:
                await conn.execute(text(INSERT), rows)
                await conn.execute(
                    text("SELECT pg_notify(:c, :p)"),
                    {"c": CHANNEL, "p": f"{analyzer.name}:{len(rows)}"},
                )
            await conn.execute(
                text(
                    "UPDATE analyzer_runs SET status = :s, finished_at = :t, results = :n, "
                    "error = :e WHERE id = :id"
                ),
                {"s": status, "t": self._clock(), "n": len(rows), "e": error[:500], "id": run_id},
            )
            await conn.execute(
                text("DELETE FROM analyzer_runs WHERE analyzer = :a AND started_at < :cutoff"),
                {"a": analyzer.name, "cutoff": started - KEEP_RUNS},
            )

    # ------------------------------------------------------------------ the schedule

    async def _last_ok(self, name: str) -> datetime | None:
        async with self._engine.connect() as conn:
            value = (
                await conn.execute(
                    text(
                        "SELECT max(started_at) FROM analyzer_runs "
                        "WHERE analyzer = :a AND status = 'ok'"
                    ),
                    {"a": name},
                )
            ).scalar_one()
        return value  # type: ignore[no-any-return]

    async def _loop(self, analyzer: Analyzer, rounds: int | None) -> None:
        last = await self._last_ok(analyzer.name)
        due = last + analyzer.interval if last else self._clock()  # a restart does not re-run
        done = 0
        while rounds is None or done < rounds:
            await self._sleep(max(0.0, (due - self._clock()).total_seconds()))
            started = self._clock()
            try:
                record = await self.run_once(analyzer)
                ok = record is None or record.status == "ok"
            except Exception:  # e.g. the database is restarting: try again later
                log.exception("could not run analyzer %s", analyzer.name)
                ok = False
            due = started + (analyzer.interval if ok else min(analyzer.interval, self._retry))
            done += 1

    async def serve(self, rounds: int | None = None) -> None:
        """Run every analyzer forever (or ``rounds`` runs each, for tests)."""
        if not self._analyzers:
            log.warning("no analyzers installed: nothing to run")
            if rounds is not None:
                return
            await asyncio.Event().wait()
        async with asyncio.TaskGroup() as group:
            for analyzer in self._analyzers.values():
                group.create_task(self._loop(analyzer, rounds))

    async def record_load_failures(self, failures: Sequence[Any]) -> None:
        """A plugin that did not load shows up in ``status`` like a failed run."""
        async with self._engine.begin() as conn:
            for failure in failures:
                await conn.execute(
                    text(
                        "INSERT INTO analyzer_runs (analyzer, started_at, finished_at, status, "
                        "interval_seconds, error) VALUES (:a, :t, :t, 'load_failed', 0, :e)"
                    ),
                    {"a": failure.entry_point[:48], "t": self._clock(), "e": failure.error[:500]},
                )
