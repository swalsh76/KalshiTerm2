"""Storage governor (PLAN §9.3): keeps the server inside its disk budget.

Each cycle it measures Postgres' footprint (data plus WAL), stores a sample, projects the days
left at the current growth rate, and moves between three modes with hysteresis so it never
flaps:

* ``normal``
* ``tightened`` from 80% of the budget: raw retention windows are halved (never below a floor)
  and restored exactly when usage falls back under 70%;
* ``shedding`` from 90%: orderbook deltas are no longer stored (snapshots still are). Trades
  and tickers are never shed. Resumes under 85%. The period is written to ``ingest_gaps`` so
  nobody mistakes the missing deltas for a quiet market.

It also checks the disk the database lives on: free space below a threshold and a real
write-and-fsync probe, because "the drive vanished" is the failure an external SSD adds.
Inside the server container the path it checks is the Docker VM's disk, which is where
Postgres' data lives; whether the host mounted the external drive is checked outside.
"""

import asyncio
import json
import logging
import os
import re
import shutil
import tempfile
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

log = logging.getLogger("kalshiterm_server")

NORMAL, TIGHTENED, SHEDDING = "normal", "tightened", "shedding"
GB = 1024**3

# raw tables whose retention may be tightened -> shortest window the governor will set (days)
TIGHTEN_FLOOR_DAYS = {"tickers": 3, "orderbook_deltas": 3, "trades": 7}
SAMPLE_KEEP = timedelta(days=30)

MEASURE_TABLES = """
SELECT name, bytes FROM (
    SELECT hypertable_name::text AS name,
           hypertable_size(format('%I.%I', hypertable_schema, hypertable_name)::regclass) AS bytes
    FROM timescaledb_information.hypertables
    WHERE hypertable_name NOT LIKE '\\_materialized\\_hypertable\\_%'
    UNION ALL
    SELECT view_name::text,
           hypertable_size(format('%I.%I', materialization_hypertable_schema,
                                  materialization_hypertable_name)::regclass)
    FROM timescaledb_information.continuous_aggregates
    UNION ALL
    SELECT c.relname::text, pg_total_relation_size(c.oid)
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relkind = 'r'
      AND NOT EXISTS (SELECT 1 FROM timescaledb_information.hypertables h
                      WHERE h.hypertable_schema = 'public' AND h.hypertable_name = c.relname)
) sizes ORDER BY bytes DESC
"""


@dataclass(slots=True)
class Usage:
    db_bytes: int
    wal_bytes: int | None  # None when the database user may not list the WAL directory
    tables: dict[str, int]
    disk_total: int | None = None
    disk_free: int | None = None
    drive_error: str | None = None

    @property
    def used(self) -> int:
        return self.db_bytes + (self.wal_bytes or 0)


def probe_drive(path: str) -> tuple[int | None, int | None, str | None]:
    """(total bytes, free bytes, error) of the disk holding ``path``, proving it is writable."""
    try:
        usage = shutil.disk_usage(path)
        fd, name = tempfile.mkstemp(prefix="kterm-probe-", dir=path)
        try:
            os.write(fd, b"kalshiterm")
            os.fsync(fd)
        finally:
            os.close(fd)
            os.unlink(name)
    except OSError as exc:
        return None, None, f"{type(exc).__name__}: {exc}"
    return usage.total, usage.free, None


async def measure_usage(engine: AsyncEngine, disk_path: str) -> Usage:
    async with engine.connect() as conn:
        db_bytes = (
            await conn.execute(text("SELECT pg_database_size(current_database())"))
        ).scalar_one()
        tables = {name: int(size) for name, size in await conn.execute(text(MEASURE_TABLES))}
        try:
            wal = (
                await conn.execute(text("SELECT COALESCE(sum(size), 0) FROM pg_ls_waldir()"))
            ).scalar_one()
        except Exception:  # needs superuser or pg_monitor
            wal = None
    total, free, error = await asyncio.to_thread(probe_drive, disk_path)
    return Usage(int(db_bytes), None if wal is None else int(wal), tables, total, free, error)


# ---------------------------------------------------------------- pure decisions


@dataclass(frozen=True, slots=True)
class Thresholds:
    warn: float = 0.80  # tighten raw retention
    shed: float = 0.90  # stop storing orderbook deltas
    resume: float = 0.85  # start storing them again
    relax: float = 0.70  # restore the retention windows
    drive_free_min: float = 0.15  # alert below this fraction of free space


def next_mode(mode: str, fraction: float, t: Thresholds | None = None) -> str:
    """Mode after seeing ``fraction`` of the budget in use, with hysteresis."""
    t = t or Thresholds()
    if fraction >= t.shed:
        return SHEDDING
    if mode == SHEDDING:
        if fraction >= t.resume:
            return SHEDDING
        return TIGHTENED if fraction >= t.relax else NORMAL
    if mode == TIGHTENED:
        return NORMAL if fraction < t.relax else TIGHTENED
    return TIGHTENED if fraction >= t.warn else NORMAL


@dataclass(frozen=True, slots=True)
class Projection:
    bytes_per_day: float | None  # None: not enough data to say
    days_to_full: float | None  # None: not growing, or not enough data
    note: str


def project(
    samples: list[tuple[datetime, int]],
    budget_bytes: int,
    *,
    min_samples: int = 3,
    min_span: timedelta = timedelta(hours=1),
) -> Projection:
    """Least-squares growth rate over the samples and the days left until the budget is hit."""
    if len(samples) < min_samples:
        return Projection(None, None, f"need {min_samples} samples, have {len(samples)}")
    ordered = sorted(samples)
    span = ordered[-1][0] - ordered[0][0]
    if span < min_span:
        shown = timedelta(seconds=int(span.total_seconds()))
        return Projection(None, None, f"samples span only {shown}, need {min_span}")
    t0 = ordered[0][0]
    xs = [(when - t0).total_seconds() / 86_400 for when, _ in ordered]  # days
    ys = [float(size) for _, size in ordered]
    n = len(xs)
    mean_x, mean_y = sum(xs) / n, sum(ys) / n
    denom = sum((x - mean_x) ** 2 for x in xs)
    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys, strict=True)) / denom
    last = ordered[-1][1]
    if slope <= 0:
        return Projection(slope, None, "not growing")
    return Projection(slope, max(0.0, (budget_bytes - last) / slope), "")


# ---------------------------------------------------------------- the governor


class Shedder(Protocol):
    """What the governor needs from the ingestor."""

    shed_orderbook_deltas: bool


@dataclass(slots=True)
class Decision:
    fraction: float
    mode: str
    previous: str
    actions: list[str] = field(default_factory=list)
    projection: Projection | None = None


class Governor:
    def __init__(
        self,
        engine: AsyncEngine,
        budget_bytes: int,
        *,
        shedder: Shedder | None = None,
        disk_path: str | None = None,
        interval: float = 600.0,
        thresholds: Thresholds | None = None,
        measure: Callable[[], Awaitable[Usage]] | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._engine = engine
        self._budget = budget_bytes
        self._shedder = shedder
        self._interval = interval
        self._t = thresholds or Thresholds()
        self._clock = clock
        path = disk_path or tempfile.gettempdir()
        self._measure = measure or (lambda: measure_usage(engine, path))

    async def run(self) -> None:
        while True:
            try:
                await self.cycle()
            except Exception:
                log.exception("storage governor cycle failed; will retry")
            await asyncio.sleep(self._interval)

    async def cycle(self) -> Decision:
        usage = await self._measure()
        now = self._clock()
        await self._store_sample(usage, now)
        fraction = usage.used / self._budget
        previous = await self._get("mode") or NORMAL
        mode = next_mode(previous, fraction, self._t)
        decision = Decision(fraction, mode, previous)
        await self._apply(decision, usage, now)
        await self._check_drive(usage, decision)
        decision.projection = project(await self._recent_samples(now), self._budget)
        if mode == SHEDDING:
            await self._extend_shed_period(now)
        if self._shedder is not None:  # a restart in shedding mode must keep shedding
            self._shedder.shed_orderbook_deltas = mode == SHEDDING
        return decision

    # -- mode transitions
    async def _apply(self, d: Decision, usage: Usage, now: datetime) -> None:
        if d.mode == d.previous:
            return
        pct = round(d.fraction * 100, 1)
        log.warning("storage %s%% of budget: %s -> %s", pct, d.previous, d.mode)
        if d.mode in (TIGHTENED, SHEDDING) and d.previous == NORMAL:
            changed = await self._tighten_retention()
            d.actions.append(f"tightened retention: {changed}")
            await self._event("tighten", {"used_pct": pct, "retention": changed})
        if d.mode == SHEDDING:
            await self._start_shedding(now)
            d.actions.append("stopped storing orderbook deltas")
            await self._event("shed_deltas", {"used_pct": pct})
        if d.previous == SHEDDING and d.mode != SHEDDING:
            await self._stop_shedding(now)
            d.actions.append("resumed storing orderbook deltas")
            await self._event("resume_deltas", {"used_pct": pct})
        if d.mode == NORMAL and d.previous != NORMAL:
            restored = await self._restore_retention()
            d.actions.append(f"restored retention: {restored}")
            await self._event("restore", {"used_pct": pct, "retention": restored})
        await self._set("mode", d.mode)

    async def _tighten_retention(self) -> dict[str, str]:
        changed: dict[str, str] = {}
        async with self._engine.begin() as conn:
            jobs = await conn.execute(
                text(
                    "SELECT job_id, hypertable_name, config FROM timescaledb_information.jobs "
                    "WHERE proc_name = 'policy_retention' AND hypertable_name = ANY(:names)"
                ),
                {"names": list(TIGHTEN_FLOOR_DAYS)},
            )
            for job_id, table, config in jobs.all():
                current = str(config["drop_after"])
                days = _days(current)
                if days is None:
                    log.warning("cannot tighten %s: unrecognised window %r", table, current)
                    continue
                new_days = max(TIGHTEN_FLOOR_DAYS[table], days // 2)
                if new_days >= days:
                    continue
                await self._remember(conn, f"retention:{table}", current)
                await self._set_window(conn, job_id, config, f"{new_days} days")
                changed[table] = f"{current} -> {new_days} days"
        return changed

    async def _restore_retention(self) -> dict[str, str]:
        restored: dict[str, str] = {}
        async with self._engine.begin() as conn:
            saved = await conn.execute(
                text("SELECT key, value FROM governor_state WHERE key LIKE 'retention:%'")
            )
            for key, original in saved.all():
                table = key.split(":", 1)[1]
                job = (
                    await conn.execute(
                        text(
                            "SELECT job_id, config FROM timescaledb_information.jobs "
                            "WHERE proc_name = 'policy_retention' AND hypertable_name = :t"
                        ),
                        {"t": table},
                    )
                ).first()
                if job is not None:
                    await self._set_window(conn, job[0], job[1], original)
                    restored[table] = original
                await conn.execute(text("DELETE FROM governor_state WHERE key = :k"), {"k": key})
        return restored

    @staticmethod
    async def _set_window(conn: Any, job_id: int, config: dict[str, Any], window: str) -> None:
        new = json.dumps({**config, "drop_after": window})
        await conn.execute(
            text("SELECT alter_job(:id, config => CAST(:cfg AS jsonb), next_start => now())"),
            {"id": job_id, "cfg": new},
        )

    async def _start_shedding(self, now: datetime) -> None:
        async with self._engine.begin() as conn:
            gap_id = (
                await conn.execute(
                    text(
                        "INSERT INTO ingest_gaps (started_at, ended_at, reason, status, note, "
                        "finished_at) VALUES (:t, :t, 'delta_shed', 'done', "
                        "'orderbook deltas not stored: storage budget', :t) RETURNING id"
                    ),
                    {"t": now},
                )
            ).scalar_one()
            await self._remember(conn, "shed_gap_id", str(gap_id))

    async def _extend_shed_period(self, now: datetime) -> None:
        """While shedding, the gap row says "no deltas stored up to at least now"."""
        gap_id = await self._get("shed_gap_id")
        if gap_id is not None:
            async with self._engine.begin() as conn:
                await conn.execute(
                    text("UPDATE ingest_gaps SET ended_at = :t WHERE id = :id"),
                    {"t": now, "id": int(gap_id)},
                )

    async def _stop_shedding(self, now: datetime) -> None:
        gap_id = await self._get("shed_gap_id")
        if gap_id is None:
            return
        async with self._engine.begin() as conn:
            await conn.execute(
                text("UPDATE ingest_gaps SET ended_at = :t, finished_at = :t WHERE id = :id"),
                {"t": now, "id": int(gap_id)},
            )
            await conn.execute(text("DELETE FROM governor_state WHERE key = 'shed_gap_id'"))

    # -- the disk
    async def _check_drive(self, usage: Usage, d: Decision) -> None:
        detail: dict[str, Any]
        if usage.drive_error:
            state, detail = "error", {"error": usage.drive_error}
        elif usage.disk_total and usage.disk_free is not None:
            free = usage.disk_free / usage.disk_total
            low = free < self._t.drive_free_min
            state, detail = ("low" if low else "ok"), {"free_pct": round(free * 100, 1)}
        else:
            return
        if state != (await self._get("drive") or "ok"):
            level = log.warning if state != "ok" else log.info
            level("data drive: %s %s", state, detail)
            await self._event(f"drive_{state}", detail)
            await self._set("drive", state)
            d.actions.append(f"drive {state}")

    # -- storage of samples, state and events
    async def _store_sample(self, usage: Usage, now: datetime) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO storage_samples (ts, db_bytes, wal_bytes, used_bytes, "
                    "disk_total, disk_free, tables) VALUES (:ts, :db, :wal, :used, :dt, :df, "
                    "CAST(:tables AS jsonb)) ON CONFLICT (ts) DO NOTHING"
                ),
                {
                    "ts": now,
                    "db": usage.db_bytes,
                    "wal": usage.wal_bytes,
                    "used": usage.used,
                    "dt": usage.disk_total,
                    "df": usage.disk_free,
                    "tables": json.dumps(usage.tables),
                },
            )
            await conn.execute(
                text("DELETE FROM storage_samples WHERE ts < :cutoff"),
                {"cutoff": now - SAMPLE_KEEP},
            )

    async def _recent_samples(self, now: datetime) -> list[tuple[datetime, int]]:
        async with self._engine.connect() as conn:
            rows = await conn.execute(
                text("SELECT ts, used_bytes FROM storage_samples WHERE ts >= :since ORDER BY ts"),
                {"since": now - timedelta(hours=24)},
            )
            return [(ts, int(used)) for ts, used in rows]

    async def _get(self, key: str) -> str | None:
        async with self._engine.connect() as conn:
            return (
                await conn.execute(
                    text("SELECT value FROM governor_state WHERE key = :k"), {"k": key}
                )
            ).scalar_one_or_none()

    async def _set(self, key: str, value: str) -> None:
        async with self._engine.begin() as conn:
            await self._remember(conn, key, value)

    @staticmethod
    async def _remember(conn: Any, key: str, value: str) -> None:
        await conn.execute(
            text(
                "INSERT INTO governor_state (key, value) VALUES (:k, :v) "
                "ON CONFLICT (key) DO UPDATE SET value = :v, updated_at = now()"
            ),
            {"k": key, "v": value},
        )

    async def _event(self, kind: str, detail: dict[str, Any]) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(
                text("INSERT INTO governor_events (kind, detail) VALUES (:k, CAST(:d AS jsonb))"),
                {"k": kind, "d": json.dumps(detail)},
            )


def _days(window: str) -> int | None:
    match = re.fullmatch(r"\s*(\d+)\s+days?\s*", window)
    return int(match.group(1)) if match else None
