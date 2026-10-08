"""Server health report: ``kterm-server status`` (and, in Phase 3, the API's ``/status``).

``collect_status`` returns plain data; ``render`` formats it. Anything that needs attention is
gathered in ``problems`` and shown first, so a healthy server prints one reassuring line.
Every query on a hypertable is bounded by time so the report stays fast on a large database.
"""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from kalshiterm_server import db
from kalshiterm_server.governor import (
    GB,
    NORMAL,
    Thresholds,
    measure_usage,
    project,
)

STREAMS = {
    "tickers": "tickers",
    "trades": "trades",
    "market lifecycle": "market_lifecycle",
    "orderbook snapshots": "orderbook_snapshots",
    "orderbook deltas": "orderbook_deltas",
}
STALE_STREAM = timedelta(minutes=2)  # trades and tickers arrive many times a second
STALE_SAMPLE = timedelta(minutes=30)  # the governor samples every 10 minutes by default
STALE_DISCOVERY = timedelta(days=2)
STALE_HOST_CHECK = timedelta(minutes=5)  # the host script runs every minute


async def collect_status(
    engine: AsyncEngine,
    budget_gb: float,
    disk_path: str,
    now: datetime | None = None,
    host_state_file: str | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    problems: list[str] = []
    report: dict[str, Any] = {"time": now.isoformat(), "problems": problems}

    revision = await _query_one(engine, "SELECT version_num FROM alembic_version")
    report["database"] = {"revision": revision, "head": db.head_revision()}
    if revision != db.head_revision():
        problems.append(f"database is at revision {revision}, code expects {db.head_revision()}")

    report["storage"] = await _storage(engine, budget_gb, disk_path, now, problems)
    report["host"] = host_check(host_state_file, now, problems)
    report["streams"] = await _streams(engine, now, problems)
    report["gaps"] = await _gaps(engine, problems)
    report["jobs"] = await _jobs(engine, problems)
    report["discovery"] = await _discovery(engine, now, problems)
    report["watchlist"] = await _watchlist(engine)
    return report


def host_check(path: str | None, now: datetime, problems: list[str]) -> dict[str, Any] | None:
    """Read the file the host-side script writes about the external data drive.

    The container cannot see whether the host mounted the SSD, so a missing, stale or
    negative report is a problem. ``None`` means this deployment does not use the check.
    """
    if not path:
        return None
    try:
        state = json.loads(Path(path).read_text())
        checked = datetime.fromtimestamp(int(state["checked_at"]), UTC)
    except (OSError, ValueError, KeyError, TypeError):
        problems.append(f"host drive check has never reported (no usable {path})")
        return {"reported": False}
    age = now - checked
    if age > STALE_HOST_CHECK:
        problems.append(f"host drive check last ran {_age(age)} ago (is its launchd job running?)")
    if not state.get("mounted"):
        problems.append(f"data drive is NOT MOUNTED on the host ({state.get('path', '?')})")
    elif not state.get("writable"):
        problems.append(f"data drive on the host is not writable: {state.get('error', '')}")
    free = state.get("free_pct")
    if isinstance(free, int | float) and free < Thresholds().drive_free_min * 100:
        problems.append(f"data drive has only {free}% free on the host")
    return {"reported": True, "age_seconds": age.total_seconds(), **state}


async def quick_health(engine: AsyncEngine, now: datetime | None = None) -> list[str]:
    """Cheap liveness check for the container health check: no size measurements."""
    now = now or datetime.now(UTC)
    problems: list[str] = []
    try:
        revision = await _query_one(engine, "SELECT version_num FROM alembic_version")
    except Exception as exc:
        return [f"database unreachable: {type(exc).__name__}"]
    if revision != db.head_revision():
        problems.append(f"database is at revision {revision}, expected {db.head_revision()}")
    for table in ("tickers", "trades"):
        newest = await _query_one(
            engine, f"SELECT max(ts) FROM {table} WHERE ts > :since", since=now - timedelta(hours=1)
        )
        if newest is None or now - newest > timedelta(minutes=5):
            problems.append(f"no {table} stored in the last 5 minutes")
    return problems


async def _query_one(engine: AsyncEngine, sql: str, **params: Any) -> Any:
    async with engine.connect() as conn:
        return (await conn.execute(text(sql), params)).scalar_one_or_none()


async def _rows(engine: AsyncEngine, sql: str, **params: Any) -> list[dict[str, Any]]:
    async with engine.connect() as conn:
        return [dict(r._mapping) for r in await conn.execute(text(sql), params)]


async def _storage(
    engine: AsyncEngine, budget_gb: float, disk_path: str, now: datetime, problems: list[str]
) -> dict[str, Any]:
    usage = await measure_usage(engine, disk_path)
    budget = int(budget_gb * GB)
    fraction = usage.used / budget
    mode = await _query_one(engine, "SELECT value FROM governor_state WHERE key = 'mode'") or NORMAL
    last_sample = await _query_one(engine, "SELECT max(ts) FROM storage_samples")
    samples = await _rows(
        engine,
        "SELECT ts, used_bytes FROM storage_samples WHERE ts >= :since ORDER BY ts",
        since=now - timedelta(hours=24),
    )
    projection = project([(s["ts"], int(s["used_bytes"])) for s in samples], budget)
    events = await _rows(
        engine, "SELECT ts, kind, detail FROM governor_events ORDER BY id DESC LIMIT 5"
    )
    top = sorted(usage.tables.items(), key=lambda kv: kv[1], reverse=True)
    result: dict[str, Any] = {
        "used_bytes": usage.used,
        "db_bytes": usage.db_bytes,
        "wal_bytes": usage.wal_bytes,
        "budget_bytes": budget,
        "percent": round(fraction * 100, 1),
        "mode": mode,
        "tables": top,
        "other_bytes": max(0, usage.db_bytes - sum(usage.tables.values())),
        "bytes_per_day": projection.bytes_per_day,
        "days_to_full": projection.days_to_full,
        "projection_note": projection.note,
        "last_sample": last_sample.isoformat() if last_sample else None,
        "recent_events": events,
        "disk": {
            "total": usage.disk_total,
            "free": usage.disk_free,
            "error": usage.drive_error,
        },
    }
    th = Thresholds()
    if mode != NORMAL:
        problems.append(f"storage governor is {mode} ({result['percent']}% of budget)")
    elif fraction >= th.warn:
        problems.append(f"storage at {result['percent']}% of budget; governor has not acted yet")
    if usage.drive_error:
        problems.append(f"data drive check failed: {usage.drive_error}")
    elif (
        usage.disk_total
        and usage.disk_free is not None
        and usage.disk_free / usage.disk_total < th.drive_free_min
    ):
        free_pct = round(100 * usage.disk_free / usage.disk_total, 1)
        problems.append(f"data drive has only {free_pct}% free")
    if usage.wal_bytes is None:
        problems.append("cannot read WAL size (database user lacks pg_monitor)")
    if last_sample is None:
        problems.append("storage governor has never recorded a sample (is ingest running?)")
    elif now - last_sample > STALE_SAMPLE:
        problems.append(f"storage governor last sampled {_age(now - last_sample)} ago")
    if projection.days_to_full is not None and projection.days_to_full < 30:
        problems.append(
            f"storage projected to fill the budget in {projection.days_to_full:.0f} days"
        )
    return result


async def _streams(engine: AsyncEngine, now: datetime, problems: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for label, table in STREAMS.items():
        row = (
            await _rows(
                engine,
                f"SELECT max(ts) AS newest, count(*) FILTER (WHERE ts > :five) AS recent "
                f"FROM {table} WHERE ts > :hour",
                five=now - timedelta(minutes=5),
                hour=now - timedelta(hours=1),
            )
        )[0]
        newest = row["newest"]
        out[label] = {
            "newest": newest.isoformat() if newest else None,
            "age_seconds": (now - newest).total_seconds() if newest else None,
            "per_second_5min": round(row["recent"] / 300, 2),
        }
    for label in ("tickers", "trades"):  # the two that must never go quiet
        age = out[label]["age_seconds"]
        if age is None:
            problems.append(f"no {label} stored in the last hour")
        elif age > STALE_STREAM.total_seconds():
            problems.append(f"newest {label} row is {_age(timedelta(seconds=age))} old")
    return out


async def _gaps(engine: AsyncEngine, problems: list[str]) -> dict[str, Any]:
    recent = await _rows(
        engine,
        "SELECT id, started_at, ended_at, reason, status, trades_added, note "
        "FROM ingest_gaps ORDER BY id DESC LIMIT 5",
    )
    counts = await _rows(
        engine,
        "SELECT status, count(*) AS n FROM ingest_gaps "
        "WHERE created_at > now() - interval '24 hours' GROUP BY status",
    )
    by_status = {r["status"]: r["n"] for r in counts}
    for bad in ("failed", "interrupted", "running"):
        if by_status.get(bad):
            problems.append(f"{by_status[bad]} gap backfill(s) {bad} in the last 24 hours")
    return {"recent": recent, "last_24h": by_status}


async def _jobs(engine: AsyncEngine, problems: list[str]) -> dict[str, Any]:
    total = await _query_one(engine, "SELECT count(*) FROM timescaledb_information.jobs")
    bad = await _rows(
        engine,
        "SELECT j.proc_name, coalesce(j.hypertable_name, '') AS target, s.last_run_status, "
        "s.last_run_started_at, s.total_failures "
        "FROM timescaledb_information.job_stats s "
        "JOIN timescaledb_information.jobs j USING (job_id) "
        "WHERE s.last_run_status NOT IN ('Success', 'Running') OR s.total_failures > 0 "
        "ORDER BY j.job_id",
    )
    bad = unhealthy_jobs(bad)
    for job in bad:
        problems.append(
            f"job {job['proc_name']} {job['target']}: {job['last_run_status']}, "
            f"{job['total_failures']} failure(s) in total"
        )
    return {"total": total, "failing": bad}


# Timescale's own usage-report job; it cannot succeed without internet and is switched off
# in our database configuration, so it never counts as a problem.
IGNORED_JOBS = frozenset({"policy_telemetry"})


def unhealthy_jobs(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [job for job in jobs if job["proc_name"] not in IGNORED_JOBS]


async def _discovery(engine: AsyncEngine, now: datetime, problems: list[str]) -> dict[str, Any]:
    state = {
        r["key"]: r["value"] for r in await _rows(engine, "SELECT key, value FROM discovery_state")
    }
    out: dict[str, Any] = {}
    for key, label in (("updated_since", "incremental"), ("last_full_at", "full")):
        if key in state:
            when = datetime.fromtimestamp(int(state[key]), UTC)
            out[label] = {"at": when.isoformat(), "age_seconds": (now - when).total_seconds()}
    if "full" not in out:
        problems.append("discovery has never completed a full refresh")
    elif now - datetime.fromisoformat(out["full"]["at"]) > STALE_DISCOVERY:
        problems.append(
            f"last full discovery was {_age(now - datetime.fromisoformat(out['full']['at']))} ago"
        )
    placeholders = await _query_one(engine, "SELECT count(*) FROM markets WHERE status = 'unknown'")
    out["unresolved_placeholders"] = placeholders
    return out


async def _watchlist(engine: AsyncEngine) -> dict[str, Any]:
    open_now = await _query_one(
        engine, "SELECT count(*) FROM watchlist_periods WHERE removed_at IS NULL"
    )
    ever = await _query_one(engine, "SELECT count(DISTINCT market_id) FROM watchlist_periods")
    return {"watching_now": open_now, "ever_watched": ever}


# ------------------------------------------------------------------ rendering


def _age(delta: timedelta) -> str:
    seconds = int(delta.total_seconds())
    if seconds < 120:
        return f"{seconds}s"
    if seconds < 7200:
        return f"{seconds // 60}m"
    if seconds < 172_800:
        return f"{seconds // 3600}h"
    return f"{seconds // 86_400}d"


def _size(n: int | None) -> str:
    if n is None:
        return "?"
    for unit, step in (("GB", GB), ("MB", 1024**2), ("kB", 1024)):
        if n >= step:
            return f"{n / step:.1f} {unit}"
    return f"{n} B"


def render(report: dict[str, Any]) -> str:
    lines: list[str] = []
    problems = report["problems"]
    lines.append(f"KalshiTerm server status  {report['time']}")
    if problems:
        lines.append(f"\nNEEDS ATTENTION ({len(problems)})")
        lines += [f"  ! {p}" for p in problems]
    else:
        lines.append("\nAll checks passed.")

    s = report["storage"]
    lines.append(
        f"\nStorage  {_size(s['used_bytes'])} of {_size(s['budget_bytes'])} budget "
        f"({s['percent']}%)  mode: {s['mode']}"
    )
    wal = _size(s["wal_bytes"])
    lines.append(f"  data {_size(s['db_bytes'])} + WAL {wal}")
    if s["bytes_per_day"] is not None:
        days = (
            "not growing" if s["days_to_full"] is None else f"{s['days_to_full']:.0f} days to full"
        )
        lines.append(f"  growth {_size(int(s['bytes_per_day']))}/day, {days}")
    else:
        lines.append(f"  growth: {s['projection_note']}")
    disk = s["disk"]
    if disk["error"]:
        lines.append(f"  disk: ERROR {disk['error']}")
    elif disk["total"]:
        lines.append(f"  disk: {_size(disk['free'])} free of {_size(disk['total'])}")
    lines.append("  largest tables:")
    for name, size in s["tables"][:8]:
        lines.append(f"    {name:<26}{_size(size):>10}")
    lines.append(f"    {'(catalog, internals)':<26}{_size(s['other_bytes']):>10}")
    for event in s["recent_events"]:
        lines.append(f"  governor {event['ts']:%Y-%m-%d %H:%M}  {event['kind']}  {event['detail']}")

    host = report.get("host")
    if host:
        if host.get("reported"):
            free = host.get("free_pct")
            mounted = "mounted" if host.get("mounted") else "NOT MOUNTED"
            writable = "writable" if host.get("writable") else "NOT WRITABLE"
            lines.append(
                f"\nHost drive ({host.get('path')}): {mounted}, {writable}, "
                f"{'?' if free is None else free}% free, checked "
                f"{_age(timedelta(seconds=host['age_seconds']))} ago"
            )
        else:
            lines.append("\nHost drive: no report from the host-side check")

    lines.append("\nStreams (newest row, rows/s over 5 min)")
    for label, info in report["streams"].items():
        age = (
            "none in the last hour"
            if info["age_seconds"] is None
            else (f"{_age(timedelta(seconds=info['age_seconds']))} ago")
        )
        lines.append(f"  {label:<20}{age:<24}{info['per_second_5min']:>8}/s")

    gaps = report["gaps"]
    lines.append(f"\nGaps (last 24 h by status: {gaps['last_24h'] or 'none'})")
    for gap in gaps["recent"]:
        lines.append(
            f"  #{gap['id']} {gap['started_at']:%m-%d %H:%M} {gap['reason']:<16}{gap['status']:<12}"
            f"+{gap['trades_added']} trades {gap['note']}"
        )

    jobs = report["jobs"]
    lines.append(f"\nBackground jobs: {jobs['total']} registered, {len(jobs['failing'])} unhealthy")
    disc = report["discovery"]
    full = disc.get("full")
    inc = disc.get("incremental")
    lines.append(
        "Discovery: full "
        + (f"{_age(timedelta(seconds=full['age_seconds']))} ago" if full else "never")
        + ", incremental "
        + (f"{_age(timedelta(seconds=inc['age_seconds']))} ago" if inc else "never")
        + f", {disc['unresolved_placeholders']} unresolved placeholder(s)"
    )
    w = report["watchlist"]
    lines.append(f"Watchlist: {w['watching_now']} market(s) watched now, {w['ever_watched']} ever")
    return "\n".join(lines)
