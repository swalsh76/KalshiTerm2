"""Backups: ``pg_dump`` to a target directory (the NAS), with retention and verification.

Design (PLAN §9.5, measured 2026-10-09 on a real database: a 952 MB database dumped in 4 s to
52 MB and restored in 10 s with compressed chunks, aggregates and jobs intact):

* the dump runs under an **exported snapshot**, and row counts of the small tables are taken in
  the same snapshot, so each dump has a manifest that a restore can be checked against;
* the target must hold a **marker file**: an unmounted NAS is just an empty local directory,
  and writing backups there would silently fill the host's disk;
* a dump is written under a temporary name and renamed only once it reads back, so a crash
  never leaves something that looks like a backup but is not;
* every outcome is recorded in ``backup_runs`` so ``status`` can shout when backups stop;
* a restore always goes into a database you name, never over the live one by accident.
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

log = logging.getLogger("kalshiterm_server")

MARKER = ".kterm-backup-target"
NAME = re.compile(r"^kterm-(\d{8})-(\d{6})\.dump$")
SMALL_TABLES = (
    "series", "events", "markets", "users", "api_tokens", "user_watchlists",
    "watchlist_periods", "pins", "ingest_gaps", "discovery_state", "governor_state",
    "combo_stats_1m", "market_lifecycle",
)  # fmt: skip


class BackupError(Exception):
    """Something the operator must fix; the message says what."""


# ---------------------------------------------------------------- the tools


@dataclass(frozen=True, slots=True)
class Conn:
    host: str
    port: int
    user: str
    password: str
    database: str

    @classmethod
    def from_url(cls, url: str, database: str | None = None) -> "Conn":
        u = make_url(url)
        return cls(
            u.host or "localhost", u.port or 5432, u.username or "", u.password or "",
            database or u.database or "postgres",
        )  # fmt: skip

    def args(self) -> list[str]:
        return ["-h", self.host, "-p", str(self.port), "-U", self.user, "-d", self.database]

    def env(self) -> dict[str, str]:
        return {"PGPASSWORD": self.password}  # never on a command line, where `ps` shows it


class PgTools:
    """Runs the PostgreSQL client programs. Tests substitute a subclass that runs them inside a
    database container (the host may not have a client as new as the server)."""

    def command(
        self, program: str, args: list[str], env: dict[str, str]
    ) -> tuple[list[str], dict[str, str]]:
        return [program, *args], {**os.environ, **env}

    def run(
        self, program: str, args: list[str], conn: Conn, *, stdout: Any = None, stdin: Any = None
    ) -> subprocess.CompletedProcess[bytes]:
        cmd, env = self.command(program, args, conn.env())
        done = subprocess.run(
            cmd, env=env, stdout=stdout or subprocess.PIPE, stdin=stdin, stderr=subprocess.PIPE
        )
        if done.returncode != 0:
            raise BackupError(
                f"{program} failed ({done.returncode}): "
                f"{done.stderr.decode(errors='replace')[-600:]}"
            )
        return done

    def dump(self, conn: Conn, snapshot: str, out: Path) -> None:
        args = [*conn.args(), "--format=custom", f"--snapshot={snapshot}", "--no-owner"]
        with out.open("wb") as f:
            self.run("pg_dump", args, conn, stdout=f)
            f.flush()
            os.fsync(f.fileno())

    def table_of_contents(self, conn: Conn, path: Path) -> str:
        with path.open("rb") as f:
            return self.run("pg_restore", ["--list"], conn, stdin=f).stdout.decode(errors="replace")

    def restore(self, conn: Conn, path: Path) -> None:
        with path.open("rb") as f:
            self.run("pg_restore", [*conn.args(), "--no-owner"], conn, stdin=f)


# ---------------------------------------------------------------- retention (pure)


def stamp_of(name: str) -> datetime | None:
    match = NAME.match(name)
    if not match:
        return None
    return datetime.strptime(match.group(1) + match.group(2), "%Y%m%d%H%M%S").replace(tzinfo=UTC)


def plan_retention(
    names: list[str], now: datetime, daily: int = 7, weekly: int = 4
) -> tuple[list[str], list[str]]:
    """(keep, delete). Keeps the newest backup of each of the last ``daily`` days and, beyond
    those, the newest of each of the last ``weekly`` weeks. The newest backup is never deleted,
    and files that are not ours are never touched."""
    dated = sorted(((stamp_of(n), n) for n in names if stamp_of(n)), key=lambda x: x[0])  # type: ignore[arg-type,return-value]
    keep: set[str] = set()
    newest_of_day: dict[date, str] = {}
    newest_of_week: dict[tuple[int, int], str] = {}
    for when, name in dated:
        assert when is not None
        newest_of_day[when.date()] = name
        iso = when.isocalendar()
        newest_of_week[(iso.year, iso.week)] = name
    today = now.date()
    for day, name in newest_of_day.items():
        if 0 <= (today - day).days < daily:
            keep.add(name)
    cutoff = today - timedelta(days=daily)
    older_weeks = {
        week: name
        for week, name in newest_of_week.items()
        if (stamp_of(name) or now).date() <= cutoff
    }
    for _, name in sorted(older_weeks.items(), reverse=True)[:weekly]:
        keep.add(name)
    if dated:
        keep.add(dated[-1][1])
    ours = [n for _, n in dated]
    return [n for n in ours if n in keep], [n for n in ours if n not in keep]


# ---------------------------------------------------------------- the target


def init_target(path: Path) -> None:
    """Mark a directory as the backup target. Run once, with the NAS mounted."""
    if not path.is_dir():
        raise BackupError(f"{path} is not a directory (is the NAS mounted?)")
    marker = path / MARKER
    if not marker.exists():
        marker.write_text("kalshiterm backup target\n")


def check_target(path: Path, needed_bytes: int = 0) -> None:
    if not path.is_dir():
        raise BackupError(f"backup target {path} does not exist (is the NAS mounted?)")
    if not (path / MARKER).exists():
        raise BackupError(
            f"{path} has no {MARKER} marker: it is probably NOT the NAS (unmounted?). "
            "Mount it, then run `kterm-server backup init-target` once."
        )
    try:
        probe = path / f".probe-{os.getpid()}"
        probe.write_bytes(b"ok")
        probe.unlink()
    except OSError as exc:
        raise BackupError(f"backup target {path} is not writable: {exc}") from exc
    free = shutil.disk_usage(path).free
    if needed_bytes and free < needed_bytes:
        raise BackupError(
            f"not enough space on {path}: {free // 2**20} MB free, "
            f"about {needed_bytes // 2**20} MB needed"
        )


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(8 * 2**20):
            digest.update(chunk)
    return digest.hexdigest()


def _write_atomic(path: Path, content: str) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


# ---------------------------------------------------------------- running a backup


@dataclass(slots=True)
class BackupResult:
    path: Path
    size: int
    seconds: float
    manifest: dict[str, Any]
    deleted: list[str] = field(default_factory=list)


async def _counts(conn: Any) -> dict[str, Any]:
    """Row counts and structure, read in the dump's own snapshot."""
    counts = {}
    for table in SMALL_TABLES:
        counts[table] = (await conn.execute(text(f"SELECT count(*) FROM {table}"))).scalar_one()
    structure = (
        await conn.execute(
            text(
                "SELECT (SELECT version_num FROM alembic_version), "
                "(SELECT count(*) FROM timescaledb_information.hypertables), "
                "(SELECT count(*) FROM timescaledb_information.chunks), "
                "(SELECT count(*) FROM timescaledb_information.chunks WHERE is_compressed), "
                "(SELECT count(*) FROM timescaledb_information.jobs), "
                "(SELECT count(*) FROM timescaledb_information.continuous_aggregates), "
                "(SELECT coalesce(last_seq, 0) FROM ingest_progress)"
            )
        )
    ).one()
    keys = (
        "revision",
        "hypertables",
        "chunks",
        "compressed_chunks",
        "jobs",
        "aggregates",
        "last_seq",
    )
    return {"tables": counts, **dict(zip(keys, structure, strict=True))}


async def run_backup(
    engine: AsyncEngine,
    tools: PgTools,
    db_url: str,
    target: Path,
    *,
    keep_daily: int = 7,
    keep_weekly: int = 4,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> BackupResult:
    started = now()
    run_id = await _record_start(engine, started)
    partial = target / f".partial-{started:%Y%m%d-%H%M%S}.dump"
    try:
        last = await _last_size(engine)
        check_target(target, int(last * 1.2) if last else 0)
        final = target / f"kterm-{started:%Y%m%d-%H%M%S}.dump"
        if final.exists():
            raise BackupError(f"{final.name} already exists (two runs in the same second?)")
        async with engine.connect() as conn:
            await conn.execution_options(isolation_level="REPEATABLE READ")
            snapshot = (await conn.execute(text("SELECT pg_export_snapshot()"))).scalar_one()
            manifest = await _counts(conn)  # in the snapshot the dump is about to use
            await asyncio.to_thread(tools.dump, Conn.from_url(db_url), snapshot, partial)
        toc = await asyncio.to_thread(tools.table_of_contents, Conn.from_url(db_url), partial)
        if "TABLE DATA" not in toc:
            raise BackupError("the dump has no table data: refusing to keep it")
        size = partial.stat().st_size
        manifest |= {
            "file": final.name,
            "created_at": started.isoformat(),
            "size_bytes": size,
            "sha256": await asyncio.to_thread(sha256_of, partial),
            "format": "pg_dump custom",
        }
        _write_atomic(
            target / (final.name + ".json"), json.dumps(manifest, indent=2, sort_keys=True)
        )
        os.replace(partial, final)
        _fsync_dir(target)
        seconds = (now() - started).total_seconds()
        deleted = prune(target, now(), keep_daily, keep_weekly)
        await _record_finish(engine, run_id, "ok", final.name, size, seconds, "")
        return BackupResult(final, size, seconds, manifest, deleted)
    except BaseException as exc:
        partial.unlink(missing_ok=True)
        message = str(exc) if isinstance(exc, BackupError) else f"{type(exc).__name__}: {exc}"
        await asyncio.shield(_record_finish(engine, run_id, "failed", None, None, None, message))
        raise


def prune(target: Path, now: datetime, keep_daily: int, keep_weekly: int) -> list[str]:
    names = [p.name for p in target.iterdir() if NAME.match(p.name)]
    _, doomed = plan_retention(names, now, keep_daily, keep_weekly)
    for name in doomed:
        (target / name).unlink(missing_ok=True)
        (target / (name + ".json")).unlink(missing_ok=True)
        log.info("backup retention: removed %s", name)
    for stale in target.glob(".partial-*"):  # left by a crash: never a backup
        stale.unlink(missing_ok=True)
    return doomed


def list_backups(target: Path) -> list[dict[str, Any]]:
    found = []
    for path in sorted(target.iterdir()):
        if NAME.match(path.name):
            manifest_path = target / (path.name + ".json")
            manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
            found.append({"file": path.name, "size": path.stat().st_size, "manifest": manifest})
    return found


# ---------------------------------------------------------------- verifying and restoring


def verify_file(tools: PgTools, db_url: str, path: Path) -> dict[str, Any]:
    """Cheap check that a backup on the target is intact: checksum and a readable contents list."""
    manifest_path = path.with_name(path.name + ".json")
    if not manifest_path.exists():
        raise BackupError(f"{manifest_path.name} is missing")
    manifest: dict[str, Any] = json.loads(manifest_path.read_text())
    if path.stat().st_size != manifest["size_bytes"]:
        raise BackupError(f"{path.name}: size differs from its manifest (truncated copy?)")
    if sha256_of(path) != manifest["sha256"]:
        raise BackupError(f"{path.name}: checksum differs from its manifest (corrupted?)")
    if "TABLE DATA" not in tools.table_of_contents(Conn.from_url(db_url), path):
        raise BackupError(f"{path.name}: unreadable contents list")
    return manifest


def _admin_engine(db_url: str, database: str = "postgres") -> AsyncEngine:
    url = make_url(db_url).set(database=database)
    return create_async_engine(url, isolation_level="AUTOCOMMIT")


def check_database_name(name: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", name):
        raise BackupError("database names use lowercase letters, digits and underscores")
    return name


async def restore_backup(
    tools: PgTools, db_url: str, path: Path, database: str, *, create: bool = True
) -> dict[str, Any]:
    """Restore ``path`` into ``database`` (created if asked; otherwise it must be empty).

    Follows TimescaleDB's procedure (pre_restore, pg_restore, post_restore). Never targets the
    live database unless you name it, and then only if it holds no tables.
    """
    check_database_name(database)
    manifest = verify_file(tools, db_url, path)
    admin = _admin_engine(db_url)
    try:
        async with admin.connect() as conn:
            exists = (
                await conn.execute(
                    text("SELECT 1 FROM pg_database WHERE datname = :d"), {"d": database}
                )
            ).first()
            if exists and create:
                raise BackupError(f"database {database} already exists: choose another name")
            if not exists:
                if not create:
                    raise BackupError(f"database {database} does not exist (use --create)")
                await conn.execute(text(f'CREATE DATABASE "{database}"'))
    finally:
        await admin.dispose()
    target = create_async_engine(
        make_url(db_url).set(database=database), isolation_level="AUTOCOMMIT"
    )
    try:
        async with target.connect() as conn:
            tables = (
                await conn.execute(
                    text("SELECT count(*) FROM pg_tables WHERE schemaname = 'public'")
                )
            ).scalar_one()
            if tables:
                raise BackupError(
                    f"database {database} is not empty ({tables} tables): "
                    "refusing to restore into it"
                )
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))
            await conn.execute(text("SELECT timescaledb_pre_restore()"))
        await asyncio.to_thread(tools.restore, Conn.from_url(db_url, database), path)
        async with target.connect() as conn:
            await conn.execute(text("SELECT timescaledb_post_restore()"))
    finally:
        await target.dispose()
    return manifest


async def compare_restored(db_url: str, database: str, manifest: dict[str, Any]) -> list[str]:
    """Differences between a restored database and the manifest of the dump it came from."""
    engine = create_async_engine(make_url(db_url).set(database=database))
    try:
        async with engine.connect() as conn:
            now = await _counts(conn)
    finally:
        await engine.dispose()
    problems = [
        f"{table}: dump had {manifest['tables'][table]} rows, restore has {n}"
        for table, n in now["tables"].items()
        if n != manifest["tables"][table]
    ]
    for key in (
        "revision",
        "hypertables",
        "chunks",
        "compressed_chunks",
        "jobs",
        "aggregates",
        "last_seq",
    ):
        if now[key] != manifest[key]:
            problems.append(f"{key}: dump had {manifest[key]}, restore has {now[key]}")
    return problems


async def deep_verify(tools: PgTools, db_url: str, path: Path) -> list[str]:
    """Restore into a scratch database, compare with the manifest, drop it. Needs free disk
    about the size of the database; returns the list of problems (empty = restorable)."""
    scratch = f"kterm_verify_{datetime.now(UTC):%Y%m%d%H%M%S}"
    manifest = await restore_backup(tools, db_url, path, scratch)
    try:
        return await compare_restored(db_url, scratch, manifest)
    finally:
        admin = _admin_engine(db_url)
        async with admin.connect() as conn:
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{scratch}" WITH (FORCE)'))
        await admin.dispose()


# ---------------------------------------------------------------- bookkeeping


async def _record_start(engine: AsyncEngine, when: datetime) -> int:
    async with engine.begin() as conn:
        run_id: int = (
            await conn.execute(
                text("INSERT INTO backup_runs (started_at) VALUES (:t) RETURNING id"), {"t": when}
            )
        ).scalar_one()
    return run_id


async def _record_finish(
    engine: AsyncEngine, run_id: int, status: str, file: str | None, size: int | None,
    seconds: float | None, error: str,
) -> None:  # fmt: skip
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE backup_runs SET finished_at = now(), status = :s, file = :f, "
                "size_bytes = :b, seconds = :sec, error = :e WHERE id = :i"
            ),
            {"s": status, "f": file, "b": size, "sec": seconds, "e": error[-1000:], "i": run_id},
        )


async def _last_size(engine: AsyncEngine) -> int:
    async with engine.connect() as conn:
        found = (
            await conn.execute(
                text(
                    "SELECT size_bytes FROM backup_runs WHERE status = 'ok' "
                    "ORDER BY id DESC LIMIT 1"
                )
            )
        ).scalar_one_or_none()
    return int(found or 0)


# ---------------------------------------------------------------- the schedule


def next_run(now: datetime, at: str) -> datetime:
    """The next occurrence of HH:MM (UTC) strictly after ``now``."""
    hour, minute = (int(x) for x in at.split(":"))
    candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return candidate if candidate > now else candidate + timedelta(days=1)


async def backup_loop(
    engine: AsyncEngine,
    tools: PgTools,
    db_url: str,
    target: Path,
    *,
    at: str,
    keep_daily: int,
    keep_weekly: int,
    retry_minutes: float = 30,
    sleep: Callable[[float], Any] = asyncio.sleep,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    iterations: int | None = None,
) -> None:
    """Back up daily at ``at`` (UTC); after a failure keep retrying until it works."""
    done = 0
    while iterations is None or done < iterations:
        wait = (next_run(now(), at) - now()).total_seconds()
        await sleep(max(0.0, wait))
        while True:
            try:
                result = await run_backup(
                    engine,
                    tools,
                    db_url,
                    target,
                    keep_daily=keep_daily,
                    keep_weekly=keep_weekly,
                    now=now,
                )
                log.info(
                    "backup ok: %s (%d MB, %.0f s)",
                    result.path.name,
                    result.size // 2**20,
                    result.seconds,
                )
                break
            except Exception:
                log.exception("BACKUP FAILED; retrying in %.0f minutes", retry_minutes)
                await sleep(retry_minutes * 60)
        done += 1
