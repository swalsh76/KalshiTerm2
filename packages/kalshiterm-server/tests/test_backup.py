"""Backups against a real TimescaleDB: real ``pg_dump`` and ``pg_restore`` (run inside the
database container, because the host may not have a client as new as the server)."""

import json
import os
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from kalshiterm_server import db
from kalshiterm_server.backup import (
    MARKER,
    BackupError,
    Conn,
    PgTools,
    backup_loop,
    check_target,
    compare_restored,
    deep_verify,
    init_target,
    list_backups,
    restore_backup,
    run_backup,
    verify_file,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from test_stream import scalar
from testcontainers.community.postgres import PostgresContainer
from timescale_jobs import quiet_background_jobs

pytestmark = pytest.mark.db


class ContainerTools(PgTools):
    """Runs the client programs inside the test container, over its own socket."""

    def __init__(self, container: PostgresContainer) -> None:
        self.container = container.get_wrapped_container().id

    def command(
        self, program: str, args: list[str], env: dict[str, str]
    ) -> tuple[list[str], dict[str, str]]:
        inside = list(args)
        for flag, value in (("-h", "localhost"), ("-p", "5432")):
            if flag in inside:
                inside[inside.index(flag) + 1] = value
        flags = [item for k, v in env.items() for item in ("-e", f"{k}={v}")]
        return ["docker", "exec", "-i", *flags, self.container, program, *inside], dict(os.environ)


@pytest.fixture
def tools(timescale_container: PostgresContainer) -> ContainerTools:
    return ContainerTools(timescale_container)


@pytest.fixture
async def restored_name(migrated_db_url: str, engine: AsyncEngine) -> AsyncIterator[str]:
    """A fresh database name; whatever a test restores into it is dropped afterwards."""
    name = f"kterm_r_{uuid.uuid4().hex[:10]}"
    yield name
    admin = create_async_engine(
        migrated_db_url.rsplit("/", 1)[0] + "/postgres", isolation_level="AUTOCOMMIT"
    )
    async with admin.connect() as conn:
        await conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
    await admin.dispose()


@pytest.fixture
async def engine(migrated_db_url: str) -> AsyncIterator[AsyncEngine]:
    engine = db.make_engine(migrated_db_url)
    await quiet_background_jobs(engine)
    yield engine
    await engine.dispose()


@pytest.fixture
def target(tmp_path: Path) -> Path:
    path = tmp_path / "nas"
    path.mkdir()
    init_target(path)
    return path


async def seed(engine: AsyncEngine) -> None:
    """Rows in plain tables and hypertables, one compressed chunk, users with tokens."""
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO events (event_ticker, series_ticker, title, mutually_exclusive) "
                "VALUES ('KX-E1', 'KX', 'An event', true)"
            )
        )
        for i in range(25):
            await conn.execute(
                text(
                    "INSERT INTO markets (ticker, event_ticker, market_type, status, rules_primary)"
                    " VALUES (:t, 'KX-E1', 'binary', 'active', :r)"
                ),
                {"t": f"KX-E1-M{i:02d}", "r": "rules " * 50},
            )
        await conn.execute(text("INSERT INTO users (name) VALUES ('alice')"))
        old = datetime.now(UTC) - timedelta(days=4)
        for i in range(300):
            await conn.execute(
                text(
                    "INSERT INTO trades (ts, received_at, market_id, trade_id, yes_price_e6, "
                    "count_e2) VALUES (:ts, :ts, 1 + :m, :id, 500000, 100)"
                ),
                {"ts": old + timedelta(seconds=i), "m": i % 25, "id": uuid.uuid4()},
            )
        await conn.execute(text("SELECT compress_chunk(c, true) FROM show_chunks('trades') c"))
        await conn.execute(
            text(
                "INSERT INTO ingest_progress (id, last_seq) VALUES (true, 7) "
                "ON CONFLICT (id) DO UPDATE SET last_seq = 7"
            )
        )


def listing(directory: Path) -> list[str]:
    return sorted(p.name for p in directory.iterdir())


async def test_a_backup_is_a_verified_dump_with_a_manifest_and_a_record(
    migrated_db_url: str, engine: AsyncEngine, tools: ContainerTools, target: Path
) -> None:
    await seed(engine)
    result = await run_backup(engine, tools, migrated_db_url, target)
    assert result.path.parent == target and result.path.name.startswith("kterm-")
    assert result.path.exists() and result.size > 1000
    manifest = json.loads((target / (result.path.name + ".json")).read_text())
    assert manifest["tables"]["markets"] == 25 and manifest["tables"]["users"] == 1
    assert (
        manifest["compressed_chunks"] >= 1 and manifest["jobs"] > 0 and manifest["aggregates"] == 4
    )
    assert manifest["revision"] == db.head_revision() and manifest["last_seq"] == 7
    assert verify_file(tools, migrated_db_url, result.path)["sha256"] == manifest["sha256"]
    assert listing(target) == sorted(
        [MARKER, result.path.name, result.path.name + ".json"]
    )  # nothing else is left behind
    recorded = await scalar(
        migrated_db_url, "select status || ':' || file from backup_runs order by id desc limit 1"
    )
    assert recorded == f"ok:{result.path.name}"


async def test_a_restore_reproduces_the_database_including_compression_jobs_and_aggregates(
    migrated_db_url: str,
    engine: AsyncIterator[AsyncEngine],
    tools: ContainerTools,
    target: Path,
    restored_name: str,
) -> None:
    await seed(engine)  # type: ignore[arg-type]
    result = await run_backup(engine, tools, migrated_db_url, target)  # type: ignore[arg-type]
    manifest = await restore_backup(tools, migrated_db_url, result.path, restored_name)
    assert await compare_restored(migrated_db_url, restored_name, manifest) == []
    restored = migrated_db_url.rsplit("/", 1)[0] + "/" + restored_name
    assert await scalar(restored, "select count(*) from trades") == 300
    assert (
        await scalar(
            restored, "select count(*) from timescaledb_information.chunks where is_compressed"
        )
        >= 1
    )
    assert await scalar(restored, "select last_seq from ingest_progress") == 7
    # the sequence behind the push cursor continues from where it was, not from 1
    assert await scalar(restored, "select nextval('ingest_seq')") >= 1


async def test_the_restore_matches_the_moment_of_the_backup_not_the_live_database(
    migrated_db_url: str,
    engine: AsyncEngine,
    tools: ContainerTools,
    target: Path,
    restored_name: str,
) -> None:
    await seed(engine)
    result = await run_backup(engine, tools, migrated_db_url, target)
    async with engine.begin() as conn:  # the live database moves on afterwards
        await conn.execute(text("INSERT INTO users (name) VALUES ('bob')"))
    manifest = await restore_backup(tools, migrated_db_url, result.path, restored_name)
    restored = migrated_db_url.rsplit("/", 1)[0] + "/" + restored_name
    assert await scalar(restored, "select count(*) from users") == 1
    assert await scalar(migrated_db_url, "select count(*) from users") == 2
    assert await compare_restored(migrated_db_url, restored_name, manifest) == []


async def test_a_deep_verification_restores_into_a_scratch_database_and_cleans_up(
    migrated_db_url: str, engine: AsyncEngine, tools: ContainerTools, target: Path
) -> None:
    await seed(engine)
    result = await run_backup(engine, tools, migrated_db_url, target)
    assert await deep_verify(tools, migrated_db_url, result.path) == []
    leftovers = await scalar(
        migrated_db_url, "select count(*) from pg_database where datname like 'kterm_verify_%'"
    )
    assert leftovers == 0


async def test_damage_to_a_backup_is_detected(
    migrated_db_url: str,
    engine: AsyncEngine,
    tools: ContainerTools,
    target: Path,
    restored_name: str,
) -> None:
    await seed(engine)
    path = (await run_backup(engine, tools, migrated_db_url, target)).path
    original = path.read_bytes()

    path.write_bytes(original[:-100])  # a truncated copy
    with pytest.raises(BackupError, match="size differs"):
        verify_file(tools, migrated_db_url, path)

    flipped = bytearray(original)
    flipped[len(flipped) // 2] ^= 0xFF  # one bit of rot in the middle
    path.write_bytes(bytes(flipped))
    with pytest.raises(BackupError, match="checksum differs"):
        verify_file(tools, migrated_db_url, path)

    path.write_bytes(original)
    (target / (path.name + ".json")).unlink()  # a lost manifest
    with pytest.raises(BackupError, match="manifest|missing"):
        verify_file(tools, migrated_db_url, path)
    with pytest.raises(BackupError):
        await restore_backup(tools, migrated_db_url, path, restored_name)  # and no restore either


async def test_a_restore_never_overwrites_an_existing_database(
    migrated_db_url: str,
    engine: AsyncEngine,
    tools: ContainerTools,
    target: Path,
    restored_name: str,
) -> None:
    await seed(engine)
    path = (await run_backup(engine, tools, migrated_db_url, target)).path
    live = migrated_db_url.rsplit("/", 1)[1]
    with pytest.raises(BackupError, match="already exists"):
        await restore_backup(tools, migrated_db_url, path, live)  # the live database, by name
    await restore_backup(tools, migrated_db_url, path, restored_name)
    with pytest.raises(BackupError, match="already exists"):
        await restore_backup(tools, migrated_db_url, path, restored_name)  # nor a restored one
    with pytest.raises(BackupError, match="not empty"):
        await restore_backup(tools, migrated_db_url, path, live, create=False)  # nor by --no-create
    assert await scalar(migrated_db_url, "select count(*) from markets") == 25  # untouched


async def test_a_missing_marker_fails_loudly_and_is_recorded_without_writing_anything(
    migrated_db_url: str, engine: AsyncEngine, tools: ContainerTools, tmp_path: Path
) -> None:
    unmounted = tmp_path / "unmounted"  # an ordinary local directory: what a missing NAS looks like
    unmounted.mkdir()
    with pytest.raises(BackupError, match="NOT the NAS"):
        await run_backup(engine, tools, migrated_db_url, unmounted)
    assert list(unmounted.iterdir()) == []
    row = await scalar(
        migrated_db_url, "select status || ' | ' || error from backup_runs order by id desc limit 1"
    )
    assert row.startswith("failed | ") and "marker" in row


async def test_a_dump_that_dies_leaves_no_partial_file_and_records_the_failure(
    migrated_db_url: str, engine: AsyncEngine, tools: ContainerTools, target: Path
) -> None:
    class Dying(ContainerTools):
        def dump(self, conn: Conn, snapshot: str, out: Path) -> None:
            out.write_bytes(b"half a dump")
            raise BackupError("pg_dump failed (1): connection reset")

    with pytest.raises(BackupError, match="connection reset"):
        await run_backup(engine, Dying.__new__(Dying), migrated_db_url, target)
    assert listing(target) == [MARKER]  # no partial, no manifest
    row = await scalar(
        migrated_db_url, "select status || ' | ' || error from backup_runs order by id desc limit 1"
    )
    assert row.startswith("failed | ") and "connection reset" in row


async def test_an_empty_looking_dump_is_refused(
    migrated_db_url: str, engine: AsyncEngine, tools: ContainerTools, target: Path
) -> None:
    class Hollow(ContainerTools):
        def table_of_contents(self, conn: Conn, path: Path) -> str:
            return "; Archive created\n"  # a listing with no table data in it

    hollow = Hollow.__new__(Hollow)
    hollow.container = tools.container
    with pytest.raises(BackupError, match="no table data"):
        await run_backup(engine, hollow, migrated_db_url, target)
    assert listing(target) == [MARKER]


async def test_retention_runs_with_each_backup_and_spares_foreign_files(
    migrated_db_url: str, engine: AsyncEngine, tools: ContainerTools, target: Path
) -> None:
    await seed(engine)
    for week in range(1, 6):  # five old backups, each in a different week
        stamp = datetime.now(UTC) - timedelta(days=20 + 7 * week)
        (target / f"kterm-{stamp:%Y%m%d-%H%M%S}.dump").write_bytes(b"x")
        (target / f"kterm-{stamp:%Y%m%d-%H%M%S}.dump.json").write_text("{}")
    (target / "notes.txt").write_text("not ours")
    (target / ".partial-20200101-000000.dump").write_bytes(b"crash leftover")
    result = await run_backup(engine, tools, migrated_db_url, target)
    names = listing(target)
    assert "notes.txt" in names and ".partial-20200101-000000.dump" not in names
    assert result.path.name in names
    assert len(result.deleted) == 1  # four weeks are kept of the old ones; the fifth went
    assert len([n for n in names if n.endswith(".dump")]) == 5  # the new one + four weekly


async def test_the_dump_listing_shows_sizes_and_manifests(
    migrated_db_url: str, engine: AsyncEngine, tools: ContainerTools, target: Path
) -> None:
    await seed(engine)
    result = await run_backup(engine, tools, migrated_db_url, target)
    [entry] = list_backups(target)
    assert entry["file"] == result.path.name and entry["size"] == result.size
    assert entry["manifest"]["tables"]["markets"] == 25


async def test_the_schedule_waits_for_the_time_of_day_retries_failures_and_goes_on(
    migrated_db_url: str, engine: AsyncEngine, tools: ContainerTools, target: Path
) -> None:
    await seed(engine)
    clock = {"now": datetime(2026, 10, 9, 5, 0, tzinfo=UTC)}
    slept: list[float] = []
    attempts = {"n": 0}
    real_dump = tools.dump

    def flaky(conn: Conn, snapshot: str, out: Path) -> None:
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise BackupError("the NAS dropped off the network")
        real_dump(conn, snapshot, out)

    tools.dump = flaky  # type: ignore[method-assign]

    async def sleep(seconds: float) -> None:
        slept.append(seconds)
        clock["now"] += timedelta(seconds=seconds)

    await backup_loop(
        engine, tools, migrated_db_url, target, at="07:00", keep_daily=7, keep_weekly=4,
        retry_minutes=30, sleep=sleep, now=lambda: clock["now"], iterations=1,
    )  # fmt: skip
    assert slept == [2 * 3600, 30 * 60]  # until 07:00, then 30 minutes after the failure
    assert attempts["n"] == 2 and len(list_backups(target)) == 1
    statuses = [
        r[0] for r in await _rows(migrated_db_url, "select status from backup_runs order by id")
    ]
    assert statuses == ["failed", "ok"]


async def _rows(url: str, sql: str) -> list[tuple[Any, ...]]:
    from test_stream import rows

    return await rows(url, sql)


def test_the_check_target_helper_is_what_run_backup_relies_on(tmp_path: Path) -> None:
    with pytest.raises(BackupError):
        check_target(tmp_path)
