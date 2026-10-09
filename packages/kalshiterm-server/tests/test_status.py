import json
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from kalshiterm_server import db
from kalshiterm_server.cli import app
from kalshiterm_server.status import collect_status, quick_health, render, unhealthy_jobs
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from typer.testing import CliRunner

pytestmark = pytest.mark.db


@pytest.fixture
async def engine(migrated_db_url: str) -> AsyncIterator[AsyncEngine]:
    engine = db.make_engine(migrated_db_url)
    yield engine
    await engine.dispose()


async def healthy(engine: AsyncEngine) -> None:
    """The state of a server whose ingest, discovery and governor are all working."""
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "insert into markets (ticker, event_ticker, market_type, status) "
                "values ('KXA-E1-X', 'KXA-E1', 'binary', 'active')"
            )
        )
        await conn.execute(
            text(
                "insert into tickers (ts, received_at, market_id, price_e6) "
                "select now() - (g || ' seconds')::interval, now(), id, 500000 "
                "from markets, generate_series(1, 30) g"
            )
        )
        await conn.execute(
            text(
                "insert into trades (ts, received_at, market_id, trade_id, yes_price_e6, count_e2)"
                " select now() - interval '5 seconds', now(), id, :t, 500000, 100 from markets"
            ),
            {"t": uuid.uuid4()},
        )
        now = int(datetime.now(UTC).timestamp())
        for key in ("updated_since", "last_full_at"):
            await conn.execute(
                text("insert into discovery_state (key, value) values (:k, :v)"),
                {"k": key, "v": str(now - 3600)},
            )
        await conn.execute(
            text(
                "insert into storage_samples (ts, db_bytes, used_bytes, tables) "
                "values (now(), 1, 1, '{}')"
            )
        )


async def test_a_fresh_empty_server_reports_what_is_missing_without_failing(
    engine: AsyncEngine, tmp_path: Path
) -> None:
    report = await collect_status(engine, 100, str(tmp_path))
    problems = " | ".join(report["problems"])
    assert "no tickers stored" in problems and "no trades stored" in problems
    assert "never recorded a sample" in problems
    assert "never completed a full refresh" in problems
    assert report["database"]["revision"] == report["database"]["head"]
    text_report = render(report)
    assert "NEEDS ATTENTION" in text_report and "Storage" in text_report


async def test_a_healthy_server_has_no_problems_and_prints_one_reassuring_line(
    engine: AsyncEngine, tmp_path: Path
) -> None:
    await healthy(engine)
    report = await collect_status(engine, 100, str(tmp_path))
    assert report["problems"] == []
    assert report["storage"]["mode"] == "normal" and report["storage"]["percent"] < 1
    assert report["streams"]["tickers"]["age_seconds"] < 60
    assert report["streams"]["tickers"]["per_second_5min"] == pytest.approx(0.1)
    assert report["watchlist"] == {
        "watching_now": 0,
        "by_source": {},
        "ever_watched": 0,
        "users_with_lists": 0,
        "markets_wanted_by_users": 0,
    }
    out = render(report)
    assert "All checks passed." in out and "NEEDS ATTENTION" not in out
    assert "largest tables:" in out and "Streams" in out and "Gaps" in out


async def test_stale_data_is_called_out(engine: AsyncEngine, tmp_path: Path) -> None:
    await healthy(engine)
    async with engine.begin() as conn:
        await conn.execute(text("update trades set ts = now() - interval '10 minutes'"))
    report = await collect_status(engine, 100, str(tmp_path))
    assert any("newest trades row is" in p for p in report["problems"])
    assert not any("tickers" in p for p in report["problems"])


async def test_failed_and_interrupted_gap_backfills_are_flagged(
    engine: AsyncEngine, tmp_path: Path
) -> None:
    await healthy(engine)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "insert into ingest_gaps (started_at, ended_at, reason, status, note) values "
                "(now(), now(), 'connection_lost', 'failed', 'rest down'), "
                "(now(), now(), 'startup', 'interrupted', '')"
            )
        )
    report = await collect_status(engine, 100, str(tmp_path))
    joined = " | ".join(report["problems"])
    assert "1 gap backfill(s) failed" in joined and "1 gap backfill(s) interrupted" in joined
    assert [g["status"] for g in report["gaps"]["recent"]] == ["interrupted", "failed"]


async def test_the_governors_mode_and_budget_pressure_are_reported(
    engine: AsyncEngine, tmp_path: Path
) -> None:
    await healthy(engine)
    tiny_budget = await collect_status(engine, 0.0001, str(tmp_path))  # ~100 kB: certainly over
    assert any("governor has not acted yet" in p for p in tiny_budget["problems"])

    async with engine.begin() as conn:
        await conn.execute(
            text("insert into governor_state (key, value) values ('mode', 'shedding')")
        )
        await conn.execute(
            text("insert into governor_events (kind, detail) values ('shed_deltas', '{}')")
        )
    report = await collect_status(engine, 100, str(tmp_path))
    assert any("governor is shedding" in p for p in report["problems"])
    assert report["storage"]["recent_events"][0]["kind"] == "shed_deltas"


async def test_a_missing_drive_is_a_problem(engine: AsyncEngine, tmp_path: Path) -> None:
    await healthy(engine)
    report = await collect_status(engine, 100, str(tmp_path / "not-mounted"))
    assert any("data drive check failed" in p for p in report["problems"])
    assert "disk: ERROR" in render(report)


def test_the_status_command_prints_a_report_and_exits_nonzero_when_something_is_wrong(
    migrated_db_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("KTERM_DB_URL", migrated_db_url)
    runner = CliRunner()
    result = runner.invoke(app, ["status"])
    assert result.exit_code == 1  # an empty server has problems
    assert "NEEDS ATTENTION" in result.output and "Storage" in result.output

    result = runner.invoke(app, ["status", "--json"])
    report = json.loads(result.output)
    assert result.exit_code == 1 and report["problems"]
    assert {"database", "storage", "streams", "gaps", "jobs", "discovery", "watchlist"} <= set(
        report
    )


def test_the_builtin_telemetry_job_never_counts_as_unhealthy() -> None:
    jobs = [
        {"proc_name": "policy_telemetry", "last_run_status": "Failed", "total_failures": 3},
        {"proc_name": "policy_compression", "last_run_status": "Failed", "total_failures": 1},
    ]
    assert [j["proc_name"] for j in unhealthy_jobs(jobs)] == ["policy_compression"]


def test_the_database_is_configured_not_to_send_telemetry() -> None:
    from conftest import COMPOSE_FILE

    assert "timescaledb.telemetry_level=off" in COMPOSE_FILE.read_text()


async def test_the_quick_health_check_is_ok_when_data_is_flowing(engine: AsyncEngine) -> None:
    await healthy(engine)
    assert await quick_health(engine) == []


async def test_the_quick_health_check_names_what_is_wrong(engine: AsyncEngine) -> None:
    empty = await quick_health(engine)
    assert "no tickers stored in the last 5 minutes" in empty
    assert "no trades stored in the last 5 minutes" in empty
    await healthy(engine)
    async with engine.begin() as conn:
        await conn.execute(text("update trades set ts = now() - interval '20 minutes'"))
        await conn.execute(text("update alembic_version set version_num = '0001'"))
    problems = await quick_health(engine)
    assert any("revision 0001" in p for p in problems)
    assert any("no trades" in p for p in problems) and not any("tickers" in p for p in problems)


async def test_an_unreachable_database_is_unhealthy_not_an_exception() -> None:
    engine = db.make_engine("postgresql+asyncpg://nobody:x@127.0.0.1:9/none")
    try:
        problems = await quick_health(engine)
    finally:
        await engine.dispose()
    assert problems and problems[0].startswith("database unreachable")


async def test_status_includes_the_host_drive_report_when_configured(
    engine: AsyncEngine, tmp_path: Path
) -> None:
    await healthy(engine)
    state = tmp_path / "host.json"
    state.write_text(
        json.dumps(
            {
                "checked_at": int(datetime.now(UTC).timestamp()),
                "path": "/Volumes/KalshiData",
                "mounted": False,
                "writable": False,
                "free_pct": None,
                "error": "/Volumes/KalshiData does not exist",
            }
        )
    )
    report = await collect_status(engine, 100, str(tmp_path), host_state_file=str(state))
    assert any("NOT MOUNTED" in p for p in report["problems"])
    assert report["host"]["reported"] is True
    assert "Host drive (/Volumes/KalshiData): NOT MOUNTED" in render(report)
    unconfigured = await collect_status(engine, 100, str(tmp_path))
    assert unconfigured["host"] is None and unconfigured["problems"] == []
