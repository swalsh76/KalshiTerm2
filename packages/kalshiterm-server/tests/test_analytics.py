"""The analyzer runner against a real TimescaleDB."""

import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta

import asyncpg  # type: ignore[import-untyped]
import pytest
from kalshiterm_server import db
from kalshiterm_server.analytics.base import AnalysisContext, Result
from kalshiterm_server.analytics.registry import LoadFailure
from kalshiterm_server.analytics.runner import CHANNEL, AnalysisRunner
from kalshiterm_server.cli import app
from kalshiterm_server.status import collect_status, render
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from test_stream import rows, scalar
from timescale_jobs import quiet_background_jobs
from typer.testing import CliRunner

pytestmark = pytest.mark.db


@pytest.fixture
async def engine(migrated_db_url: str) -> AsyncIterator[AsyncEngine]:
    engine = db.make_engine(migrated_db_url)
    await quiet_background_jobs(engine)
    yield engine
    await engine.dispose()


class Fake:
    """An analyzer whose behaviour a test scripts."""

    interval = timedelta(minutes=1)
    timeout = timedelta(seconds=5)

    def __init__(self, name: str = "fake", results: Sequence[object] | None = None) -> None:
        self.name = name
        self.results = (
            [Result("spread.cents", 2_000_000, market_id=1)] if results is None else results
        )
        self.calls = 0
        self.error: Exception | None = None
        self.delay = 0.0
        self.contexts: list[AnalysisContext] = []

    async def run(self, ctx: AnalysisContext) -> Sequence[Result]:
        self.calls += 1
        self.contexts.append(ctx)
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.error:
            raise self.error
        return self.results  # type: ignore[return-value]


def runner_for(engine: AsyncEngine, *analyzers: Fake, **kw: object) -> AnalysisRunner:
    return AnalysisRunner(engine, {a.name: a for a in analyzers}, **kw)  # type: ignore[arg-type]


async def last_run(url: str) -> tuple[str, str, int, str]:
    [row] = await rows(
        url,
        "select analyzer, status, results, error from analyzer_runs order by id desc limit 1",
    )
    return row


async def test_a_run_stores_its_results_records_itself_and_announces_them(
    migrated_db_url: str, engine: AsyncEngine
) -> None:
    dsn = migrated_db_url.replace("postgresql+asyncpg://", "postgresql://")
    listener = await asyncpg.connect(dsn)
    heard: list[str] = []
    await listener.add_listener(CHANNEL, lambda *args: heard.append(args[3]))
    try:
        fake = Fake(results=[
            Result("spread.cents", 2_000_000, market_id=1),
            Result("arb.profit", -5, event_ticker="KXA-E1"),
        ])  # fmt: skip
        record = await runner_for(engine, fake).run_once(fake)
        await asyncio.sleep(0.2)
    finally:
        await listener.close()
    assert record is not None and (record.status, record.results, record.error) == ("ok", 2, "")
    assert await last_run(migrated_db_url) == ("fake", "ok", 2, "")
    stored = await rows(
        migrated_db_url,
        "select analyzer, metric, market_id, event_ticker, value_e6 from analysis_results "
        "order by metric",
    )
    assert stored == [
        ("fake", "arb.profit", None, "KXA-E1", -5),
        ("fake", "spread.cents", 1, None, 2_000_000),
    ]
    assert heard == ["fake:2"]
    assert await scalar(migrated_db_url, "select finished_at is not null from analyzer_runs")


async def test_results_default_to_the_start_of_the_run_unless_they_carry_a_time(
    migrated_db_url: str, engine: AsyncEngine
) -> None:
    earlier = datetime(2026, 1, 1, tzinfo=UTC)
    fake = Fake(results=[Result("a", 1, market_id=1), Result("b", 2, market_id=1, ts=earlier)])
    await runner_for(engine, fake).run_once(fake)
    by_metric = dict(await rows(migrated_db_url, "select metric, ts from analysis_results"))
    assert by_metric["b"] == earlier and by_metric["a"] == fake.contexts[0].now


async def test_an_exception_is_recorded_and_writes_nothing(
    migrated_db_url: str, engine: AsyncEngine
) -> None:
    fake = Fake()
    fake.error = RuntimeError("the model blew up")
    record = await runner_for(engine, fake).run_once(fake)
    assert record is not None and record.status == "failed"
    assert await last_run(migrated_db_url) == (
        "fake",
        "failed",
        0,
        "RuntimeError: the model blew up",
    )
    assert await scalar(migrated_db_url, "select count(*) from analysis_results") == 0


async def test_one_bad_result_rejects_the_whole_run_so_nothing_is_half_written(
    migrated_db_url: str, engine: AsyncEngine
) -> None:
    fake = Fake(results=[Result("good", 1, market_id=1), Result("bad", 0.5, market_id=1)])  # type: ignore[arg-type]
    record = await runner_for(engine, fake).run_once(fake)
    assert record is not None and record.status == "failed" and "value_e6" in record.error
    assert await scalar(migrated_db_url, "select count(*) from analysis_results") == 0


async def test_a_run_that_returns_the_wrong_kind_of_thing_fails(
    migrated_db_url: str, engine: AsyncEngine
) -> None:
    fake = Fake(results="not a list")
    record = await runner_for(engine, fake).run_once(fake)
    assert record is not None and record.status == "failed" and "list of Result" in record.error


async def test_too_many_results_in_one_run_fail_it(
    migrated_db_url: str, engine: AsyncEngine
) -> None:
    fake = Fake(results=[Result("m", i, market_id=1) for i in range(11)])
    record = await runner_for(engine, fake, max_results=10).run_once(fake)
    assert record is not None and record.status == "failed" and "limit 10" in record.error
    assert await scalar(migrated_db_url, "select count(*) from analysis_results") == 0


async def test_a_run_that_takes_too_long_is_cancelled_and_recorded_as_a_timeout(
    migrated_db_url: str, engine: AsyncEngine
) -> None:
    fake = Fake()
    fake.timeout = timedelta(seconds=0.3)
    fake.delay = 5
    start = asyncio.get_running_loop().time()
    record = await runner_for(engine, fake).run_once(fake)
    assert asyncio.get_running_loop().time() - start < 3
    assert record is not None and record.status == "timeout"
    assert (await last_run(migrated_db_url))[1] == "timeout"


async def test_two_processes_do_not_run_the_same_analyzer_at_once(
    migrated_db_url: str, engine: AsyncEngine
) -> None:
    slow = Fake()
    slow.delay = 0.6
    other_engine = db.make_engine(migrated_db_url)
    try:
        first = asyncio.create_task(runner_for(engine, slow).run_once(slow))
        await asyncio.sleep(0.2)  # the first run holds the lock
        second = await runner_for(other_engine, slow).run_once(slow)
        assert second is None  # skipped, not queued, not failed
        assert (await first) is not None
    finally:
        await other_engine.dispose()
    assert slow.calls == 1
    assert await scalar(migrated_db_url, "select count(*) from analyzer_runs") == 1
    again = await runner_for(engine, slow).run_once(slow)  # and the lock was released
    assert again is not None and slow.calls == 2


async def test_a_run_left_running_by_a_dead_process_is_marked_interrupted(
    migrated_db_url: str, engine: AsyncEngine
) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "insert into analyzer_runs (analyzer, started_at, interval_seconds) "
                "values ('fake', now() - interval '1 hour', 60)"
            )
        )
    fake = Fake()
    await runner_for(engine, fake).run_once(fake)
    statuses = [
        r[0] for r in await rows(migrated_db_url, "select status from analyzer_runs order by id")
    ]
    assert statuses == ["failed", "ok"]
    assert "interrupted" in await scalar(
        migrated_db_url, "select error from analyzer_runs order by id limit 1"
    )


async def test_old_run_records_are_pruned_but_recent_ones_stay(
    migrated_db_url: str, engine: AsyncEngine
) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "insert into analyzer_runs (analyzer, started_at, finished_at, status, "
                "interval_seconds) values ('fake', now() - interval '8 days', now(), 'ok', 60), "
                "('fake', now() - interval '2 days', now(), 'ok', 60)"
            )
        )
    fake = Fake()
    await runner_for(engine, fake).run_once(fake)
    assert await scalar(migrated_db_url, "select count(*) from analyzer_runs") == 2


async def test_the_schedule_waits_the_interval_retries_failures_sooner_and_survives_restarts(
    migrated_db_url: str, engine: AsyncEngine
) -> None:
    now = datetime(2026, 10, 10, 12, 0, tzinfo=UTC)
    clock = {"t": now}
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)
        clock["t"] += timedelta(seconds=seconds)

    fake = Fake()
    fake.interval = timedelta(minutes=10)
    fake.timeout = timedelta(minutes=1)
    fake.error = RuntimeError("first time fails")
    runner = runner_for(
        engine, fake, clock=lambda: clock["t"], sleep=sleep, retry=timedelta(minutes=2)
    )

    async def second_attempt_works() -> None:
        await asyncio.sleep(0)

    await runner._loop(fake, rounds=1)  # runs at once, fails
    fake.error = None
    await runner._loop(fake, rounds=2)  # a "restarted" loop: last ok is none -> runs at once
    assert slept[0] == 0.0 and slept[1] == 0.0
    assert slept[2] == 600.0  # after a success, the full interval
    statuses = [
        r[0] for r in await rows(migrated_db_url, "select status from analyzer_runs order by id")
    ]
    assert statuses == ["failed", "ok", "ok"]

    # A restart right after a success does not run again until the interval has passed.
    slept.clear()
    clock["t"] += timedelta(minutes=3)
    await runner._loop(fake, rounds=1)
    assert 0 < slept[0] <= 600.0


async def test_a_failing_analyzer_does_not_stop_the_others(
    migrated_db_url: str, engine: AsyncEngine
) -> None:
    bad, good = Fake("bad"), Fake("good")
    bad.error = RuntimeError("always")
    await runner_for(engine, bad, good, sleep=lambda s: asyncio.sleep(0)).serve(rounds=2)
    by_name = {
        name: (status, n)
        for name, status, n in await rows(
            migrated_db_url,
            "select analyzer, status, count(*) from analyzer_runs group by 1, 2",
        )
    }
    assert by_name == {"bad": ("failed", 2), "good": ("ok", 2)}


async def test_serving_with_no_analyzers_returns_when_asked_to(engine: AsyncEngine) -> None:
    await AnalysisRunner(engine, {}).serve(rounds=1)


async def test_the_database_enforces_one_subject_and_known_statuses(
    migrated_db_url: str, engine: AsyncEngine
) -> None:
    for sql in (
        "insert into analysis_results values (now(), 'a', 'm', null, null, 1)",
        "insert into analysis_results values (now(), 'a', 'm', 1, 'E', 1)",
        "insert into analyzer_runs (analyzer, started_at, interval_seconds, status) "
        "values ('a', now(), 60, 'weird')",
    ):
        with pytest.raises(Exception, match="violates check constraint"):
            async with engine.begin() as conn:
                await conn.execute(text(sql))


async def test_results_are_compressed_and_expire_by_policy(migrated_db_url: str) -> None:
    jobs = await rows(
        migrated_db_url,
        "select proc_name, config->>'drop_after', config->>'compress_after' "
        "from timescaledb_information.jobs where hypertable_name = 'analysis_results' "
        "order by proc_name",
    )
    assert jobs == [("policy_compression", None, "1 day"), ("policy_retention", "30 days", None)]


# ------------------------------------------------------------------ status and the CLI


async def test_status_lists_analyzers_and_flags_failures_and_silence(
    migrated_db_url: str, engine: AsyncEngine, tmp_path: object
) -> None:
    good, bad = Fake("good"), Fake("bad")
    bad.error = ValueError("nope")
    runner = runner_for(engine, good, bad)
    await runner.run_once(good)
    await runner.run_once(bad)
    await runner.record_load_failures([LoadFailure("broken", "ModuleNotFoundError: x")])
    report = await collect_status(engine, 100, str(tmp_path))
    by_name = {a["analyzer"]: a for a in report["analyzers"]}
    assert by_name["good"]["status"] == "ok" and by_name["bad"]["status"] == "failed"
    problems = " | ".join(report["problems"])
    assert "analyzer bad failed: ValueError: nope" in problems
    assert "analyzer broken did not load" in problems and "analyzer good" not in problems
    out = render(report)
    assert "Analyzers:" in out and "good" in out and "failed" in out

    async with engine.begin() as conn:  # the good one goes quiet for an hour
        await conn.execute(
            text(
                "update analyzer_runs set started_at = now() - interval '1 hour' "
                "where analyzer = 'good'"
            )
        )
    stale = await collect_status(engine, 100, str(tmp_path))
    assert any("analyzer good last completed a run 60m ago" in p for p in stale["problems"])


async def test_no_analyzers_at_all_is_not_a_problem(engine: AsyncEngine, tmp_path: object) -> None:
    report = await collect_status(engine, 100, str(tmp_path))
    assert report["analyzers"] == [] and not any("analyzer" in p for p in report["problems"])


def test_the_list_and_run_commands(migrated_db_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    from kalshiterm_server import cli_analyze
    from kalshiterm_server.analytics.registry import Discovery

    fake = Fake("demo")
    found = Discovery({"demo": fake}, {"demo": "kterm-demo: kterm_demo:Demo"}, [])
    monkeypatch.setattr(cli_analyze, "_discover", lambda: found)
    monkeypatch.setenv("KTERM_DB_URL", migrated_db_url)
    runner = CliRunner()
    listed = runner.invoke(app, ["analyze", "list"])
    assert listed.exit_code == 0 and "demo" in listed.output and "every     60s" in listed.output
    ran = runner.invoke(app, ["analyze", "run", "demo"])
    assert ran.exit_code == 0 and "demo: ok, 1 result(s)" in ran.output
    assert runner.invoke(app, ["analyze", "run", "ghost"]).exit_code == 1
    found.failures.append(LoadFailure("broken", "ImportError: x"))
    failed = runner.invoke(app, ["analyze", "list"])
    assert failed.exit_code == 1 and "FAILED TO LOAD broken" in failed.output
