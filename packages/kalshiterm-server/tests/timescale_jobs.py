import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


async def quiet_background_jobs(engine: AsyncEngine) -> None:
    """Stop Timescale's policy jobs in a test database and wait for any run in progress.

    A new database starts its policy jobs at once. A job running at the same moment as a
    test that triggers the same policy (or refreshes an aggregate) by hand makes Timescale
    reject the second ("concurrent refresh"), so tests that run policies switch them off and
    call them explicitly.
    """
    async with engine.begin() as conn:
        await conn.execute(
            text("select alter_job(job_id, scheduled => false) from timescaledb_information.jobs")
        )
    async with asyncio.timeout(30):
        while True:
            async with engine.connect() as conn:
                running = (
                    await conn.execute(
                        text(
                            "select count(*) from timescaledb_information.job_stats "
                            "where job_status = 'Running'"
                        )
                    )
                ).scalar_one()
            if not running:
                return
            await asyncio.sleep(0.05)


async def job_state(engine: AsyncEngine, proc_name: str) -> str:
    """What Timescale and Postgres say about a job right now, for failure messages."""
    async with engine.connect() as conn:
        jobs = (
            await conn.execute(
                text(
                    "select j.job_id, j.scheduled, j.config, s.job_status, s.last_run_status, "
                    "s.total_runs, s.last_run_started_at from timescaledb_information.jobs j "
                    "left join timescaledb_information.job_stats s using (job_id) "
                    "where j.proc_name = :p"
                ),
                {"p": proc_name},
            )
        ).all()
        others = (
            await conn.execute(
                text(
                    "select application_name, state, left(query, 60) from pg_stat_activity "
                    "where datname = current_database() and pid <> pg_backend_pid() "
                    "and backend_type <> 'client backend' or application_name like '%job%'"
                )
            )
        ).all()
    return (
        f"job {proc_name}: {[tuple(r) for r in jobs]}; other backends: {[tuple(r) for r in others]}"
    )


async def run_job_named(engine: AsyncEngine, proc_name: str) -> str:
    """Run a user-defined job (one with no hypertable) now; returns its state for messages.

    Tests put the result in their assertion messages (``assert ok, state``): the one failure
    seen so far (CI, 2026-10-09: a market the job should have slimmed was not) could not be
    explained from the assertion alone.
    """
    async with engine.connect() as conn:
        job = (
            await conn.execute(
                text("select job_id from timescaledb_information.jobs where proc_name = :p"),
                {"p": proc_name},
            )
        ).scalar_one()
    before = await job_state(engine, proc_name)
    async with engine.connect() as conn:
        auto = await conn.execution_options(isolation_level="AUTOCOMMIT")
        await auto.execute(text(f"CALL run_job({job})"))
    return f"before: {before}\nafter: {await job_state(engine, proc_name)}"


async def quiet_database(url: str) -> None:
    """``quiet_background_jobs`` for a database given by URL, before a migration downgrade.

    A downgrade drops tables the policy jobs are using; a job running at that moment deadlocks
    with it (seen on CI). Production downgrades should stop the ingest service first.
    """
    from kalshiterm_server import db

    engine = db.make_engine(url)
    try:
        await quiet_background_jobs(engine)
    finally:
        await engine.dispose()
