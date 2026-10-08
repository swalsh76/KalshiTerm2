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


async def run_job_named(engine: AsyncEngine, proc_name: str) -> None:
    """Run a user-defined job (one with no hypertable) now."""
    async with engine.connect() as conn:
        job = (
            await conn.execute(
                text("select job_id from timescaledb_information.jobs where proc_name = :p"),
                {"p": proc_name},
            )
        ).scalar_one()
    async with engine.connect() as conn:
        auto = await conn.execution_options(isolation_level="AUTOCOMMIT")
        await auto.execute(text(f"CALL run_job({job})"))


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
