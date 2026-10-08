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
