"""Alembic environment (async engine). The URL is passed in via ``config.attributes``."""

import asyncio

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

config = context.config
target_metadata = None  # tables are written as explicit SQL/op migrations


def _url() -> str:
    url = config.attributes.get("url")
    if not url:
        raise RuntimeError("no database URL: run migrations via kalshiterm_server.db")
    return str(url)


def _run(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def _run_async() -> None:
    engine = create_async_engine(_url())
    async with engine.connect() as connection:
        await connection.run_sync(_run)
    await engine.dispose()


asyncio.run(_run_async())
