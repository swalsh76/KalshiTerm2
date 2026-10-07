"""Engine creation and Alembic migration helpers."""

import asyncio
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def make_engine(url: str) -> AsyncEngine:
    return create_async_engine(url, pool_pre_ping=True)


def _config(url: str) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(MIGRATIONS_DIR))
    cfg.attributes["url"] = url  # not set_main_option: configparser would mangle '%' in URLs
    return cfg


def head_revision() -> str:
    """The newest migration revision shipped in this package."""
    head = ScriptDirectory(str(MIGRATIONS_DIR)).get_current_head()
    assert head is not None
    return head


def upgrade(url: str, revision: str = "head") -> None:
    """Apply migrations (blocking; run it in a thread from async code)."""
    command.upgrade(_config(url), revision)


def downgrade(url: str, revision: str) -> None:
    command.downgrade(_config(url), revision)


async def upgrade_async(url: str, revision: str = "head") -> None:
    await asyncio.to_thread(upgrade, url, revision)


async def downgrade_async(url: str, revision: str) -> None:
    await asyncio.to_thread(downgrade, url, revision)


async def current_revision(engine: AsyncEngine) -> str | None:
    def read(sync_conn: object) -> str | None:
        return MigrationContext.configure(sync_conn).get_current_revision()  # type: ignore[arg-type]

    async with engine.connect() as conn:
        return await conn.run_sync(read)
