"""Database test harness: one real TimescaleDB container per session, one fresh database per test.

The image tag is read from ``deploy/docker-compose.dev.yml`` so dev and tests cannot drift.
Tests that need Docker are marked ``db`` and skip cleanly if Docker is unavailable.
"""

import asyncio
import re
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.postgres import PostgresContainer

COMPOSE_FILE = Path(__file__).resolve().parents[3] / "deploy" / "docker-compose.dev.yml"


def timescale_image() -> str:
    match = re.search(r"^\s*image:\s*(\S+)\s*$", COMPOSE_FILE.read_text(), re.MULTILINE)
    assert match, f"no image line in {COMPOSE_FILE}"
    return match.group(1)


@pytest.fixture(scope="session")
def timescale_container() -> Iterator[PostgresContainer]:
    try:
        container = PostgresContainer(
            timescale_image(), username="test", password="test", dbname="postgres", driver=None
        )
        container.start()
    except Exception as exc:  # no Docker daemon, image pull failure, ...
        pytest.skip(f"Docker/TimescaleDB container unavailable: {exc}")
    try:
        yield container
    finally:
        container.stop()


def db_url(container: PostgresContainer, dbname: str) -> str:
    host, port = container.get_container_host_ip(), container.get_exposed_port(5432)
    return f"postgresql+asyncpg://test:test@{host}:{port}/{dbname}"


async def admin(container: PostgresContainer, statement: str) -> None:
    engine = create_async_engine(db_url(container, "postgres"), isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as conn:
            await conn.execute(text(statement))
    finally:
        await engine.dispose()


def new_db_name() -> str:
    return f"t_{uuid.uuid4().hex[:12]}"


@pytest.fixture
def fresh_db_url(timescale_container: PostgresContainer) -> Iterator[str]:
    """URL of a brand-new, truly empty database; dropped after the test.

    Created from ``template0``: the Timescale image installs the extension into ``template1``,
    so a plain ``CREATE DATABASE`` would already contain it.
    """
    name = new_db_name()
    asyncio.run(admin(timescale_container, f'CREATE DATABASE "{name}" TEMPLATE template0'))
    try:
        yield db_url(timescale_container, name)
    finally:
        asyncio.run(admin(timescale_container, f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
