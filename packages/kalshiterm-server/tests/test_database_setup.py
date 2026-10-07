"""Guards for the database container setup (image, data volume, durability settings)."""

import re
import uuid
from collections.abc import Iterator

import docker  # type: ignore[import-untyped]  # no stubs; transitive via testcontainers
import pytest
from conftest import compose_mount_target, db_url, timescale_image
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.postgres import PostgresContainer


def pg_major(image: str) -> int:
    match = re.search(r"-pg(\d+)", image)
    assert match, f"no -pgNN in {image}"
    return int(match.group(1))


def test_mount_path_matches_the_images_postgres_major() -> None:
    """Mounting the wrong path silently leaves the data on an anonymous volume."""
    expected = (
        "/var/lib/postgresql" if pg_major(timescale_image()) >= 18 else ("/var/lib/postgresql/data")
    )
    assert compose_mount_target() == expected


async def scalar(url: str, statement: str) -> str:
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            return str((await conn.execute(text(statement))).scalar_one())
    finally:
        await engine.dispose()


@pytest.mark.db
async def test_data_checksums_are_on(timescale_container: PostgresContainer) -> None:
    url = db_url(timescale_container, "postgres")
    assert await scalar(url, "show data_checksums") == "on"


@pytest.fixture
def named_volume() -> Iterator[str]:
    client = docker.from_env()
    volume = client.volumes.create(name=f"kterm-test-{uuid.uuid4().hex[:10]}")
    try:
        yield str(volume.name)
    finally:
        volume.remove(force=True)


def start(volume: str) -> PostgresContainer:
    container = PostgresContainer(
        timescale_image(), username="test", password="test", dbname="durable", driver=None
    ).with_volume_mapping(volume, compose_mount_target(), "rw")
    container.start()
    return container


@pytest.mark.db
async def test_data_survives_destroying_and_recreating_the_container(named_volume: str) -> None:
    """The real durability check, using the compose file's mount path."""
    first = start(named_volume)
    try:
        url = db_url(first, "durable")
        engine = create_async_engine(url)
        async with engine.begin() as conn:
            await conn.execute(text("create table keepme (v text)"))
            await conn.execute(text("insert into keepme values ('still here')"))
        await engine.dispose()
    finally:
        first.stop()  # container removed; only the named volume remains

    second = start(named_volume)
    try:
        assert await scalar(db_url(second, "durable"), "select v from keepme") == "still here"
    finally:
        second.stop()
