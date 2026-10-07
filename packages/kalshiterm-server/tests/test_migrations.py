from pathlib import Path

import pytest
from conftest import admin, db_url, new_db_name, timescale_image
from kalshiterm_server import db
from kalshiterm_server.cli import app
from kalshiterm_server.config import ServerSettings
from sqlalchemy import text
from testcontainers.community.postgres import PostgresContainer
from typer.testing import CliRunner

pytestmark = pytest.mark.db


async def extension_version(url: str) -> str | None:
    engine = db.make_engine(url)
    try:
        async with engine.connect() as conn:
            row = await conn.execute(
                text("select extversion from pg_extension where extname = 'timescaledb'")
            )
            return row.scalar_one_or_none()
    finally:
        await engine.dispose()


async def revision(url: str) -> str | None:
    engine = db.make_engine(url)
    try:
        return await db.current_revision(engine)
    finally:
        await engine.dispose()


async def test_upgrade_enables_timescaledb_and_records_the_revision(fresh_db_url: str) -> None:
    assert await extension_version(fresh_db_url) is None  # truly empty to begin with
    await db.upgrade_async(fresh_db_url)
    version = await extension_version(fresh_db_url)
    assert version is not None and version.startswith("2.")
    assert await revision(fresh_db_url) == db.head_revision()


async def test_upgrade_is_idempotent(fresh_db_url: str) -> None:
    await db.upgrade_async(fresh_db_url)
    await db.upgrade_async(fresh_db_url)  # second run is a no-op, not an error
    assert await extension_version(fresh_db_url) is not None


async def test_downgrade_to_base_removes_the_extension(fresh_db_url: str) -> None:
    await db.upgrade_async(fresh_db_url)
    await db.downgrade_async(fresh_db_url, "base")
    assert await extension_version(fresh_db_url) is None
    assert await revision(fresh_db_url) is None


async def test_upgrade_is_a_safe_noop_when_the_template_already_has_the_extension(
    timescale_container: PostgresContainer,
) -> None:
    """The deployed POSTGRES_DB comes from template1, where the Timescale image installs it."""
    name = new_db_name()
    await admin(timescale_container, f'CREATE DATABASE "{name}"')  # from template1
    url = db_url(timescale_container, name)
    try:
        assert await extension_version(url) is not None
        await db.upgrade_async(url)
        assert await extension_version(url) is not None
        assert await revision(url) == db.head_revision()
    finally:
        await admin(timescale_container, f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


def test_cli_upgrade_current_and_downgrade(
    fresh_db_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("KTERM_DB_URL", fresh_db_url)
    runner = CliRunner()
    assert "(no migrations applied)" in runner.invoke(app, ["db", "current"]).output
    result = runner.invoke(app, ["db", "upgrade"])
    assert result.exit_code == 0, result.output
    assert runner.invoke(app, ["db", "current"]).output.strip() == db.head_revision()
    result = runner.invoke(app, ["db", "downgrade", "base"])
    assert result.exit_code == 0, result.output
    assert "(no migrations applied)" in runner.invoke(app, ["db", "current"]).output


def test_database_url_has_no_default(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("KTERM_DB_URL", raising=False)
    monkeypatch.chdir(tmp_path)  # no .env here
    with pytest.raises(ValueError):
        ServerSettings()  # type: ignore[call-arg]


def test_the_database_image_is_pinned_not_floating() -> None:
    image = timescale_image()
    assert ":" in image and not image.endswith(":latest")
    assert "-pg" in image  # a Postgres major is part of the tag
