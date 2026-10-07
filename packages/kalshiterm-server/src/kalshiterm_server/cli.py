"""``kterm-server`` operations CLI (grows with later slices)."""

import asyncio

import typer

from kalshiterm_server import db
from kalshiterm_server.config import ServerSettings

app = typer.Typer(no_args_is_help=True, help="KalshiTerm server operations.")
db_app = typer.Typer(no_args_is_help=True, help="Database migrations.")
app.add_typer(db_app, name="db")


def _url() -> str:
    return ServerSettings().db_url  # type: ignore[call-arg]  # read from KTERM_DB_URL


@db_app.command("upgrade")
def db_upgrade(revision: str = "head") -> None:
    """Apply migrations up to REVISION (default: head)."""
    db.upgrade(_url(), revision)
    typer.echo(f"database is at {revision}")


@db_app.command("downgrade")
def db_downgrade(revision: str) -> None:
    """Roll back to REVISION (e.g. -1 or a revision id)."""
    db.downgrade(_url(), revision)
    typer.echo(f"database rolled back to {revision}")


@db_app.command("current")
def db_current() -> None:
    """Show the revision the database is at."""

    async def run() -> str | None:
        engine = db.make_engine(_url())
        try:
            return await db.current_revision(engine)
        finally:
            await engine.dispose()

    typer.echo(asyncio.run(run()) or "(no migrations applied)")
