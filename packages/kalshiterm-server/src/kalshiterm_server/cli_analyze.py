"""``kterm-server analyze ...``: list, run and serve the analyzers."""

import asyncio
import contextlib
import logging
from datetime import timedelta

import typer

from kalshiterm_server import db
from kalshiterm_server.analytics import registry
from kalshiterm_server.analytics.runner import AnalysisRunner
from kalshiterm_server.cli_common import server_settings
from kalshiterm_server.shutdown import cancel_on_sigterm

analyze_app = typer.Typer(no_args_is_help=True, help="Analyzers (built in and plugins).")


def _discover() -> registry.Discovery:
    disabled = {n.strip() for n in server_settings().analyzers_disabled.split(",") if n.strip()}
    return registry.discover(disabled)


@analyze_app.command("list")
def analyze_list() -> None:
    """The installed analyzers and any plugin that failed to load (exit 1 if one did)."""
    found = _discover()
    for name, analyzer in sorted(found.analyzers.items()):
        every = int(analyzer.interval.total_seconds())
        limit = int(analyzer.timeout.total_seconds())
        typer.echo(f"{name:<24}every {every:>6}s  timeout {limit:>4}s  {found.sources[name]}")
    if not found.analyzers:
        typer.echo("no analyzers installed")
    for failure in found.failures:
        typer.echo(f"FAILED TO LOAD {failure.entry_point}: {failure.error}", err=True)
    if found.failures:
        raise typer.Exit(code=1)


@analyze_app.command("run")
def analyze_run(name: str) -> None:
    """Run one analyzer now and store its results (for trying a plugin out)."""
    found = _discover()
    if name not in found.analyzers:
        known = ", ".join(sorted(found.analyzers)) or "none installed"
        typer.echo(f"error: no analyzer {name!r} (have: {known})", err=True)
        raise typer.Exit(code=1)
    settings = server_settings()

    async def go() -> object:
        engine = db.make_engine(settings.db_url)
        try:
            runner = AnalysisRunner(
                engine, found.analyzers, max_results=settings.analyze_max_results
            )
            return await runner.run_once(found.analyzers[name])
        finally:
            await engine.dispose()

    record = asyncio.run(go())
    if record is None:
        typer.echo("that analyzer is already running in another process")
        raise typer.Exit(code=1)
    typer.echo(f"{name}: {record.status}, {record.results} result(s) {record.error}".rstrip())  # type: ignore[attr-defined]
    if record.status != "ok":  # type: ignore[attr-defined]
        raise typer.Exit(code=1)


@analyze_app.command("serve")
def analyze_serve() -> None:
    """Run every installed analyzer on its schedule (this is what the analyze container runs)."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    settings = server_settings()
    found = _discover()

    async def go() -> None:
        cancel_on_sigterm()  # `docker stop` ends the loops quietly
        engine = db.make_engine(settings.db_url)
        try:
            runner = AnalysisRunner(
                engine,
                found.analyzers,
                max_results=settings.analyze_max_results,
                retry=timedelta(minutes=settings.analyze_retry_minutes),
            )
            await runner.record_load_failures(found.failures)
            for name in sorted(found.analyzers):
                typer.echo(f"analyzer {name}: {found.sources[name]}")
            await runner.serve()
        finally:
            await engine.dispose()

    with contextlib.suppress(asyncio.CancelledError, KeyboardInterrupt):
        asyncio.run(go())


def register(app: typer.Typer) -> None:
    app.add_typer(analyze_app, name="analyze")
