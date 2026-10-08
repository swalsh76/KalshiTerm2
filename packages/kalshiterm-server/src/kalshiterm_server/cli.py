"""``kterm-server`` operations CLI (grows with later slices)."""

import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path
from typing import Annotated, Any

import typer
from kalshi_core.auth import AuthError, KalshiSigner
from kalshi_core.config import KalshiSettings
from kalshi_core.orderbook import OrderBookFeed
from kalshi_core.rest import KalshiRestClient
from kalshi_core.ws import KalshiWebSocket

from kalshiterm_server import db
from kalshiterm_server.api.serve import UnsafeBind, serve
from kalshiterm_server.config import ServerSettings
from kalshiterm_server.governor import GB, Governor
from kalshiterm_server.ingest.backfill import GapBackfiller
from kalshiterm_server.ingest.discovery import discover, discovery_loop
from kalshiterm_server.ingest.stream import StreamIngestor
from kalshiterm_server.ingest.watchlist import WatchlistConfig, WatchlistController, load_config
from kalshiterm_server.init import InitError, run_init
from kalshiterm_server.shutdown import cancel_on_sigterm
from kalshiterm_server.status import collect_status, quick_health, render

app = typer.Typer(no_args_is_help=True, help="KalshiTerm server operations.")
db_app = typer.Typer(no_args_is_help=True, help="Database migrations.")
app.add_typer(db_app, name="db")


def _settings() -> ServerSettings:
    return ServerSettings()  # type: ignore[call-arg]  # db_url is read from KTERM_DB_URL


def _url() -> str:
    return _settings().db_url


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


@app.command("discover")
def discover_command(
    full: bool = typer.Option(False, "--full", help="Force a full refresh."),
    lookback_days: int = typer.Option(
        7, help="On the first run only: how far back to collect already-settled markets."
    ),
) -> None:
    """Run one discovery cycle: series, events and markets into the database."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per request is noise

    async def run() -> None:
        settings = KalshiSettings()
        signer = KalshiSigner.from_settings(settings) if settings.key_id else None
        engine = db.make_engine(_url())
        try:
            async with KalshiRestClient(settings, signer=signer) as rest:
                report = await discover(
                    rest, engine, first_run_lookback=timedelta(days=lookback_days), full=full
                )
        finally:
            await engine.dispose()
        for line in report.lines():
            typer.echo(line)

    asyncio.run(run())


@app.command("ingest")
def ingest_command(
    seconds: float = typer.Option(0, help="Stop after this many seconds (0 = run until stopped)."),
    stats_every: float = typer.Option(30, help="Seconds between statistics lines."),
    watch: Annotated[
        list[str] | None,
        typer.Option("--watch", help="Market ticker whose orderbook to store (repeatable)."),
    ] = None,
    snapshot_interval: float = typer.Option(
        300, help="Seconds between full orderbook snapshots of each watched market."
    ),
    watchlist: Annotated[
        Path | None,
        typer.Option(
            "--watchlist",
            exists=True,
            dir_okay=False,
            help="TOML file with [watchlist] markets / auto_top_n; re-read every cycle.",
        ),
    ] = None,
    watchlist_every: float = typer.Option(
        300, help="Seconds between watchlist updates (file re-read, top-N refresh)."
    ),
    max_backfill_hours: float = typer.Option(
        6, help="Longest outage whose missed trades are fetched from REST (older is truncated)."
    ),
    governor_every: float = typer.Option(
        600, help="Seconds between storage-governor cycles (size sample, thresholds)."
    ),
    discover_every: float = typer.Option(
        0, help="Seconds between market-discovery cycles (0 = off; production uses 900)."
    ),
) -> None:
    """Stream tickers, trades and market lifecycle events (and watched orderbooks) to the DB."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if watchlist is not None:
        try:
            load_config(watchlist)  # fail fast on a bad file; later edits are re-read live
        except ValueError as exc:
            raise typer.BadParameter(str(exc), param_hint="--watchlist") from exc

    async def run() -> None:
        cancel_on_sigterm()  # `docker stop` must drain buffered rows, not kill us mid-flush
        settings = KalshiSettings()
        signer = KalshiSigner.from_settings(settings)
        engine = db.make_engine(_url())
        try:
            async with contextlib.AsyncExitStack() as stack:
                ws = await stack.enter_async_context(KalshiWebSocket(settings, signer))
                for channel in (
                    "ticker",
                    "trade",
                    "market_lifecycle_v2",
                    "multivariate_market_lifecycle",
                ):
                    await ws.subscribe(channel)
                source: AsyncIterator[Any] = ws.messages()
                feed: OrderBookFeed | None = None
                rest = await stack.enter_async_context(KalshiRestClient(settings, signer=signer))
                if watch or watchlist:
                    # Starts empty: the controller adds the markets (an empty subscription is
                    # never sent, so it cannot mean "every market").
                    feed = await stack.enter_async_context(
                        OrderBookFeed(ws, rest, [], periodic_snapshot_interval=snapshot_interval)
                    )
                    source = feed.events()  # orderbook events plus everything else, in order
                ingestor = StreamIngestor(source, engine)
                backfiller = GapBackfiller(
                    rest, engine, ingestor, max_window=timedelta(hours=max_backfill_hours)
                )
                await backfiller.start()  # queues the gap since the last run stopped
                server = _settings()
                governor = Governor(
                    engine,
                    int(server.storage_budget_gb * GB),
                    shedder=ingestor,
                    disk_path=server.disk_check_path,
                    interval=governor_every,
                )
                tasks: list[asyncio.Task[None]] = [
                    asyncio.create_task(backfiller.run()),
                    asyncio.create_task(governor.run()),
                ]
                if discover_every > 0:  # shares the REST client, so one rate limit for all
                    tasks.append(asyncio.create_task(discovery_loop(rest, engine, discover_every)))
                if feed is not None:
                    controller = WatchlistController(
                        engine,
                        feed,
                        ingestor,
                        (lambda: load_config(watchlist)) if watchlist else WatchlistConfig,
                        extra_manual=frozenset(watch or ()),
                        interval=watchlist_every,
                    )

                    async def keep_watchlist() -> None:
                        try:
                            await controller.start()
                        except Exception:  # the periodic cycle retries
                            logging.getLogger("kalshiterm_server").exception(
                                "watchlist start failed"
                            )
                        await controller.run()

                    tasks.append(asyncio.create_task(keep_watchlist()))

                async def report() -> None:
                    while True:
                        await asyncio.sleep(stats_every)
                        typer.echo(f"stats: {ingestor.stats()} ws: {ws.stats()}")

                tasks.append(asyncio.create_task(report()))
                try:
                    if seconds > 0:
                        async with asyncio.timeout(seconds):
                            await ingestor.run()
                    else:
                        await ingestor.run()
                except TimeoutError:
                    pass
                except asyncio.CancelledError:  # SIGTERM / Ctrl-C: the ingestor has drained
                    typer.echo("stop requested: buffered rows flushed")
                finally:
                    for task in tasks:
                        task.cancel()
                typer.echo(f"final stats: {ingestor.stats()}")
        finally:
            await engine.dispose()

    asyncio.run(run())


@app.command("status")
def status_command(
    as_json: bool = typer.Option(False, "--json", help="Machine-readable output."),
) -> None:
    """Storage, ingest freshness, gaps, jobs and discovery in one report."""

    async def run() -> dict[str, object]:
        server = _settings()
        engine = db.make_engine(server.db_url)
        try:
            return await collect_status(
                engine,
                server.storage_budget_gb,
                server.disk_check_path,
                host_state_file=server.host_state_file,
            )
        finally:
            await engine.dispose()

    report = asyncio.run(run())
    typer.echo(json.dumps(report, indent=2, default=str) if as_json else render(report))
    if report["problems"]:
        raise typer.Exit(code=1)  # so scripts and health checks can act on it


@app.command("health")
def health_command() -> None:
    """Fast liveness check (for the container health check): exit 0 if healthy, else 1."""

    async def run() -> list[str]:
        engine = db.make_engine(_url())
        try:
            return await quick_health(engine)
        finally:
            await engine.dispose()

    problems = asyncio.run(run())
    if problems:
        typer.echo("unhealthy: " + "; ".join(problems))
        raise typer.Exit(code=1)
    typer.echo("ok")


@app.command("init")
def init_command(
    key_id: Annotated[str, typer.Option(help="Key ID of the server's own READ-ONLY Kalshi key.")],
    key_file: Annotated[
        Path,
        typer.Option(exists=True, dir_okay=False, help="Its Ed25519 private key (PEM file)."),
    ],
    out: Annotated[Path, typer.Option(help="The deploy directory to write into.")] = Path(),
    budget_gb: float = typer.Option(500, help="Storage budget in GB (PLAN §9)."),
    force: bool = typer.Option(False, help="Overwrite an existing setup (new DB password!)."),
) -> None:
    """Write .env, secrets and a starter watchlist for a production deployment."""
    try:
        result = run_init(out, key_id=key_id, key_file=key_file, budget_gb=budget_gb, force=force)
    except (InitError, AuthError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    for path in result.written:
        typer.echo(f"wrote {path}")
    for path in result.kept:
        typer.echo(f"kept  {path} (already there)")
    typer.echo("Secrets were written with owner-only permissions and are not shown.")


@app.command("api")
def api_command(
    host: Annotated[str | None, typer.Option(help="Address to bind (default: loopback).")] = None,
    port: Annotated[int | None, typer.Option(help="Port (default 8700).")] = None,
    tls_cert: Annotated[Path | None, typer.Option(help="TLS certificate (PEM).")] = None,
    tls_key: Annotated[Path | None, typer.Option(help="TLS private key (PEM).")] = None,
) -> None:
    """Serve the HTTP API. Refuses to serve plaintext HTTP beyond this machine."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    settings = _settings()
    try:
        serve(
            settings,
            host or settings.api_host,
            port or settings.api_port,
            tls_cert,
            tls_key,
        )
    except UnsafeBind as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
