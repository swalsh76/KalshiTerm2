"""``kterm-server`` operations CLI (grows with later slices)."""

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path
from typing import Annotated, Any

import typer
from kalshi_core.auth import KalshiSigner
from kalshi_core.config import KalshiSettings
from kalshi_core.orderbook import OrderBookFeed
from kalshi_core.rest import KalshiRestClient
from kalshi_core.ws import KalshiWebSocket

from kalshiterm_server import db
from kalshiterm_server.config import ServerSettings
from kalshiterm_server.ingest.backfill import GapBackfiller
from kalshiterm_server.ingest.discovery import discover
from kalshiterm_server.ingest.stream import StreamIngestor
from kalshiterm_server.ingest.watchlist import WatchlistConfig, WatchlistController, load_config

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
                tasks: list[asyncio.Task[None]] = [asyncio.create_task(backfiller.run())]
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
                finally:
                    for task in tasks:
                        task.cancel()
                typer.echo(f"final stats: {ingestor.stats()}")
        finally:
            await engine.dispose()

    asyncio.run(run())
