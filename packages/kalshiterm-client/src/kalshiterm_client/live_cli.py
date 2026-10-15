"""``kterm watch`` and ``kterm alerts watch``: live views over the stream.

Both are built on ``StreamClient``, so they survive dropped connections and show each event
exactly once. Human notes (connecting, reconnecting, resyncing) go to stderr, so stdout is only
the data and can be piped; ``--json`` prints one JSON object per line. ``--count N`` stops after
N events, which is how scripts (and the tests) use them; otherwise Ctrl-C stops.
"""

import asyncio
import json
import sys
import time
from typing import Annotated, Any

import typer
from rich.live import Live

from kalshiterm_client import views
from kalshiterm_client.book import BookTracker
from kalshiterm_client.cli_common import current_profile, fail, make_console, plain
from kalshiterm_client.client import ServerClient
from kalshiterm_client.profiles import ProfileError
from kalshiterm_client.stream import StreamClient
from kalshiterm_client.stream_events import (
    AlertEvent,
    BookDeltaEvent,
    BookSnapshotEvent,
    Connected,
    Disconnected,
    Resync,
    TickerEvent,
    TradeEvent,
)

JsonOption = Annotated[bool, typer.Option("--json", help="One JSON object per line.")]
CountOption = Annotated[
    int | None, typer.Option("--count", min=1, help="Stop after this many events.")
]
REDRAW = 0.25  # seconds between redraws of a live book


def note(text: str) -> None:
    typer.echo(text, err=True)


def line(event: Any, as_json: bool) -> None:
    if as_json:
        typer.echo(json.dumps(plain(event)))
    else:
        make_console().print(event, markup=False, highlight=False, soft_wrap=True)


def run_until_done(work: Any) -> None:
    """Run ``work()``; Ctrl-C ends it quietly, a problem a person can act on is one line."""
    try:
        asyncio.run(work())
    except KeyboardInterrupt:
        note("stopped")
    except ProfileError as exc:
        raise fail(str(exc)) from exc


def watch_command(
    tickers: Annotated[list[str] | None, typer.Argument(help="Market tickers to follow.")] = None,
    watchlist: Annotated[bool, typer.Option("--watchlist", help="Follow your watchlist.")] = False,
    trades: Annotated[bool, typer.Option("--trades/--no-trades")] = True,
    ticker_updates: Annotated[bool, typer.Option("--ticker/--no-ticker")] = True,
    book: Annotated[
        bool, typer.Option("--book", help="Keep the orderbook live (first market).")
    ] = False,
    depth: Annotated[int, typer.Option(min=1, max=50, help="Book levels per side.")] = 8,
    count: CountOption = None,
    as_json: JsonOption = False,
) -> None:
    """Follow markets live: trades and ticker updates (and the book with --book)."""
    if bool(tickers) == watchlist:
        raise fail("give market tickers, or --watchlist, not both and not neither")
    channels = [
        c for c, on in (("trades", trades), ("ticker", ticker_updates), ("orderbook", book)) if on
    ]
    if not channels:
        raise fail("nothing to follow: enable --trades, --ticker or --book")
    profile = current_profile()
    tracker = BookTracker()
    interactive = book and sys.stdout.isatty() and not as_json

    async def work() -> None:
        """Follow the stream and show it until --count events have been seen or Ctrl-C.

        A line per trade or ticker update (JSON with --json). The order book is kept from the
        snapshots and deltas; with --book on a terminal it is redrawn live, otherwise the final book
        is printed at the end. Connection changes (connected, reconnected and resumed, disconnected,
        resynced) are shown as notes, never as data. A resync resets the book, because what was
        missed could not be replayed.
        """
        stream = StreamClient.for_profile(
            profile, channels=channels, markets=tickers or (), watchlist=watchlist
        )
        shown = tickers[0] if tickers else None  # the book on show
        seen = 0
        last_draw = 0.0
        live: Live | None = None
        if interactive:
            live = Live(
                views.tracker_book_view(shown or "", None, depth),
                console=make_console(),
                refresh_per_second=4,
            )
            live.start()
        note("following; Ctrl-C to stop")
        try:
            async for event in stream:
                if isinstance(event, Connected):
                    note("reconnected (resumed, nothing missed)" if event.resumed else "connected")
                    if event.unknown:
                        note(f"not found: {', '.join(event.unknown)}")
                elif isinstance(event, Disconnected):
                    note(f"disconnected ({event.reason}); retrying in {event.retry_in}s")
                elif isinstance(event, Resync):
                    tracker.reset()
                    note(f"resynced ({event.reason}): could not replay what was missed")
                elif isinstance(event, BookSnapshotEvent | BookDeltaEvent):
                    tracker.apply(event)
                    shown = shown or event.ticker
                    seen += 1
                    if as_json:
                        line(event, True)
                    elif live is not None and time.monotonic() - last_draw >= REDRAW:
                        last_draw = time.monotonic()
                        live.update(views.tracker_book_view(shown, tracker.book(shown), depth))
                elif isinstance(event, TradeEvent | TickerEvent):
                    line(event if as_json else views.event_line(event), as_json)
                    seen += 1
                if count is not None and seen >= count:
                    break
        finally:
            await stream.aclose()
            if live is not None:
                live.update(views.tracker_book_view(shown or "", tracker.book(shown or ""), depth))
                live.stop()
        if book and live is None and not as_json and shown:
            make_console().print(views.tracker_book_view(shown, tracker.book(shown), depth))

    run_until_done(work)


def alerts_watch(
    backlog: Annotated[
        bool, typer.Option("--backlog/--no-backlog", help="Show unacknowledged first.")
    ] = True,
    count: CountOption = None,
    as_json: JsonOption = False,
) -> None:
    """Follow your alerts live: the unacknowledged ones first, then new ones as they fire."""
    profile = current_profile()

    async def work() -> None:
        async with ServerClient.for_profile(profile) as api:
            newest = (await api.alerts_page(limit=1)).alerts  # the head, read BEFORE the backlog
            head = newest[0].id if newest else 0
            waiting = [a async for a in api.iter_alerts(unacknowledged=True)] if backlog else []
        printed: set[int] = set()
        seen = 0
        for alert in reversed(waiting):  # oldest first
            line(alert if as_json else views.alert_line(alert), as_json)
            printed.add(alert.id)
            seen += 1
            if count is not None and seen >= count:
                return
        stream = StreamClient.for_profile(profile, channels=["alerts"], alerts_since=head)
        note("following your alerts; Ctrl-C to stop")
        try:
            async for event in stream:
                if isinstance(event, Disconnected):
                    note(f"disconnected ({event.reason}); retrying in {event.retry_in}s")
                elif isinstance(event, Resync):
                    note(f"resynced ({event.reason})")
                elif isinstance(event, AlertEvent) and event.id not in printed:
                    # (an alert can land between reading the backlog and subscribing)
                    printed.add(event.id)
                    line(event if as_json else views.alert_line(event), as_json)
                    seen += 1
                    if count is not None and seen >= count:
                        break
        finally:
            await stream.aclose()

    run_until_done(work)


def register(app: typer.Typer, alerts_app: typer.Typer) -> None:
    app.command("watch")(watch_command)
    alerts_app.command("watch")(alerts_watch)
