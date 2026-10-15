"""``kterm markets | market | candles | trades | book | analytics | arbitrage``.

Thin consumers of ``ServerClient``: each command fetches typed models and either prints them as
tables (``views``) or, with ``--json``, as the models serialized (decimals as exact strings).
"""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Annotated, Any, Literal

import typer

from kalshiterm_client import views
from kalshiterm_client.cli_common import emit_json, fail, make_console, run_with_api
from kalshiterm_client.client import ServerClient
from kalshiterm_client.errors import ApiProblem

JsonOption = Annotated[bool, typer.Option("--json", help="Machine-readable output.")]
Ticker = Annotated[str, typer.Argument(help="A market ticker, e.g. KXBTC-26OCT0913-T82799.99.")]
MAX_ROWS = 5000  # `markets --limit`: the server holds ~800,000 markets; never fetch them all


def parse_time(text: str | None, name: str) -> datetime | None:
    """ISO-8601; a time without an offset is read as UTC."""
    if text is None:
        return None
    try:
        moment = datetime.fromisoformat(text)
    except ValueError as exc:
        raise typer.BadParameter(
            f"{text!r} is not an ISO-8601 time (e.g. 2026-10-12T18:00)", param_hint=name
        ) from exc
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def parse_dollars(text: str | None, name: str) -> Decimal | None:
    if text is None:
        return None
    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise typer.BadParameter(f"{text!r} is not a number", param_hint=name) from exc


def markets_command(
    query: Annotated[str | None, typer.Argument(help="Words in the ticker or event title.")] = None,
    status: Annotated[str | None, typer.Option(help="active, closed, finalized, ...")] = None,
    event: Annotated[str | None, typer.Option(help="Only this event's markets.")] = None,
    series: Annotated[str | None, typer.Option(help="Only this series' markets.")] = None,
    limit: Annotated[int, typer.Option(min=1, max=MAX_ROWS, help="How many to list.")] = 50,
    as_json: JsonOption = False,
) -> None:
    """Search markets, in ticker order (a search matches tickers and event titles)."""
    if query is not None and len(query) < 2:
        raise fail("a search needs at least two characters")

    async def work(api: ServerClient) -> list[Any]:
        found: list[Any] = []
        async for market in api.iter_markets(
            status=status, event=event, series=series, q=query, limit=min(limit, 1000)
        ):
            found.append(market)
            if len(found) >= limit:
                break
        return found

    found = run_with_api(work)
    if as_json:
        return emit_json({"markets": found})
    make_console().print(views.markets_table(found))
    if len(found) == limit:
        typer.echo(f"(showing the first {limit}; narrow the search or raise --limit)")


def market_command(
    ticker: Ticker,
    rules: Annotated[bool, typer.Option("--rules", help="Print the full rules text.")] = False,
    as_json: JsonOption = False,
) -> None:
    """One market: what it is, its last quote and the latest analytics."""

    async def work(api: ServerClient) -> tuple[Any, Any, Any]:
        detail = await api.market(ticker)
        tick, results = await asyncio.gather(
            api.ticks_page(ticker, limit=1),
            api.market_results(ticker, max_age_seconds=3600),
            return_exceptions=True,
        )
        latest = tick.ticks[-1] if not isinstance(tick, BaseException) and tick.ticks else None
        analytics = [] if isinstance(results, ApiProblem | BaseException) else results.results
        return detail, latest, analytics

    detail, latest, analytics = run_with_api(work)
    if as_json:
        return emit_json({"market": detail, "latest_tick": latest, "analytics": analytics})
    make_console().print(views.market_view(detail, latest, analytics, all_rules=rules))


def candles_command(
    ticker: Ticker,
    interval: Annotated[Literal["1m", "1h"], typer.Option(help="Bar size.")] = "1m",
    source: Annotated[
        Literal["trades", "ticker"],
        typer.Option(help="Bars from trades or from ticker updates."),
    ] = "trades",
    limit: Annotated[int, typer.Option(min=1, max=1000)] = 30,
    start: Annotated[
        str | None, typer.Option(help="From this time (ISO-8601, UTC if no offset).")
    ] = None,
    end: Annotated[str | None, typer.Option(help="Before this time (default: now).")] = None,
    as_json: JsonOption = False,
) -> None:
    """Candles, oldest first. Without --start: the most recent bars before --end."""
    begin, finish = parse_time(start, "--start"), parse_time(end, "--end")

    async def work(api: ServerClient) -> Any:
        return await api.candles_page(
            ticker, interval=interval, source=source, start=begin, end=finish, limit=limit
        )

    page = run_with_api(work)
    if as_json:
        return emit_json(page)
    make_console().print(views.candles_table(page))
    if page.next_cursor:
        typer.echo(f"(more bars exist after {views.stamp(page.bars[-1].time)}; use --start)")


def trades_command(
    ticker: Ticker,
    limit: Annotated[int, typer.Option(min=1, max=1000)] = 20,
    start: Annotated[
        str | None, typer.Option(help="From this time (ISO-8601, UTC if no offset).")
    ] = None,
    end: Annotated[str | None, typer.Option(help="Before this time (default: now).")] = None,
    as_json: JsonOption = False,
) -> None:
    """Executed trades, oldest first. Without --start: the most recent ones."""
    begin, finish = parse_time(start, "--start"), parse_time(end, "--end")

    async def work(api: ServerClient) -> Any:
        return await api.trades_page(ticker, start=begin, end=finish, limit=limit)

    page = run_with_api(work)
    if as_json:
        return emit_json(page)
    make_console().print(views.trades_table(ticker, page.trades))


def book_command(
    ticker: Ticker,
    at: Annotated[str | None, typer.Option(help="The book at this time (default: now).")] = None,
    depth: Annotated[int, typer.Option(min=1, max=1000, help="Levels per side.")] = 10,
    as_json: JsonOption = False,
) -> None:
    """The orderbook (only markets that have been watched have one)."""
    moment = parse_time(at, "--at")

    async def work(api: ServerClient) -> Any:
        return await api.orderbook(ticker, at=moment, depth=depth)

    book = run_with_api(work)
    if as_json:
        return emit_json(book)
    make_console().print(views.book_view(book, depth))


def analytics_command(
    ticker: Annotated[str | None, typer.Argument(help="A market ticker.")] = None,
    event: Annotated[str | None, typer.Option(help="An event ticker instead of a market.")] = None,
    metric: Annotated[
        str | None, typer.Option(help="History of one metric, e.g. market_quotes.spread.")
    ] = None,
    max_age: Annotated[
        int, typer.Option(min=1, max=86400, help="Ignore values older than this many seconds.")
    ] = 3600,
    limit: Annotated[int, typer.Option(min=1, max=1000, help="History points.")] = 20,
    as_json: JsonOption = False,
) -> None:
    """Analyzer results for a market (or --event), or one metric's history (--metric)."""
    if (ticker is None) == (event is None):
        raise fail("give a market ticker or --event, not both and not neither")
    if metric is not None and ticker is None:
        raise fail("--metric needs a market ticker")

    async def work(api: ServerClient) -> Any:
        if metric is not None:
            assert ticker is not None
            return await api.market_history_page(ticker, metric, limit=limit)
        if event is not None:
            return await api.event_results(event, max_age_seconds=max_age)
        assert ticker is not None
        return await api.market_results(ticker, max_age_seconds=max_age)

    result = run_with_api(work)
    if as_json:
        return emit_json(result)
    console = make_console()
    if metric is not None:
        console.print(views.history_table(result.ticker, result.metric, result.unit, result.points))
    elif not result.results:
        where = event or ticker
        typer.echo(
            f"no analytics for {where} in the last {max_age}s (analytics cover watched markets)"
        )
    else:
        console.print(views.analytics_table(result.results, title=event or ticker))


def arbitrage_command(
    min_profit: Annotated[
        str | None,
        typer.Option(help="Only events with at least this profit (dollars); 0 = profitable only."),
    ] = None,
    limit: Annotated[int, typer.Option(min=1, max=200)] = 20,
    max_age: Annotated[int, typer.Option(min=1, max=3600)] = 300,
    as_json: JsonOption = False,
) -> None:
    """Events whose YES bids sum to more than $1 right now, best profit first."""
    floor = parse_dollars(min_profit, "--min-profit")

    async def work(api: ServerClient) -> Any:
        return await api.arbitrage(min_profit=floor, max_age_seconds=max_age, limit=limit)

    found = run_with_api(work)
    if as_json:
        return emit_json(found)
    make_console().print(views.arbitrage_table(found.opportunities, found.total))


def register(app: typer.Typer) -> None:
    app.command("markets")(markets_command)
    app.command("market")(market_command)
    app.command("candles")(candles_command)
    app.command("trades")(trades_command)
    app.command("book")(book_command)
    app.command("analytics")(analytics_command)
    app.command("arbitrage")(arbitrage_command)
