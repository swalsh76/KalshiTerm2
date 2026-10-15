"""How the server's data looks in a terminal.

Everything here is a pure function from typed models to ``rich`` renderables, so it is tested
without a server or a terminal. Rules that matter:

* Numbers come from ``Decimal`` and are never rounded: trailing zeros are dropped (0.560000 shows
  as 0.56, 0.123456 stays 0.123456), and a price keeps at least two decimals.
* Every string that came from the server is wrapped in ``Text``: rich would otherwise read a
  title like "Will [bold]X[/bold] win?" as markup and let Kalshi's text change the display.
* Times are UTC and say so.
"""

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from itertools import zip_longest

from rich import box
from rich.console import Group, RenderableType
from rich.table import Table
from rich.text import Text

from kalshiterm_client.book import Book
from kalshiterm_client.models import (
    Alert,
    AlertMetric,
    AlertRule,
    AnalyticsValue,
    Bar,
    CandlePage,
    HistoryPoint,
    Market,
    MarketDetail,
    Opportunity,
    OrderBook,
    Tick,
    Trade,
    Watchlist,
)
from kalshiterm_client.stream_events import AlertEvent, TickerEvent, TradeEvent

DASH = "-"


def num(value: Decimal | None) -> str:
    """An exact number without trailing zeros (``12.50`` -> ``12.5``, ``100.00`` -> ``100``)."""
    if value is None:
        return DASH
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in ("-0", "") else text


def money(value: Decimal | None) -> str:
    """Dollars: exact, at least two decimals (``0.560000`` -> ``0.56``, ``1`` -> ``1.00``)."""
    if value is None:
        return DASH
    text = format(value, "f")
    whole, _, fraction = text.partition(".")
    fraction = fraction.rstrip("0").ljust(2, "0")
    return f"{whole}.{fraction}"


def stamp(value: datetime | None, seconds: bool = False) -> str:
    """``2026-10-12 18:00`` (UTC); the column header says UTC."""
    if value is None:
        return DASH
    return value.strftime("%Y-%m-%d %H:%M:%S" if seconds else "%Y-%m-%d %H:%M")


def clip(value: str | None, width: int) -> Text:
    """Server text, shortened with an ellipsis, never interpreted as markup."""
    if not value:
        return Text(DASH)
    return Text(value if len(value) <= width else value[: width - 1] + "…")


def _table(*columns: tuple[str, str], title: str | None = None) -> Table:
    table = Table(box=box.SIMPLE_HEAD, title=title, title_justify="left", pad_edge=False)
    for name, justify in columns:
        table.add_column(name, justify=justify, no_wrap=True)  # type: ignore[arg-type]
    return table


# ---------------------------------------------------------------- markets


def markets_table(markets: Sequence[Market]) -> Table:
    table = _table(
        ("TICKER", "left"), ("STATUS", "left"), ("TITLE", "left"), ("CLOSES (UTC)", "left")
    )
    for m in markets:
        table.add_row(Text(m.ticker), Text(m.status), clip(m.event_title, 56), stamp(m.close_time))
    return table


def market_view(
    market: MarketDetail,
    tick: Tick | None,
    analytics: Sequence[AnalyticsValue],
    *,
    all_rules: bool = False,
) -> RenderableType:
    """The page for ``kterm market``: one market in detail.

    Ticker and status, event, yes/no labels, open, close and expiry times, strike and the result
    if it has settled; then the last quote and the analytics when given, and the rules text (cut
    to 400 characters unless ``all_rules``).
    """
    head = Text()
    head.append(market.ticker, style="bold")
    head.append(f"   {market.status}")
    if market.stage != "full":
        head.append(f"   (kept as: {market.stage})")
    facts = Table.grid(padding=(0, 2))
    facts.add_column(style="dim", no_wrap=True)
    facts.add_column()

    def row(label: str, value: str | Text) -> None:
        facts.add_row(label, value if isinstance(value, Text) else Text(value))

    row("event", clip(market.event_title, 90))
    row("yes / no", Text(f"{market.yes_label or DASH}  /  {market.no_label or DASH}"))
    row("opens (UTC)", stamp(market.open_time))
    row("closes (UTC)", stamp(market.close_time))
    row("expires (UTC)", stamp(market.expiration_time))
    if market.strike_type:
        strike = market.strike_type
        if market.floor_strike is not None:
            strike += f"  {num(market.floor_strike)}"
        if market.cap_strike is not None:
            strike += f" .. {num(market.cap_strike)}"
        row("strike", strike)
    if market.result or market.settlement_time:
        row(
            "result",
            f"{market.result or DASH}  settled {stamp(market.settlement_time)} "
            f"at {money(market.settlement_value)}",
        )
    parts: list[RenderableType] = [head, facts]
    if tick is not None:
        quote = Table.grid(padding=(0, 2))
        quote.add_column(style="dim")
        quote.add_column()
        quote.add_row(
            f"last quote ({stamp(tick.time, seconds=True)} UTC)",
            f"price {money(tick.price)}   bid {money(tick.yes_bid)} x {num(tick.yes_bid_size)}   "
            f"ask {money(tick.yes_ask)} x {num(tick.yes_ask_size)}",
        )
        quote.add_row("", f"volume {num(tick.volume)}   open interest {num(tick.open_interest)}")
        parts += [Text(""), quote]
    if analytics:
        parts += [Text(""), analytics_table(analytics, title="analytics")]
    rules = market.rules_primary
    if rules:
        shown = rules if all_rules or len(rules) <= 400 else rules[:399] + "… (--rules for all)"
        parts += [Text(""), Text("rules", style="dim"), Text(shown)]
    return Group(*parts)


# ---------------------------------------------------------------- market data


def candles_table(page: CandlePage) -> Table:
    ticker_bars = page.source == "ticker"
    columns = [
        ("TIME (UTC)", "left"),
        ("OPEN", "right"),
        ("HIGH", "right"),
        ("LOW", "right"),
        ("CLOSE", "right"),
        ("VOLUME", "right"),
    ]
    columns += (
        [("BID", "right"), ("ASK", "right"), ("TICKS", "right")]
        if ticker_bars
        else [("TRADES", "right")]
    )
    table = _table(*columns, title=f"{page.ticker}  {page.interval} bars from {page.source}")
    for bar in page.bars:
        cells = [
            stamp(bar.time),
            money(bar.open),
            money(bar.high),
            money(bar.low),
            money(bar.close),
            num(bar.volume),
        ]
        cells += _bar_tail(bar, ticker_bars)
        table.add_row(*cells)
    return table


def _bar_tail(bar: Bar, ticker_bars: bool) -> list[str]:
    if ticker_bars:
        return [
            money(bar.yes_bid),
            money(bar.yes_ask),
            str(bar.ticks if bar.ticks is not None else DASH),
        ]
    return [str(bar.trades if bar.trades is not None else DASH)]


def trades_table(ticker: str, trades: Sequence[Trade]) -> Table:
    table = _table(
        ("TIME (UTC)", "left"),
        ("YES PRICE", "right"),
        ("NO PRICE", "right"),
        ("CONTRACTS", "right"),
        ("TAKER", "left"),
        ("", "left"),
        title=ticker,
    )
    for t in trades:
        table.add_row(
            stamp(t.time, seconds=True),
            money(t.yes_price),
            money(t.no_price),
            num(t.count),
            Text(t.taker_side or DASH),
            "block" if t.is_block_trade else "",
        )
    return table


def book_view(book: OrderBook, depth: int) -> RenderableType:
    table = _table(
        ("YES BID", "right"),
        ("SIZE", "right"),
        ("", "left"),
        ("NO BID", "right"),
        ("SIZE", "right"),
        title=f"{book.ticker} as of {stamp(book.as_of, seconds=True)} UTC",
    )
    yes, no = book.yes[:depth], book.no[:depth]
    for y, n in zip_longest(yes, no):
        table.add_row(
            money(y.price) if y else "",
            num(y.size) if y else "",
            "|",
            money(n.price) if n else "",
            num(n.size) if n else "",
        )
    summary = Text(
        f"best YES bid {money(book.best_yes_bid)}   best YES ask {money(book.best_yes_ask)}   "
        f"(snapshot {stamp(book.snapshot_time, seconds=True)} UTC, "
        f"{book.changes_applied} changes applied)"
    )
    parts: list[RenderableType] = [table, summary]
    if not book.complete:
        why = "; ".join(
            f"{g.reason} {stamp(g.start, seconds=True)}..{stamp(g.end, seconds=True)}"
            for g in book.gaps
        )
        if book.approximate_snapshot:
            why = "the snapshot is approximate" + (f"; {why}" if why else "")
        parts.append(
            Text(
                "WARNING: this book may be incomplete "
                f"({why or 'a stored change is inconsistent'})",
                style="bold",
            )
        )
    return Group(*parts)


# ---------------------------------------------------------------- analytics


def analytics_table(values: Sequence[AnalyticsValue], title: str | None = None) -> Table:
    table = _table(
        ("METRIC", "left"), ("VALUE", "right"), ("UNIT", "left"), ("AGE", "right"), title=title
    )
    for v in values:
        shown = money(v.value) if v.unit == "dollars" else num(v.value)
        table.add_row(Text(v.metric), shown, Text(v.unit or DASH), f"{v.age_seconds}s")
    return table


def history_table(
    ticker: str, metric: str, unit: str | None, points: Sequence[HistoryPoint]
) -> Table:
    table = _table(
        ("TIME (UTC)", "left"),
        (f"VALUE ({unit or 'unit unknown'})", "right"),
        title=f"{ticker}  {metric}",
    )
    for p in points:
        table.add_row(
            stamp(p.time, seconds=True), money(p.value) if unit == "dollars" else num(p.value)
        )
    return table


def arbitrage_table(opportunities: Sequence[Opportunity], total: int) -> RenderableType:
    caveat = Text(
        "Indications from stored quotes, profit is after Kalshi's taker fees; "
        "re-read the live book before trading.",
        style="dim",
    )
    if not opportunities:
        return Group(Text("no opportunities right now"), caveat)
    table = _table(
        ("EVENT", "left"),
        ("BID SUM", "right"),
        ("EDGE", "right"),
        ("CONTRACTS", "right"),
        ("FEES", "right"),
        ("PROFIT", "right"),
        ("LEGS", "right"),
        ("QUOTE AGE", "right"),
    )
    for o in opportunities:
        legs = f"{o.legs}/{o.legs_total}" if o.legs is not None else DASH
        age = f"{o.oldest_quote_age_seconds}s" if o.oldest_quote_age_seconds is not None else DASH
        table.add_row(
            Text(o.event_ticker),
            money(o.bid_sum),
            money(o.edge),
            num(o.size),
            money(o.fees),
            money(o.profit),
            legs,
            age,
        )
    shown = (
        f"{len(opportunities)} of {total} shown" if total > len(opportunities) else f"{total} found"
    )
    return Group(table, Text(shown), caveat)


def metrics_table(metrics: Sequence[AlertMetric]) -> Table:
    table = _table(
        ("METRIC", "left"),
        ("ON", "left"),
        ("UNIT", "left"),
        ("COMPARISONS", "left"),
        ("MEANING", "left"),
    )
    for m in metrics:
        table.add_row(Text(m.metric), m.subject, m.unit, ", ".join(m.ops), Text(m.meaning))
    return table


# ---------------------------------------------------------------- your watchlist and alerts

OPS = {"gt": ">", "lt": "<", "abs_gt": "|change| >"}


def amount(value: Decimal | None, unit: str | None) -> str:
    """A metric value in its own unit: dollars keep their cents, contracts and seconds do not."""
    return money(value) if unit == "dollars" else num(value)


def condition(op: str, threshold: Decimal, unit: str | None) -> str:
    return f"{OPS.get(op, op)} {amount(threshold, unit)}"


def watchlist_table(watchlist: Watchlist) -> RenderableType:
    table = _table(
        ("TICKER", "left"),
        ("STATUS", "left"),
        ("ORDERBOOK", "left"),
        ("ADDED (UTC)", "left"),
        ("EVENT", "left"),
    )
    for m in watchlist.markets:
        table.add_row(
            Text(m.ticker),
            Text(m.status),
            "capturing" if m.capturing else "waiting",
            stamp(m.added_at),
            clip(m.event_title, 50),
        )
    return Group(table, Text(f"{len(watchlist.markets)} of {watchlist.limit} places used"))


def rules_table(rules: Sequence[AlertRule], limit: int) -> RenderableType:
    table = _table(
        ("ID", "right"),
        ("NAME", "left"),
        ("ON", "left"),
        ("METRIC", "left"),
        ("CONDITION", "left"),
        ("COOLDOWN", "right"),
        ("STATE", "left"),
        ("LAST FIRED (UTC)", "left"),
    )
    for r in rules:
        state = "disabled" if not r.enabled else ("triggered" if r.active else "armed")
        table.add_row(
            str(r.id),
            clip(r.name, 24),
            Text(r.ticker or r.event_ticker or DASH),
            Text(r.metric),
            condition(r.op, r.threshold, r.unit),
            f"{r.cooldown_seconds}s",
            state,
            stamp(r.last_fired_at),
        )
    return Group(table, Text(f"{len(rules)} of {limit} rules used"))


def alerts_table(alerts: Sequence[Alert]) -> Table:
    table = _table(
        ("ID", "right"),
        ("FIRED (UTC)", "left"),
        ("RULE", "left"),
        ("ON", "left"),
        ("METRIC", "left"),
        ("CONDITION", "left"),
        ("VALUE", "right"),
        ("", "left"),
    )
    for a in alerts:
        table.add_row(
            str(a.id),
            stamp(a.fired_at, seconds=True),
            clip(a.rule_name, 24),
            Text(a.ticker or a.event_ticker or DASH),
            Text(a.metric),
            condition(a.op, a.threshold, a.unit),
            amount(a.value, a.unit),
            "acknowledged" if a.acknowledged_at else "NEW",
        )
    return table


def alert_line(a: Alert | AlertEvent) -> Text:
    """One line for a live alert."""
    subject = a.ticker or a.event_ticker or DASH
    return Text(
        f"{stamp(a.fired_at, seconds=True)} UTC  ALERT #{a.id}  {a.rule_name or '(unnamed)'}  "
        f"{subject}  {a.metric} {condition(a.op, a.threshold, a.unit)}  "
        f"value {amount(a.value, a.unit)}"
    )


# ---------------------------------------------------------------- live events


def _clock(value: datetime) -> str:
    return value.strftime("%H:%M:%S.") + f"{value.microsecond // 1000:03d}"


def event_line(event: TradeEvent | TickerEvent) -> Text:
    """One line per live trade or ticker update (times are UTC)."""
    if isinstance(event, TradeEvent):
        block = "  block" if event.is_block_trade else ""
        return Text(
            f"{_clock(event.time)}  TRADE  {event.ticker}  yes {money(event.yes_price)} x "
            f"{num(event.count)}  taker {event.taker_side or DASH}{block}"
        )
    return Text(
        f"{_clock(event.time)}  TICK   {event.ticker}  last {money(event.price)}  "
        f"bid {money(event.yes_bid)} x {num(event.yes_bid_size)}  "
        f"ask {money(event.yes_ask)} x {num(event.yes_ask_size)}  volume {num(event.volume)}"
    )


def tracker_book_view(ticker: str, book: Book | None, depth: int) -> RenderableType:
    """The live book kept by ``BookTracker`` (not the server's reconstruction of a past moment)."""
    if book is None:
        return Text(f"{ticker}: waiting for the book...")
    table = _table(
        ("YES BID", "right"),
        ("SIZE", "right"),
        ("", "left"),
        ("NO BID", "right"),
        ("SIZE", "right"),
        title=ticker,
    )
    for y, n in zip_longest(book.levels("yes", depth), book.levels("no", depth)):
        table.add_row(
            money(y[0]) if y else "",
            num(y[1]) if y else "",
            "|",
            money(n[0]) if n else "",
            num(n[1]) if n else "",
        )
    summary = Text(
        f"best YES bid {money(book.best_yes_bid)}   best YES ask {money(book.best_yes_ask)}"
    )
    parts: list[RenderableType] = [table, summary]
    if book.inconsistent:
        parts.append(
            Text("WARNING: a change removed more than the book held; it may be wrong", style="bold")
        )
    return Group(*parts)
