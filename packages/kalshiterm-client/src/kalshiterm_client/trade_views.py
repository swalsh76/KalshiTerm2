"""How the account looks in a terminal: balance, positions, orders, fills, and what an order did.

Pure functions from the typed models to ``rich`` renderables (tested without a terminal), with the
same rules as ``views``: numbers come from ``Decimal`` and are never rounded, text that came from
Kalshi is wrapped in ``Text`` so it cannot change the display, times are UTC.
"""

from collections.abc import Sequence
from decimal import Decimal

from kalshi_core.fixedpoint import E6
from kalshi_core.portfolio import Balance, Fill, MarketPosition, Order
from kalshi_core.trading import AmendedOrder, CreatedOrder
from rich.console import Group, RenderableType
from rich.table import Table
from rich.text import Text

from kalshiterm_client.orders import Amendment, Placement
from kalshiterm_client.views import DASH, _table, money, num, stamp


def fitted_table(
    columns: Sequence[tuple[str, str]],
    rows: Sequence[Sequence[str | Text]],
    title: str | None = None,
) -> Table:
    """Like ``views._table`` but no column is ever narrower than its longest cell, so an order id
    or a ticker is always complete (you copy them into ``kterm cancel``). The caller makes the
    console wide enough (``trade_cli.show``)."""
    table = _table(*columns, title=title)
    for column, (header, _) in zip(table.columns, columns, strict=True):
        index = table.columns.index(column)
        longest = max((Text(str(r[index])).cell_len for r in rows), default=0)
        column.min_width = max(len(header), longest)
    for row in rows:
        table.add_row(*row)
    return table


def balance_view(balance: Balance, halted_reason: str | None = None) -> RenderableType:
    table = _table(("SHARD", "right"), ("AVAILABLE ($)", "right"), title="Balance")
    for row in sorted(balance.balance_breakdown, key=lambda r: r.exchange_index):
        table.add_row(str(row.exchange_index), money(row.balance))
    table.add_row("total", money(balance.balance_dollars))
    parts: list[RenderableType] = []
    if halted_reason is not None:
        parts.append(
            Text(
                f"TRADING HALTED ({halted_reason or 'kill switch'}): kterm resume", style="bold red"
            )
        )
    parts.append(table)
    parts.append(Text(f"portfolio value: {money(Decimal(balance.portfolio_value) / 100)}"))
    return Group(*parts)


def _position_label(count: Decimal) -> str:
    if count == 0:
        return "flat"
    return f"{num(abs(count))} {'YES' if count > 0 else 'NO'}"


def positions_table(positions: Sequence[MarketPosition]) -> RenderableType:
    if not positions:
        return Text("no open positions")
    rows = [
        (
            Text(p.ticker),
            str(p.exchange_index),
            _position_label(p.position_fp),
            money(p.market_exposure_dollars),
            money(p.realized_pnl_dollars),
            money(p.fees_paid_dollars),
        )
        for p in positions
    ]
    return fitted_table(
        [
            ("TICKER", "left"),
            ("SHARD", "right"),
            ("POSITION", "right"),
            ("EXPOSURE ($)", "right"),
            ("REALIZED ($)", "right"),
            ("FEES ($)", "right"),
        ],
        rows,
    )


def _own_side(outcome_side: str, book_side: str) -> str:
    action = "BUY" if book_side == "bid" else "SELL"
    return f"{action} {outcome_side.upper()}"


def orders_table(orders: Sequence[Order], title: str | None = None) -> RenderableType:
    """Orders as Kalshi's YES book holds them: a BID buys YES, an ASK sells YES (which is the same
    as buying NO at 1 - price), so both prices are shown. Kalshi's own action / outcome labels on
    an order are not dependable (a buy of NO is labelled "sell, no, ask"), so they are not used."""
    if not orders:
        return Text("no orders")
    rows = [
        (
            Text(o.order_id),
            Text(o.ticker),
            o.book_side.upper() or DASH,
            money(o.yes_price_dollars),
            money(o.no_price_dollars),
            num(o.fill_count_fp),
            num(o.remaining_count_fp),
            Text(o.status),
            stamp(o.created_time),
        )
        for o in orders
    ]
    return fitted_table(
        [
            ("ORDER", "left"),
            ("TICKER", "left"),
            ("BOOK", "left"),
            ("YES ($)", "right"),
            ("NO ($)", "right"),
            ("FILLED", "right"),
            ("LEFT", "right"),
            ("STATUS", "left"),
            ("CREATED (UTC)", "left"),
        ],
        rows,
        title,
    )


ORDERS_NOTE = (
    "BID = buy YES (or sell NO); ASK = sell YES (or buy NO at the NO price shown). "
    "`kterm amend --price` takes the YES price."
)


def fills_table(fills: Sequence[Fill]) -> RenderableType:
    """Fills on the YES book, like ``orders_table``: Kalshi's action / outcome labels on a fill
    are not dependable (a sale of YES at 1c is labelled "sell, no, 0.99"), the book side and the
    two prices are."""
    if not fills:
        return Text("no fills")
    rows = [
        (
            stamp(f.created_time, seconds=True),
            Text(f.ticker),
            f.book_side.upper() or DASH,
            num(f.count_fp),
            money(f.yes_price_dollars),
            money(f.no_price_dollars),
            money(f.fee_cost),
            "taker" if f.is_taker else "maker",
        )
        for f in fills
    ]
    return fitted_table(
        [
            ("TIME (UTC)", "left"),
            ("TICKER", "left"),
            ("BOOK", "left"),
            ("COUNT", "right"),
            ("YES ($)", "right"),
            ("NO ($)", "right"),
            ("FEE ($)", "right"),
            ("ROLE", "left"),
        ],
        rows,
    )


def placement_lines(placement: Placement) -> list[str]:
    created: CreatedOrder = placement.created
    lines = []
    for t in placement.transfers:
        lines.append(
            f"moved ${t.amount_e6 / E6:.4f} from shard {t.source} to shard {t.destination}"
        )
    how = " (found by looking after an unclear answer)" if created.recovered else ""
    lines.append(f"order {created.order_id} placed{how}")
    lines.append(
        f"filled {num(created.fill_count)}, resting {num(created.remaining_count)}; "
        f"client id {placement.client_order_id}"
    )
    if created.average_fill_price is not None:
        lines.append(f"average fill price ${money(created.average_fill_price)}")
    return lines


def amendment_lines(amendment: Amendment) -> list[str]:
    result: AmendedOrder = amendment.result
    lines = []
    for t in amendment.transfers:
        lines.append(
            f"moved ${t.amount_e6 / E6:.4f} from shard {t.source} to shard {t.destination}"
        )
    lines.append(f"order {amendment.order_id} amended")
    lines.append(
        f"filled {num(result.fill_count) if result.fill_count is not None else DASH}, "
        f"resting {num(result.remaining_count) if result.remaining_count is not None else DASH}"
    )
    return lines
