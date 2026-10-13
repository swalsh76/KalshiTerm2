"""Typed models for the account (portfolio) endpoints: balance, positions, orders, fills.

Read side only. Shapes were measured on a live account on 2026-10-13 (PLAN §10.7, slice 6.1):
money is a fixed-point dollar string (4 or 6 decimals) or, on a few older fields, integer cents;
counts are fixed-point strings with two decimals; everything is parsed to ``Decimal`` / ``int``,
never ``float``. Kalshi keeps every order on the YES book, so ``side`` is always ``yes``; the
user's intent is ``action`` (buy / sell) with ``outcome_side`` (yes / no).
"""

from datetime import datetime
from decimal import Decimal

from kalshi_core.models import KalshiModel


class ShardBalance(KalshiModel):
    exchange_index: int
    balance: Decimal  # dollars, e.g. "10.3490"


class Balance(KalshiModel):
    # ``balance`` is ``balance_dollars`` truncated to whole cents ($10.3490 -> 1034): always use
    # the dollar value, the cents field silently loses up to 0.99 of a cent.
    balance: int
    balance_dollars: Decimal
    portfolio_value: int = 0  # cents
    updated_ts: int | None = None  # unix seconds
    balance_breakdown: list[ShardBalance] = []

    def shard(self, exchange_index: int) -> Decimal:
        """Dollars available on one exchange shard (zero if it is not listed)."""
        for row in self.balance_breakdown:
            if row.exchange_index == exchange_index:
                return row.balance
        return Decimal(0)


class MarketPosition(KalshiModel):
    ticker: str
    exchange_index: int = 0
    position_fp: Decimal = Decimal(0)  # net YES contracts; negative = long NO
    total_traded_dollars: Decimal | None = None
    market_exposure_dollars: Decimal | None = None
    realized_pnl_dollars: Decimal | None = None
    fees_paid_dollars: Decimal | None = None
    last_updated_ts: datetime | None = None


class EventPosition(KalshiModel):
    event_ticker: str
    total_cost_dollars: Decimal | None = None
    total_cost_shares_fp: Decimal | None = None
    event_exposure_dollars: Decimal | None = None
    realized_pnl_dollars: Decimal | None = None
    fees_paid_dollars: Decimal | None = None


class PositionsPage(KalshiModel):
    market_positions: list[MarketPosition] = []
    event_positions: list[EventPosition] = []
    cursor: str = ""


class Order(KalshiModel):
    order_id: str
    ticker: str
    status: str  # resting, canceled, executed, ...
    client_order_id: str = ""
    user_id: str = ""
    type: str = "limit"
    action: str  # buy / sell
    side: str = "yes"  # the book it sits on; see the module note
    outcome_side: str  # yes / no
    book_side: str = ""  # bid / ask
    yes_price_dollars: Decimal
    no_price_dollars: Decimal
    initial_count_fp: Decimal
    fill_count_fp: Decimal
    remaining_count_fp: Decimal
    taker_fees_dollars: Decimal = Decimal(0)
    maker_fees_dollars: Decimal = Decimal(0)
    taker_fill_cost_dollars: Decimal = Decimal(0)
    maker_fill_cost_dollars: Decimal = Decimal(0)
    self_trade_prevention_type: str = ""
    exchange_index: int = 0
    subaccount_number: int = 0
    created_time: datetime | None = None
    last_update_time: datetime | None = None
    expiration_time: datetime | None = None

    @property
    def price_dollars(self) -> Decimal:
        """The limit price of the outcome being traded (the user's own side)."""
        return self.yes_price_dollars if self.outcome_side == "yes" else self.no_price_dollars

    @property
    def is_resting(self) -> bool:
        return self.status == "resting"


class OrdersPage(KalshiModel):
    orders: list[Order] = []
    cursor: str = ""


class Fill(KalshiModel):
    fill_id: str
    trade_id: str = ""
    order_id: str
    ticker: str
    market_ticker: str = ""
    action: str
    side: str = "yes"
    outcome_side: str
    book_side: str = ""
    count_fp: Decimal
    yes_price_dollars: Decimal
    no_price_dollars: Decimal
    fee_cost: Decimal = Decimal(0)  # dollars
    is_taker: bool = False
    exchange_index: int = 0
    subaccount_number: int = 0
    created_time: datetime | None = None
    ts: int | None = None  # unix seconds


class FillsPage(KalshiModel):
    fills: list[Fill] = []
    cursor: str = ""


class Settlement(KalshiModel):
    ticker: str
    event_ticker: str = ""
    market_result: str = ""
    yes_count_fp: Decimal = Decimal(0)
    no_count_fp: Decimal = Decimal(0)
    yes_total_cost_dollars: Decimal = Decimal(0)
    no_total_cost_dollars: Decimal = Decimal(0)
    fee_cost: Decimal = Decimal(0)  # dollars
    revenue: int = 0  # cents
    value: int | None = None  # cents per winning contract
    exchange_index: int = 0
    settled_time: datetime | None = None


class SettlementsPage(KalshiModel):
    settlements: list[Settlement] = []
    cursor: str = ""


class QueuePosition(KalshiModel):
    order_id: str
    market_ticker: str
    queue_position_fp: Decimal  # contracts ahead of the order


class QueuePositions(KalshiModel):
    queue_positions: list[QueuePosition] = []
