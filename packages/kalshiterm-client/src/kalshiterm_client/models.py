"""The server's JSON as typed objects (PLAN §10.6).

Prices, sizes and counts arrive as decimal strings and become ``Decimal`` here, exactly: nothing
in this package turns a market value into a ``float``. Unknown fields are ignored so a newer server
does not break an older client; the repository's contract tests fail instead if the server sends a
field these models do not cover, which is how the two are kept in step.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ApiModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore", populate_by_name=True)


# ---------------------------------------------------------------- who you are, how it is


class Me(ApiModel):
    user: str
    role: str
    token_id: int


class StatusReport(ApiModel):
    """The operator's report. ``problems`` is typed; the rest stays as the server sent it in
    ``data`` (it is wide and meant to be read, not programmed against)."""

    time: datetime
    problems: list[str]
    data: dict[str, Any]


# ---------------------------------------------------------------- reference data


class Market(ApiModel):
    ticker: str
    event_ticker: str
    series_ticker: str | None = None
    event_title: str | None = None
    type: str
    status: str
    yes_label: str
    no_label: str
    open_time: datetime | None = None
    close_time: datetime | None = None
    expiration_time: datetime | None = None
    result: str | None = None
    settlement_value: Decimal | None = None
    settlement_time: datetime | None = None
    strike_type: str | None = None
    floor_strike: Decimal | None = None
    cap_strike: Decimal | None = None
    stage: Literal["full", "slim", "tombstone"]  # how much of the row the server still keeps


class MarketDetail(Market):
    rules_primary: str
    rules_secondary: str
    created_time: datetime | None = None
    updated_time: datetime | None = None


class MarketPage(ApiModel):
    markets: list[Market]
    next_cursor: str | None = None


class Event(ApiModel):
    event_ticker: str
    series_ticker: str
    title: str
    sub_title: str
    mutually_exclusive: bool
    strike_date: datetime | None = None
    strike_period: str | None = None
    markets: list[Market]


# ---------------------------------------------------------------- market data


class Bar(ApiModel):
    """One candle. ``source=trades`` bars carry ``trades``; ``source=ticker`` bars carry the
    last quote, open interest and ``ticks``."""

    time: datetime
    open: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None
    close: Decimal | None = None
    volume: Decimal | None = None
    trades: int | None = None
    yes_bid: Decimal | None = None
    yes_ask: Decimal | None = None
    open_interest: Decimal | None = None
    ticks: int | None = None


class CandlePage(ApiModel):
    ticker: str
    interval: Literal["1m", "1h"]
    source: Literal["trades", "ticker"]
    bars: list[Bar]
    next_cursor: str | None = None


class Trade(ApiModel):
    time: datetime
    trade_id: UUID
    yes_price: Decimal
    no_price: Decimal
    count: Decimal
    taker_side: str | None = None
    is_block_trade: bool


class TradePage(ApiModel):
    ticker: str
    trades: list[Trade]
    next_cursor: str | None = None


class Tick(ApiModel):
    time: datetime
    price: Decimal | None = None
    yes_bid: Decimal | None = None
    yes_ask: Decimal | None = None
    yes_bid_size: Decimal | None = None
    yes_ask_size: Decimal | None = None
    volume: Decimal | None = None
    open_interest: Decimal | None = None
    last_trade_size: Decimal | None = None


class TickPage(ApiModel):
    ticker: str
    ticks: list[Tick]
    next_cursor: str | None = None


class BookLevel(ApiModel):
    price: Decimal
    size: Decimal


class BookGap(ApiModel):
    reason: str
    start: datetime = Field(alias="from")
    end: datetime = Field(alias="to")


class OrderBook(ApiModel):
    """The book at ``as_of``. ``complete`` is false when the stored changes cannot be trusted to
    be gap-free; ``gaps`` says why."""

    ticker: str
    as_of: datetime
    snapshot_time: datetime
    changes_applied: int
    approximate_snapshot: bool
    complete: bool
    gaps: list[BookGap]
    yes: list[BookLevel]
    no: list[BookLevel]
    best_yes_bid: Decimal | None = None
    best_no_bid: Decimal | None = None
    best_yes_ask: Decimal | None = None
    best_no_ask: Decimal | None = None


class Gap(ApiModel):
    """A period the live stream may have missed (tickers, lifecycle events and orderbook
    changes; trades are recovered, see ``trades_recovered``)."""

    id: int
    reason: str
    status: str
    start: datetime = Field(alias="from")
    end: datetime = Field(alias="to")
    messages_dropped: int
    trades_recovered: int
    note: str
