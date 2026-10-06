"""Typed models for Kalshi REST responses (read endpoints only).

Kalshi sends prices and counts as fixed-point strings; they are parsed to ``Decimal``.
Unknown fields are ignored because Kalshi adds fields over time. Only fields the
project uses are modelled.
"""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class KalshiModel(BaseModel):
    model_config = ConfigDict(extra="ignore")


class ExchangeIndexStatus(KalshiModel):
    exchange_index: int
    description: str
    exchange_active: bool
    trading_active: bool


class ExchangeStatus(KalshiModel):
    exchange_active: bool
    trading_active: bool
    exchange_estimated_resume_time: datetime | None = None
    exchange_index_statuses: list[ExchangeIndexStatus] | None = None


class Market(KalshiModel):
    ticker: str
    event_ticker: str
    market_type: str
    status: str
    yes_sub_title: str = ""
    no_sub_title: str = ""
    created_time: datetime | None = None
    updated_time: datetime | None = None
    open_time: datetime | None = None
    close_time: datetime | None = None
    latest_expiration_time: datetime | None = None
    yes_bid_dollars: Decimal | None = None
    yes_ask_dollars: Decimal | None = None
    no_bid_dollars: Decimal | None = None
    no_ask_dollars: Decimal | None = None
    yes_bid_size_fp: Decimal | None = None
    yes_ask_size_fp: Decimal | None = None
    last_price_dollars: Decimal | None = None
    volume_fp: Decimal | None = None
    volume_24h_fp: Decimal | None = None
    open_interest_fp: Decimal | None = None
    result: str = ""
    settlement_value_dollars: Decimal | None = None
    settlement_ts: datetime | None = None
    rules_primary: str = ""
    rules_secondary: str = ""
    strike_type: str | None = None
    floor_strike: float | None = None
    cap_strike: float | None = None


class Event(KalshiModel):
    event_ticker: str
    series_ticker: str
    title: str
    sub_title: str = ""
    mutually_exclusive: bool
    strike_date: datetime | None = None
    strike_period: str | None = None
    last_updated_ts: datetime | None = None
    markets: list[Market] | None = None


class Series(KalshiModel):
    ticker: str
    title: str
    frequency: str
    category: str
    tags: list[str] | None = None
    fee_type: str | None = None
    fee_multiplier: float | None = None
    volume_fp: Decimal | None = None
    last_updated_ts: datetime | None = None


class MarketsPage(KalshiModel):
    markets: list[Market]
    cursor: str = ""


class EventsPage(KalshiModel):
    events: list[Event]
    cursor: str = ""


class SeriesList(KalshiModel):
    series: list[Series]


class PriceLevel(KalshiModel):
    """One orderbook level: bid price in dollars and contract quantity."""

    price: Decimal
    quantity: Decimal


class OrderBook(KalshiModel):
    """Bids only; a YES bid at X is equivalent to a NO ask at 1 - X."""

    yes: list[PriceLevel]
    no: list[PriceLevel]

    @classmethod
    def from_response(cls, payload: dict[str, object]) -> "OrderBook":
        fp = payload["orderbook_fp"]
        assert isinstance(fp, dict)

        def levels(key: str) -> list[PriceLevel]:
            rows = fp.get(key) or []
            return [PriceLevel(price=Decimal(p), quantity=Decimal(q)) for p, q in rows]

        return cls(yes=levels("yes_dollars"), no=levels("no_dollars"))
