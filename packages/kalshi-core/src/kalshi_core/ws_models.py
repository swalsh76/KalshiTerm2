"""Typed models for Kalshi WebSocket messages (public channels).

Unknown fields are ignored. Orderbook messages are added with the orderbook slice.
"""

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict


class WsModel(BaseModel):
    model_config = ConfigDict(extra="ignore")


class TickerMsg(WsModel):
    market_ticker: str
    market_id: str | None = None
    price_dollars: Decimal | None = None
    yes_bid_dollars: Decimal | None = None
    yes_ask_dollars: Decimal | None = None
    yes_bid_size_fp: Decimal | None = None
    yes_ask_size_fp: Decimal | None = None
    volume_fp: Decimal | None = None
    open_interest_fp: Decimal | None = None
    last_trade_size_fp: Decimal | None = None
    ts_ms: int | None = None


class TradeMsg(WsModel):
    trade_id: str
    market_ticker: str
    yes_price_dollars: Decimal
    no_price_dollars: Decimal
    count_fp: Decimal
    taker_side: str | None = None
    is_block_trade: bool = False
    ts_ms: int | None = None


class LifecycleMsg(WsModel):
    market_ticker: str | None = None
    event_type: str
    open_ts: int | None = None
    close_ts: int | None = None
    determination_ts: int | None = None
    settled_ts: int | None = None
    result: str | None = None
    settlement_value: str | None = None
    is_deactivated: bool | None = None
    exchange_index: int | None = None
    price_level_structure: str | None = None
    additional_metadata: dict[str, Any] | None = None


class EventLifecycleMsg(WsModel):
    """An event was created (sent on both lifecycle channels)."""

    event_ticker: str
    series_ticker: str | None = None
    title: str = ""
    subtitle: str = ""
    collateral_return_type: str = ""
    exchange_index: int | None = None


class OrderbookSnapshotMsg(WsModel):
    """Full book for one market; levels are ``(price, contracts)``, best bid last."""

    market_ticker: str
    market_id: str | None = None
    yes_dollars_fp: list[tuple[Decimal, Decimal]] = []
    no_dollars_fp: list[tuple[Decimal, Decimal]] = []


class OrderbookDeltaMsg(WsModel):
    """Signed change in contracts at one price level on one side."""

    market_ticker: str
    price_dollars: Decimal
    delta_fp: Decimal
    side: str
    ts_ms: int | None = None
    client_order_id: str | None = None


class FillMsg(WsModel):
    """Private ``fill`` channel (type ``fill``): one fill of one of your orders."""

    trade_id: str
    order_id: str
    client_order_id: str = ""
    market_ticker: str
    exchange_index: int = 0
    is_taker: bool = False
    side: str = ""
    outcome_side: str = ""
    purchased_side: str = ""
    book_side: str = ""
    action: str = ""
    yes_price_dollars: Decimal
    count_fp: Decimal
    fee_cost: Decimal = Decimal(0)
    post_position_fp: Decimal | None = None
    ts_ms: int | None = None
    subaccount: int | None = None


class UserOrderMsg(WsModel):
    """Private ``user_orders`` channel (type ``user_order``): the full state of one order."""

    order_id: str
    client_order_id: str = ""
    user_id: str = ""
    ticker: str
    exchange_index: int = 0
    status: str  # resting, canceled, executed, unknown
    side: str = ""
    outcome_side: str = ""
    book_side: str = ""
    yes_price_dollars: Decimal
    initial_count_fp: Decimal
    fill_count_fp: Decimal
    remaining_count_fp: Decimal
    taker_fill_cost_dollars: Decimal = Decimal(0)
    maker_fill_cost_dollars: Decimal = Decimal(0)
    taker_fees_dollars: Decimal = Decimal(0)
    maker_fees_dollars: Decimal = Decimal(0)
    created_ts_ms: int | None = None
    last_updated_ts_ms: int | None = None
    last_update_reason: str = ""


class MarketPositionMsg(WsModel):
    """Private ``market_positions`` channel (type ``market_position``): a market's new state."""

    market_ticker: str
    user_id: str = ""
    position_fp: Decimal  # net YES contracts; negative = long NO
    position_cost_dollars: Decimal | None = None
    realized_pnl_dollars: Decimal | None = None
    fees_paid_dollars: Decimal | None = None
    position_fee_cost_dollars: Decimal | None = None
    volume_fp: Decimal | None = None
    subaccount: int | None = None


class WsMessage(WsModel):
    """Envelope for everything the server sends."""

    type: str
    id: int | None = None
    sid: int | None = None
    seq: int | None = None
    sending_ts_ms: int | None = None
    msg: Any = None
    received_at: float | None = None  # local epoch seconds, stamped when read off the socket

    def payload(
        self,
    ) -> (
        TickerMsg
        | TradeMsg
        | LifecycleMsg
        | EventLifecycleMsg
        | OrderbookSnapshotMsg
        | OrderbookDeltaMsg
        | FillMsg
        | UserOrderMsg
        | MarketPositionMsg
        | None
    ):
        """Typed payload for known data channels, else ``None`` (use ``msg``)."""
        if self.type == "ticker":
            return TickerMsg.model_validate(self.msg)
        if self.type == "trade":
            return TradeMsg.model_validate(self.msg)
        if self.type in ("market_lifecycle_v2", "multivariate_market_lifecycle"):
            return LifecycleMsg.model_validate(self.msg)
        if self.type == "event_lifecycle":
            return EventLifecycleMsg.model_validate(self.msg)
        if self.type == "orderbook_snapshot":
            return OrderbookSnapshotMsg.model_validate(self.msg)
        if self.type == "orderbook_delta":
            return OrderbookDeltaMsg.model_validate(self.msg)
        if self.type == "fill":
            return FillMsg.model_validate(self.msg)
        if self.type == "user_order":
            return UserOrderMsg.model_validate(self.msg)
        if self.type == "market_position":
            return MarketPositionMsg.model_validate(self.msg)
        return None
