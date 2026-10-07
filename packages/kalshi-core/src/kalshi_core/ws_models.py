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


class WsMessage(WsModel):
    """Envelope for everything the server sends."""

    type: str
    id: int | None = None
    sid: int | None = None
    seq: int | None = None
    sending_ts_ms: int | None = None
    msg: Any = None

    def payload(
        self,
    ) -> TickerMsg | TradeMsg | LifecycleMsg | OrderbookSnapshotMsg | OrderbookDeltaMsg | None:
        """Typed payload for known data channels, else ``None`` (use ``msg``)."""
        if self.type == "ticker":
            return TickerMsg.model_validate(self.msg)
        if self.type == "trade":
            return TradeMsg.model_validate(self.msg)
        if self.type == "market_lifecycle_v2":
            return LifecycleMsg.model_validate(self.msg)
        if self.type == "orderbook_snapshot":
            return OrderbookSnapshotMsg.model_validate(self.msg)
        if self.type == "orderbook_delta":
            return OrderbookDeltaMsg.model_validate(self.msg)
        return None
