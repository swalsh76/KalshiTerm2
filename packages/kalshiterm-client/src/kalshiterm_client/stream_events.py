"""What comes out of a live stream (PLAN §10.6): market data, alerts, and a few control events.

Data events carry the server's ``cursor`` (an integer here). Orderbook events marked ``replay``
are the book state sent when you subscribe, not part of the live sequence, so they have none.
Alerts have their own cursor, ``alert_cursor``; the two are never interchangeable.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from pydantic import ValidationError

from kalshiterm_client.models import ApiModel, BookLevel


class _Data(ApiModel):
    ticker: str
    time: datetime


class TradeEvent(_Data):
    type: Literal["trade"] = "trade"
    cursor: int
    trade_id: UUID
    yes_price: Decimal
    no_price: Decimal
    count: Decimal
    taker_side: str | None = None
    is_block_trade: bool


class TickerEvent(_Data):
    type: Literal["ticker"] = "ticker"
    cursor: int
    price: Decimal | None = None
    yes_bid: Decimal | None = None
    yes_ask: Decimal | None = None
    yes_bid_size: Decimal | None = None
    yes_ask_size: Decimal | None = None
    volume: Decimal | None = None
    open_interest: Decimal | None = None
    last_trade_size: Decimal | None = None


class BookSnapshotEvent(_Data):
    type: Literal["book_snapshot"] = "book_snapshot"
    cursor: int | None = None  # None on the replayed state sent at subscribe time
    replay: bool = False
    sid: int | None = None
    seq: int | None = None
    approximate: bool = False
    yes: list[BookLevel]
    no: list[BookLevel]


class BookDeltaEvent(_Data):
    type: Literal["book_delta"] = "book_delta"
    cursor: int | None = None
    replay: bool = False
    sid: int | None = None
    seq: int | None = None
    side: Literal["yes", "no"]
    price: Decimal
    delta: Decimal  # contracts added (positive) or removed (negative) at the price


class AlertEvent(ApiModel):
    type: Literal["alert"] = "alert"
    alert_cursor: int
    id: int
    rule_id: int | None = None
    rule_name: str
    fired_at: datetime
    metric: str
    op: str
    threshold: Decimal
    value: Decimal
    unit: str | None = None
    acknowledged_at: datetime | None = None
    ticker: str | None = None
    event_ticker: str | None = None


# ---------------------------------------------------------------- control events


class Connected(ApiModel):
    """A connection is up and subscribed. ``resumed`` is true when it continued from the cursors
    the caller already had (so nothing was missed), false when it started fresh."""

    type: Literal["connected"] = "connected"
    resumed: bool
    markets: list[str]
    unknown: list[str]  # tickers the server does not know
    no_orderbook: list[str]  # watched markets that have no stored book
    cursor: int
    alerts_cursor: int | None = None


class CaughtUp(ApiModel):
    """The replay after a resume has finished; everything after this is live."""

    type: Literal["caught_up"] = "caught_up"
    stream: Literal["market", "alerts"]
    cursor: int


class Resync(ApiModel):
    """The server could not replay what was missed. State built from the stream (orderbooks) is
    stale: reset it. The stream resubscribed from now, so fresh books follow, but trades and
    ticks between ``missed_after`` and now must be fetched over REST if they matter."""

    type: Literal["resync"] = "resync"
    reason: str
    missed_after: int | None
    cursor: int | None  # where the fresh stream starts, if the server said


class WatchlistChanged(ApiModel):
    type: Literal["watchlist_changed"] = "watchlist_changed"
    added: list[str]
    removed: list[str]


class Disconnected(ApiModel):
    """The connection dropped; the client will reconnect after ``retry_in`` seconds."""

    type: Literal["disconnected"] = "disconnected"
    reason: str
    retry_in: float


class UnknownEvent(ApiModel):
    """A message type this client does not know (a newer server); kept, not dropped."""

    type: Literal["unknown"] = "unknown"
    original_type: str
    data: dict[str, Any]


DataEvent = TradeEvent | TickerEvent | BookSnapshotEvent | BookDeltaEvent | AlertEvent
StreamEvent = (
    DataEvent | Connected | CaughtUp | Resync | WatchlistChanged | Disconnected | UnknownEvent
)

_MODELS: dict[str, type[ApiModel]] = {
    "trade": TradeEvent,
    "ticker": TickerEvent,
    "book_snapshot": BookSnapshotEvent,
    "book_delta": BookDeltaEvent,
    "alert": AlertEvent,
}


class MalformedMessage(ValueError):
    """A message of a known type that does not fit its model: the server and client disagree."""


def parse_data(message: dict[str, Any]) -> Any:
    """A server message of a data type as its event, or None if it is not a data type."""
    model = _MODELS.get(str(message.get("type")))
    if model is None:
        return None
    try:
        return model.model_validate(message)
    except ValidationError as exc:
        fields = sorted({".".join(str(p) for p in e["loc"]) for e in exc.errors()})
        raise MalformedMessage(
            f"a {message['type']} message does not fit ({', '.join(fields)}); "
            "is this client older than the server?"
        ) from exc
