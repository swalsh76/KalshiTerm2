"""Every message on the browser WebSocket, as typed models.

These exist so the TypeScript side is *generated* from them (``schema.py``) and cannot drift.
Decimals are strings on the wire, never JSON numbers.
"""

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from kalshiterm_client.stream_events import AlertEvent, TickerEvent, TradeEvent

Channel = Literal["ticker", "trades", "book"]
TickerSymbol = Annotated[str, Field(pattern=r"^[A-Za-z0-9._-]{1,128}$")]


class _Out(BaseModel):
    model_config = ConfigDict(extra="forbid")


# ---------------------------------------------------------------- browser -> backend


class Auth(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["auth"]
    token: str


class Ping(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["ping"]


class Subscribe(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["subscribe"]
    channels: list[Channel] = Field(min_length=1, max_length=3)
    tickers: list[TickerSymbol] = Field(min_length=1, max_length=100)


class Unsubscribe(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["unsubscribe"]
    channels: list[Channel] = Field(min_length=1, max_length=3)
    tickers: list[TickerSymbol] = Field(min_length=1, max_length=100)


class SetAlerts(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["alerts"]
    enabled: bool


class SetAccountFeed(BaseModel):
    """Ask for (or stop) live account updates; only meaningful while trading is switched on."""

    model_config = ConfigDict(extra="forbid")
    type: Literal["account"]
    enabled: bool


ClientMessage = Annotated[
    Auth | Ping | Subscribe | Unsubscribe | SetAlerts | SetAccountFeed,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------- backend -> browser


class Hello(_Out):
    type: Literal["hello"] = "hello"
    mode: Literal["read-only", "trading"]
    environment: str


class Pong(_Out):
    type: Literal["pong"] = "pong"


class ErrorMessage(_Out):
    type: Literal["error"] = "error"
    error: str


class Subscribed(_Out):
    """The server's answer to a (re)subscription: which tickers it does not know and which have
    no stored order book (so a ``book`` channel for them stays empty)."""

    type: Literal["subscribed"] = "subscribed"
    unknown: list[str]
    no_orderbook: list[str]
    resumed: bool


class Link(_Out):
    """State of the backend's connection to the KalshiTerm server: ``up``, ``down`` (retrying in
    ``retry_in`` seconds) or ``failed`` (needs attention; ``error`` says why)."""

    type: Literal["link"] = "link"
    state: Literal["up", "down", "failed"]
    retry_in: float | None = None
    error: str | None = None


class Resync(_Out):
    """Something may have been missed: forget the order books; fresh ones follow."""

    type: Literal["resync"] = "resync"
    reason: str


class BookMessage(_Out):
    """A whole order book (best price first), at most ``MAX_BOOK_DEPTH`` levels a side."""

    type: Literal["book"] = "book"
    ticker: str
    time: datetime
    yes: list[tuple[Decimal, Decimal]]  # (price, contracts)
    no: list[tuple[Decimal, Decimal]]
    inconsistent: bool = False


class ModeMessage(_Out):
    """Trading was switched on or off. ``reason`` says why when the backend did it itself
    (``idle``, ``no browser``, ``shutdown``) or ``user``."""

    type: Literal["mode"] = "mode"
    mode: Literal["read-only", "trading"]
    reason: str


class AccountMessage(_Out):
    """The whole account view (see ``trading_models.AccountView``), pushed when it changes, at most
    twice a second. ``view`` is null when trading is off."""

    type: Literal["account"] = "account"
    view: dict[str, Any] | None


ServerMessage = Annotated[
    Hello
    | Pong
    | ErrorMessage
    | Subscribed
    | Link
    | Resync
    | ModeMessage
    | AccountMessage
    | BookMessage
    | TickerEvent
    | TradeEvent
    | AlertEvent,
    Field(discriminator="type"),
]
