"""What the trading routes accept and return.

Every amount is a **decimal string** (dollars with six places, contracts with two), never a JSON
number, so JavaScript cannot round it. Requests refuse unknown fields.
"""

from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

DecimalText = Annotated[str, Field(pattern=r"^[0-9]+(\.[0-9]+)?$", max_length=24)]
Ticker = Annotated[str, Field(pattern=r"^[A-Za-z0-9._-]{1,128}$")]


class _In(BaseModel):
    """Requests are strict: no coercion ("yes" is not true, 1 is not true), no unknown fields."""

    model_config = ConfigDict(extra="forbid", strict=True)


# ------------------------------------------------------------------ the mode


class Limits(BaseModel):
    """The limits from ``risk.toml`` that apply to this session (all decimal strings)."""

    max_order_count: str
    max_position: str
    max_order_notional: str
    max_open_exposure: str
    max_daily_loss: str
    max_open_orders: int
    max_transfer_per_order: str
    max_transfer_per_day: str


class ModeInfo(BaseModel):
    mode: Literal["read-only", "trading"]
    environment: str
    production: bool
    permitted: bool  # risk.toml allows trading in this environment
    permission_help: str | None = None  # what to edit when it does not
    enable_phrase: str  # what the user must type to switch trading on
    halted: bool
    halt_reason: str
    idle_minutes: int
    disconnect_seconds: int
    seconds_until_idle: int | None = None  # while trading
    limits: Limits
    transferred_today: str
    day: str


class EnableRequest(_In):
    phrase: str = Field(max_length=100)


class DisableRequest(_In):
    cancel_open_orders: bool = False


class ModeChange(BaseModel):
    mode: Literal["read-only", "trading"]
    cancelled: list[str] = []
    problems: list[str] = []


# ------------------------------------------------------------------ the account


class ShardView(BaseModel):
    index: int
    cash: str  # what Kalshi reports (gross)
    free: str  # cash less what resting orders may need
    trading: bool
    transfers: bool


class PositionView(BaseModel):
    ticker: str
    position: str  # contracts; negative = long NO
    exposure: str
    realized: str


class OrderView(BaseModel):
    order_id: str
    client_order_id: str
    ticker: str
    shard: int
    book_side: str  # bid (buys YES) or ask (sells YES = buys NO)
    yes_price: str
    no_price: str
    filled: str
    remaining: str
    total: str
    status: str


class FillView(BaseModel):
    trade_id: str
    order_id: str
    ticker: str
    book_side: str
    yes_price: str
    count: str
    fee: str
    is_taker: bool
    time_ms: int | None


class AccountView(BaseModel):
    as_of: str | None
    synced: bool
    halted: bool
    halt_reason: str
    shards: list[ShardView]
    positions: list[PositionView]
    orders: list[OrderView]
    fills: list[FillView]
    realized_today: str
    transferred_today: str
    exposure: str
    open_orders: int
    day: str


# ------------------------------------------------------------------ orders


class PrepareRequest(_In):
    ticker: Ticker
    action: Literal["buy", "sell"]
    outcome: Literal["yes", "no"]
    count: DecimalText
    price: DecimalText
    time_in_force: Literal["good_till_canceled", "immediate_or_cancel"] = "good_till_canceled"
    post_only: bool = False

    def count_value(self) -> Decimal:
        return Decimal(self.count)

    def price_value(self) -> Decimal:
        return Decimal(self.price)


class AmendPrepareRequest(_In):
    price: DecimalText | None = None  # the YES price, as the orders table shows it
    count: DecimalText | None = None  # the new total count


class RefusalView(BaseModel):
    code: str
    message: str


class TransferView(BaseModel):
    source: int
    destination: int
    amount: str


class PreparedView(BaseModel):
    """What would happen. ``prepare_id`` is null when the order is refused (nothing to execute)."""

    prepare_id: str | None
    kind: Literal["order", "amend"]
    expires_in_seconds: int
    lines: list[str]
    approved: bool
    refusals: list[RefusalView]
    shard: int
    worst_case_cost: str | None
    fee: str | None
    funding: list[TransferView]
    requires_phrase: bool
    phrase_format: str | None


class ExecuteRequest(_In):
    prepare_id: str = Field(min_length=8, max_length=64)
    phrase: str | None = Field(default=None, max_length=300)


class ExecutedView(BaseModel):
    kind: Literal["order", "amend"]
    order_id: str
    client_order_id: str | None = None
    filled: str | None = None
    remaining: str | None = None
    recovered: bool = False
    transfers: list[TransferView]
    lines: list[str]


class CancelView(BaseModel):
    order_id: str
    ok: bool
    message: str


class KillView(BaseModel):
    cancelled: list[str]
    remaining: list[str]
    errors: list[str]
    clean: bool
    halted: bool


class ResumeRequest(_In):
    confirm: Literal[True]


class Resumed(BaseModel):
    halted: bool
