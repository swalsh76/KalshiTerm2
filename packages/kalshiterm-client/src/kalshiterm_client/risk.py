"""The risk layer: every order passes through ``check`` before anything can be sent.

``check`` is a pure function of (order, account snapshot, policy, clock): no I/O, no globals, so
it can be tested exhaustively. It returns a ``Verdict``. An approved verdict carries an
``ApprovedOrder``, which only this module can construct; the order service accepts nothing else,
so an order cannot reach Kalshi without having been checked.

Rules that shape everything here:

* **Fail closed.** A missing, stale or future-dated snapshot, a private stream that is down, an
  unknown shard, an inactive shard or an unknown fee multiplier all refuse the order.
* **Worst case.** Cost is the most the order can take from the balance, fee included: the fee
  peaks at a fill price of 0.50 (it is ``P x (1 - P)``), and a limit order can fill at any price
  at or better than its limit, so the fee is evaluated at the worst price the limit allows and
  rounded up to a cent (Kalshi aligns balances to $0.0001 or $0.01).
* **Exact numbers.** Dollars are integers in millionths (``*_e6``), counts integers in hundredths
  (``*_e2``); nothing here is a float.
* **Cancels are never checked** (nothing is ever refused to the kill switch), so they do not
  appear in this module.
* Shard funding: if the order's shard lacks the balance, the verdict carries the smallest
  transfers from other shards that would cover it (largest free balance first). The balance of a
  shard is taken to be what is free to use; whether Kalshi already deducts resting-order
  collateral from it is checked on the live account in slice 6.7.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum

from kalshi_core.fees import E6, round_up, taker_fee_e6

CENTICENT_E6 = 100  # transfers are whole centicents ($0.0001)
FEE_UNIT_E6 = 10_000  # fees are rounded up to a cent per order, the conservative alignment
FUTURE_TOLERANCE = timedelta(seconds=5)


class Action(StrEnum):
    BUY = "buy"
    SELL = "sell"


class Outcome(StrEnum):
    YES = "yes"
    NO = "no"


class Code(StrEnum):
    PRODUCTION_DISABLED = "production_disabled"
    NO_SNAPSHOT = "no_snapshot"
    STALE_SNAPSHOT = "stale_snapshot"
    STREAM_DOWN = "stream_down"
    UNKNOWN_SHARD = "unknown_shard"
    SHARD_NOT_TRADING = "shard_not_trading"
    UNKNOWN_FEE = "unknown_fee"
    ORDER_TOO_LARGE = "order_too_large"
    NOTIONAL_TOO_LARGE = "notional_too_large"
    POSITION_LIMIT = "position_limit"
    TOO_MANY_ORDERS = "too_many_orders"
    EXPOSURE_LIMIT = "exposure_limit"
    DAILY_LOSS = "daily_loss"
    INSUFFICIENT_FUNDS = "insufficient_funds"
    TRANSFER_LIMIT = "transfer_limit"


@dataclass(frozen=True)
class Refusal:
    code: Code
    message: str


@dataclass(frozen=True)
class OrderIntent:
    """What the user wants: ``outcome`` is the side they are trading, ``price_e6`` its limit."""

    ticker: str
    exchange_index: int
    action: Action
    outcome: Outcome
    count_e2: int
    price_e6: int
    fee_multiplier_e6: int | None

    def __post_init__(self) -> None:
        if self.count_e2 <= 0:
            raise ValueError("an order needs a positive count")
        if not 0 < self.price_e6 < E6:
            raise ValueError("the price must be strictly between $0 and $1")

    @property
    def adds_yes(self) -> bool:
        """True if filling it moves the net position towards YES (else towards NO)."""
        return (self.action, self.outcome) in (
            (Action.BUY, Outcome.YES),
            (Action.SELL, Outcome.NO),
        )


@dataclass(frozen=True)
class ShardState:
    index: int
    balance_e6: int
    trading_active: bool
    transfers_active: bool


@dataclass(frozen=True)
class MarketState:
    """``position_e2`` is the net YES position (negative = long NO); the resting figures are the
    open orders that would add to the YES (or NO) position if they filled."""

    position_e2: int = 0
    resting_yes_e2: int = 0
    resting_no_e2: int = 0


@dataclass(frozen=True)
class AccountSnapshot:
    as_of: datetime
    shards: Mapping[int, ShardState]
    markets: Mapping[str, MarketState]
    open_orders: int
    exposure_e6: int  # market exposure plus the cost of resting buys, account-wide
    realized_pnl_today_e6: int
    transferred_today_e6: int
    stream_ok: bool


@dataclass(frozen=True)
class RiskPolicy:
    """The limits. The defaults are deliberately tiny; they change only by editing ``risk.toml``."""

    max_order_count_e2: int = 500
    max_position_e2: int = 1_000
    max_order_notional_e6: int = 1_000_000
    max_open_exposure_e6: int = 3_000_000
    max_daily_loss_e6: int = 2_000_000
    max_open_orders: int = 3
    max_transfer_per_order_e6: int = 2_000_000
    max_transfer_per_day_e6: int = 5_000_000
    snapshot_max_age_seconds: int = 10
    production_enabled: bool = False


@dataclass(frozen=True)
class Transfer:
    source: int
    destination: int
    amount_e6: int


_MINT = object()


@dataclass(frozen=True)
class ApprovedOrder:
    """Proof that ``check`` approved ``intent``. Constructing one anywhere else raises."""

    intent: OrderIntent
    fee_e6: int
    worst_case_cost_e6: int
    funding: tuple[Transfer, ...]
    production: bool
    approved_at: datetime
    _mint: object = field(repr=False, compare=False, default=None)

    def __post_init__(self) -> None:
        if self._mint is not _MINT:
            raise TypeError("an ApprovedOrder can only be produced by risk.check")


@dataclass(frozen=True)
class Verdict:
    approved: ApprovedOrder | None
    refusals: tuple[Refusal, ...]
    fee_e6: int | None
    worst_case_cost_e6: int | None
    funding: tuple[Transfer, ...]
    trip_kill: bool = False

    @property
    def ok(self) -> bool:
        return self.approved is not None


def _ceil_div(numerator: int, denominator: int) -> int:
    return -(-numerator // denominator)


def worst_case_fee_e6(intent: OrderIntent) -> int | None:
    """The largest fee the order can incur, or None if the fee cannot be modelled."""
    if intent.fee_multiplier_e6 is None:
        return None
    half = E6 // 2
    limit = intent.price_e6
    # A buy fills at or below its limit, a sell at or above it; the fee peaks at 0.50.
    fee_price = min(limit, half) if intent.action is Action.BUY else max(limit, half)
    fee = taker_fee_e6(intent.fee_multiplier_e6, intent.count_e2, fee_price)
    return round_up(fee, FEE_UNIT_E6)


def collateral_e6(intent: OrderIntent, position_e2: int) -> int:
    """Cash the order can lock up, excluding fees.

    A buy locks ``count x limit``. A sell first closes what is held of that outcome (no new
    collateral); anything beyond is selling short, i.e. buying the other side at ``1 - limit``.
    """
    if intent.action is Action.BUY:
        return _ceil_div(intent.count_e2 * intent.price_e6, 100)
    held = max(position_e2 if intent.outcome is Outcome.YES else -position_e2, 0)
    opening = max(intent.count_e2 - held, 0)
    return _ceil_div(opening * (E6 - intent.price_e6), 100)


def plan_funding(
    snapshot: AccountSnapshot, target: int, need_e6: int
) -> tuple[Transfer, ...] | None:
    """Transfers that bring ``target`` up by ``need_e6``, or None if the account cannot."""
    if need_e6 <= 0:
        return ()
    need = round_up(need_e6, CENTICENT_E6)
    if not snapshot.shards[target].transfers_active:
        return None
    sources = sorted(
        (s for s in snapshot.shards.values() if s.index != target and s.transfers_active),
        key=lambda s: (-s.balance_e6, s.index),
    )
    plan: list[Transfer] = []
    for source in sources:
        if need <= 0:
            break
        take = min(need, source.balance_e6 - source.balance_e6 % CENTICENT_E6)
        if take > 0:
            plan.append(Transfer(source.index, target, take))
            need -= take
    return tuple(plan) if need <= 0 else None


def _dollars(value_e6: int) -> str:
    """Dollars to four places, or six when the value is finer than a centicent."""
    sign = "-" if value_e6 < 0 else ""
    whole, frac = divmod(abs(value_e6), E6)
    digits = f"{frac:06d}" if frac % CENTICENT_E6 else f"{frac // CENTICENT_E6:04d}"
    return f"{sign}${whole}.{digits}"


def _count(value_e2: int) -> str:
    return f"{value_e2 / 100:g}"


def check(
    intent: OrderIntent,
    snapshot: AccountSnapshot | None,
    policy: RiskPolicy,
    *,
    now: datetime,
    production: bool,
) -> Verdict:
    refusals: list[Refusal] = []

    def refuse(code: Code, message: str) -> None:
        refusals.append(Refusal(code, message))

    if production and not policy.production_enabled:
        refuse(
            Code.PRODUCTION_DISABLED,
            "trading on the production account is switched off "
            "(production_enabled = false in risk.toml)",
        )

    fee = worst_case_fee_e6(intent)
    if fee is None:
        refuse(
            Code.UNKNOWN_FEE, f"{intent.ticker}: the fee cannot be modelled, so the cost is unknown"
        )

    if snapshot is None:
        refuse(Code.NO_SNAPSHOT, "no account snapshot yet, so limits cannot be checked")
        return _refused(refusals, fee)

    age = now - snapshot.as_of
    if age > timedelta(seconds=policy.snapshot_max_age_seconds) or age < -FUTURE_TOLERANCE:
        refuse(
            Code.STALE_SNAPSHOT,
            f"the account snapshot is {age.total_seconds():.0f}s old "
            f"(limit {policy.snapshot_max_age_seconds}s)",
        )
    if not snapshot.stream_ok:
        refuse(Code.STREAM_DOWN, "the private account stream is down, so positions may be stale")

    shard = snapshot.shards.get(intent.exchange_index)
    if shard is None:
        refuse(Code.UNKNOWN_SHARD, f"exchange shard {intent.exchange_index} is not in the snapshot")
    elif not shard.trading_active:
        refuse(Code.SHARD_NOT_TRADING, f"trading is not active on exchange shard {shard.index}")

    market = snapshot.markets.get(intent.ticker, MarketState())
    cost: int | None = None
    if fee is not None:
        cost = collateral_e6(intent, market.position_e2) + fee

    if intent.count_e2 > policy.max_order_count_e2:
        refuse(
            Code.ORDER_TOO_LARGE,
            f"{_count(intent.count_e2)} contracts exceeds the limit of "
            f"{_count(policy.max_order_count_e2)} per order",
        )
    if cost is not None and cost > policy.max_order_notional_e6:
        refuse(
            Code.NOTIONAL_TOO_LARGE,
            f"worst-case cost {_dollars(cost)} exceeds the per-order limit "
            f"of {_dollars(policy.max_order_notional_e6)}",
        )

    if intent.adds_yes:
        worst = market.position_e2 + market.resting_yes_e2 + intent.count_e2
        side = "YES"
    else:
        worst = -market.position_e2 + market.resting_no_e2 + intent.count_e2
        side = "NO"
    if worst > policy.max_position_e2:
        refuse(
            Code.POSITION_LIMIT,
            f"{intent.ticker}: this could leave {_count(worst)} {side} contracts "
            f"(limit {_count(policy.max_position_e2)} per market)",
        )

    if snapshot.open_orders + 1 > policy.max_open_orders:
        refuse(
            Code.TOO_MANY_ORDERS,
            f"{snapshot.open_orders} orders are already open (limit {policy.max_open_orders})",
        )
    if cost is not None and snapshot.exposure_e6 + cost > policy.max_open_exposure_e6:
        refuse(
            Code.EXPOSURE_LIMIT,
            f"exposure would reach {_dollars(snapshot.exposure_e6 + cost)} "
            f"(limit {_dollars(policy.max_open_exposure_e6)})",
        )

    trip = snapshot.realized_pnl_today_e6 <= -policy.max_daily_loss_e6
    if trip:
        refuse(
            Code.DAILY_LOSS,
            f"today's realized loss {_dollars(-snapshot.realized_pnl_today_e6)} has reached the "
            f"daily limit of {_dollars(policy.max_daily_loss_e6)}",
        )

    funding: tuple[Transfer, ...] = ()
    if cost is not None and shard is not None and shard.trading_active:
        need = cost - shard.balance_e6
        planned = plan_funding(snapshot, shard.index, need)
        if planned is None:
            free = sum(s.balance_e6 for s in snapshot.shards.values())
            if cost > free:
                why = (
                    f"the account has only {_dollars(free)} in total, {_dollars(cost - free)} short"
                )
            else:
                why = "the other shards' funds cannot be moved right now"
            refuse(
                Code.INSUFFICIENT_FUNDS,
                f"the order needs {_dollars(cost)} on shard {shard.index}, which has "
                f"{_dollars(shard.balance_e6)}; {why}",
            )
        else:
            funding = planned
            moved = sum(t.amount_e6 for t in funding)
            if moved > policy.max_transfer_per_order_e6:
                refuse(
                    Code.TRANSFER_LIMIT,
                    f"funding the shard needs a {_dollars(moved)} transfer "
                    f"(limit {_dollars(policy.max_transfer_per_order_e6)} per order)",
                )
            elif snapshot.transferred_today_e6 + moved > policy.max_transfer_per_day_e6:
                refuse(
                    Code.TRANSFER_LIMIT,
                    "funding the shard would move "
                    f"{_dollars(snapshot.transferred_today_e6 + moved)} today "
                    f"(limit {_dollars(policy.max_transfer_per_day_e6)})",
                )

    if refusals:
        return Verdict(None, tuple(refusals), fee, cost, (), trip_kill=trip)
    assert fee is not None and cost is not None
    approved = ApprovedOrder(intent, fee, cost, funding, production, now, _MINT)
    return Verdict(approved, (), fee, cost, funding)


def _refused(refusals: list[Refusal], fee: int | None) -> Verdict:
    return Verdict(None, tuple(refusals), fee, None, ())
