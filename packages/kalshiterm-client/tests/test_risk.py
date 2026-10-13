import random
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from kalshiterm_client.risk import (
    AccountSnapshot,
    Action,
    ApprovedOrder,
    Code,
    MarketState,
    OrderIntent,
    Outcome,
    RiskPolicy,
    ShardState,
    Transfer,
    Verdict,
    check,
    collateral_e6,
    plan_funding,
    worst_case_fee_e6,
)

NOW = datetime(2026, 10, 13, 12, 0, tzinfo=UTC)
M1 = 1_000_000  # fee multiplier 1.0
CENT = 10_000


def shards(*balances: int, active: bool = True) -> dict[int, ShardState]:
    return {i: ShardState(i, b, active, True) for i, b in enumerate(balances)}


def snap(**kw: object) -> AccountSnapshot:
    base = AccountSnapshot(
        as_of=NOW,
        shards=shards(5_000_000, 0, 0, 0),
        markets={},
        open_orders=0,
        exposure_e6=0,
        realized_pnl_today_e6=0,
        transferred_today_e6=0,
        stream_ok=True,
    )
    return replace(base, **kw)  # type: ignore[arg-type]


def order(**kw: object) -> OrderIntent:
    base = OrderIntent(
        ticker="KXA-1",
        exchange_index=0,
        action=Action.BUY,
        outcome=Outcome.YES,
        count_e2=100,
        price_e6=500_000,
        fee_multiplier_e6=M1,
    )
    return replace(base, **kw)  # type: ignore[arg-type]


POLICY = RiskPolicy()


def verdict(
    intent: OrderIntent | None = None,
    snapshot: AccountSnapshot | None = None,
    *,
    policy: RiskPolicy = POLICY,
    production: bool = False,
) -> Verdict:
    return check(
        intent or order(),
        snapshot if snapshot is not None else snap(),
        policy,
        now=NOW,
        production=production,
    )


def codes(v: Verdict) -> set[Code]:
    return {r.code for r in v.refusals}


def test_a_small_order_inside_every_limit_is_approved_with_its_exact_worst_case_cost() -> None:
    v = verdict()
    assert v.ok and v.approved is not None
    # 1 contract at $0.50: 0.07 x 1 x 0.25 = $0.0175, rounded up to a cent = $0.02
    assert v.fee_e6 == 20_000 and v.worst_case_cost_e6 == 500_000 + 20_000
    assert v.funding == () and not v.refusals


def test_the_fee_is_taken_at_the_worst_price_the_limit_allows() -> None:
    # A buy limited at 0.90 can fill as low as 0.50, where the fee peaks; a buy at 0.20 cannot.
    assert worst_case_fee_e6(order(price_e6=900_000)) == worst_case_fee_e6(order(price_e6=500_000))
    big = dict(count_e2=100_000)  # large enough that the cent rounding cannot hide the difference
    assert worst_case_fee_e6(order(price_e6=900_000, **big)) == worst_case_fee_e6(
        order(price_e6=500_000, **big)
    )
    low = worst_case_fee_e6(order(price_e6=200_000, **big))
    mid = worst_case_fee_e6(order(price_e6=500_000, **big))
    assert low is not None and mid is not None and low < mid
    # A sell is the mirror image: limited at 0.10 it can fill as high as 0.50.
    assert worst_case_fee_e6(order(action=Action.SELL, price_e6=100_000)) == 20_000
    assert worst_case_fee_e6(order(fee_multiplier_e6=None)) is None


def test_selling_what_you_hold_locks_nothing_and_selling_short_locks_the_other_side() -> None:
    sell = order(action=Action.SELL, count_e2=300, price_e6=600_000)
    assert collateral_e6(sell, position_e2=300) == 0  # a pure close
    assert collateral_e6(sell, position_e2=100) == 2 * 400_000  # 2 uncovered = NO at 0.40
    assert collateral_e6(sell, position_e2=-500) == 3 * 400_000  # long NO does not cover YES
    sell_no = order(action=Action.SELL, outcome=Outcome.NO, count_e2=100, price_e6=300_000)
    assert collateral_e6(sell_no, position_e2=-100) == 0
    assert collateral_e6(sell_no, position_e2=0) == 700_000


@pytest.mark.parametrize(
    ("what", "code"),
    [
        (dict(intent=order(count_e2=600)), Code.ORDER_TOO_LARGE),
        (dict(intent=order(count_e2=300, price_e6=900_000)), Code.NOTIONAL_TOO_LARGE),
        (dict(snapshot=snap(open_orders=3)), Code.TOO_MANY_ORDERS),
        (dict(snapshot=snap(exposure_e6=2_900_000)), Code.EXPOSURE_LIMIT),
        (dict(snapshot=snap(realized_pnl_today_e6=-2_000_000)), Code.DAILY_LOSS),
        (dict(snapshot=snap(stream_ok=False)), Code.STREAM_DOWN),
        (dict(snapshot=snap(as_of=NOW - timedelta(seconds=11))), Code.STALE_SNAPSHOT),
        (dict(snapshot=snap(as_of=NOW + timedelta(minutes=1))), Code.STALE_SNAPSHOT),
        (dict(intent=order(exchange_index=9)), Code.UNKNOWN_SHARD),
        (dict(intent=order(fee_multiplier_e6=None)), Code.UNKNOWN_FEE),
        (
            dict(snapshot=snap(shards=shards(5_000_000, 0, 0, 0, active=False))),
            Code.SHARD_NOT_TRADING,
        ),
    ],
)
def test_each_limit_refuses_with_its_own_code(what: dict[str, Any], code: Code) -> None:
    v = verdict(**what)
    assert not v.ok and v.approved is None
    assert code in codes(v) and v.funding == ()


def test_the_limits_are_inclusive_at_the_boundary() -> None:
    assert verdict(order(count_e2=500, price_e6=100_000)).ok  # exactly the size limit
    assert verdict(snapshot=snap(open_orders=2)).ok  # the third order is allowed
    assert verdict(snapshot=snap(realized_pnl_today_e6=-1_999_999)).ok


def test_a_missing_snapshot_refuses_everything() -> None:
    v = check(order(), None, POLICY, now=NOW, production=False)
    assert Code.NO_SNAPSHOT in codes(v) and not v.ok


def test_all_the_reasons_are_reported_not_just_the_first() -> None:
    v = verdict(order(count_e2=600), snap(open_orders=3, stream_ok=False))
    assert {Code.ORDER_TOO_LARGE, Code.TOO_MANY_ORDERS, Code.STREAM_DOWN} <= codes(v)


def test_the_daily_loss_limit_trips_the_kill_switch_and_nothing_else_does() -> None:
    assert verdict(snapshot=snap(realized_pnl_today_e6=-3_000_000)).trip_kill
    assert not verdict(order(count_e2=600)).trip_kill


def test_position_limits_count_resting_orders_and_net_against_the_other_side() -> None:
    policy = RiskPolicy(max_position_e2=500, max_order_notional_e6=5_000_000)
    held = snap(markets={"KXA-1": MarketState(position_e2=300, resting_yes_e2=100)})
    assert verdict(order(count_e2=100), held, policy=policy).ok  # 300 + 100 + 100 = 500
    assert Code.POSITION_LIMIT in codes(verdict(order(count_e2=101), held, policy=policy))
    # Buying NO against a long YES position reduces it, so it is fine at any size within limits.
    close = order(outcome=Outcome.NO, count_e2=300)
    assert verdict(close, held, policy=policy).ok
    # ...but buying more NO than the position leaves a net NO position that is itself limited.
    assert Code.POSITION_LIMIT in codes(
        verdict(order(outcome=Outcome.NO, count_e2=900), held, policy=policy)
    )
    # Selling NO adds YES exactly like buying YES does.
    assert Code.POSITION_LIMIT in codes(
        verdict(order(action=Action.SELL, outcome=Outcome.NO, count_e2=101), held, policy=policy)
    )


def test_production_needs_the_switch_demo_does_not() -> None:
    assert verdict().ok
    assert Code.PRODUCTION_DISABLED in codes(verdict(production=True))
    on = RiskPolicy(production_enabled=True)
    v = verdict(production=True, policy=on)
    assert v.ok and v.approved is not None and v.approved.production


def test_an_unfunded_shard_is_funded_from_the_richest_other_shard() -> None:
    s = snap(shards=shards(3_000_000, 0, 1_000_000, 0))
    v = verdict(order(exchange_index=3), s)
    assert v.ok
    assert v.funding == (Transfer(0, 3, v.worst_case_cost_e6 or 0),)


def test_funding_can_come_from_several_shards_and_only_what_is_needed() -> None:
    s = snap(shards=shards(300_000, 0, 250_000, 0))
    v = verdict(order(exchange_index=3), s)  # needs 520,000
    assert v.ok and v.approved is not None
    assert [(t.source, t.amount_e6) for t in v.funding] == [(0, 300_000), (2, 220_000)]
    assert sum(t.amount_e6 for t in v.funding) == v.worst_case_cost_e6
    # a shard that already has enough is never topped up
    assert (
        verdict(order(exchange_index=0), snap(shards=shards(900_000, 5_000_000, 0, 0))).funding
        == ()
    )


def test_transfers_are_whole_centicents_even_when_the_cost_is_not() -> None:
    s = snap(shards=shards(5_000_000, 0, 0, 0))
    v = verdict(order(exchange_index=1, count_e2=1, price_e6=333_333, fee_multiplier_e6=M1), s)
    assert v.ok and all(t.amount_e6 % 100 == 0 for t in v.funding)
    assert sum(t.amount_e6 for t in v.funding) >= (v.worst_case_cost_e6 or 0)


def test_an_account_that_cannot_cover_the_order_is_refused_with_the_shortfall() -> None:
    v = verdict(order(exchange_index=3), snap(shards=shards(200_000, 100_000, 0, 0)))
    assert Code.INSUFFICIENT_FUNDS in codes(v)
    assert "$0.2200 short" in v.refusals[0].message


def test_funding_respects_which_shards_can_transfer() -> None:
    sh = shards(5_000_000, 0, 0, 0)
    sh[0] = ShardState(0, 5_000_000, True, transfers_active=False)
    assert Code.INSUFFICIENT_FUNDS in codes(verdict(order(exchange_index=3), snap(shards=sh)))
    sh = shards(5_000_000, 0, 0, 0)
    sh[3] = ShardState(3, 0, True, transfers_active=False)
    assert Code.INSUFFICIENT_FUNDS in codes(verdict(order(exchange_index=3), snap(shards=sh)))


def test_transfer_caps_bound_what_an_order_can_move() -> None:
    policy = RiskPolicy(max_transfer_per_order_e6=400_000, max_transfer_per_day_e6=1_000_000)
    s = snap(shards=shards(5_000_000, 0, 0, 0))
    assert Code.TRANSFER_LIMIT in codes(verdict(order(exchange_index=3), s, policy=policy))
    roomy = RiskPolicy(max_transfer_per_order_e6=600_000, max_transfer_per_day_e6=1_000_000)
    assert verdict(order(exchange_index=3), s, policy=roomy).ok
    used = snap(shards=shards(5_000_000, 0, 0, 0), transferred_today_e6=600_000)
    assert Code.TRANSFER_LIMIT in codes(verdict(order(exchange_index=3), used, policy=roomy))


def test_an_approved_order_cannot_be_forged() -> None:
    with pytest.raises(TypeError):
        ApprovedOrder(order(), 0, 0, (), False, NOW)
    with pytest.raises(TypeError):
        ApprovedOrder(order(), 0, 0, (), False, NOW, object())
    assert verdict().approved is not None


def test_an_invalid_order_is_a_programming_error_not_a_verdict() -> None:
    for bad in (dict(count_e2=0), dict(count_e2=-1), dict(price_e6=0), dict(price_e6=1_000_000)):
        with pytest.raises(ValueError):
            order(**bad)


def test_plan_funding_never_overdraws_a_source() -> None:
    s = snap(shards=shards(150_050, 99_999, 0, 0))
    plan = plan_funding(s, 3, 260_000)
    assert plan is None  # only 249,900 is usable once balances are cut to whole centicents
    plan = plan_funding(s, 3, 200_000)
    assert plan is not None
    for t in plan:
        assert t.amount_e6 <= s.shards[t.source].balance_e6 and t.amount_e6 % 100 == 0


def test_fuzz_an_approved_order_always_satisfies_every_limit() -> None:
    """Random orders and accounts, with the limits re-derived independently of ``check``."""
    rng = random.Random(6)
    approved = refused = 0
    for _ in range(20_000):
        policy = RiskPolicy(
            max_order_count_e2=rng.choice([300, 500, 2_000]),
            max_position_e2=rng.choice([800, 2_000]),
            max_order_notional_e6=rng.choice([1_000_000, 5_000_000, 20_000_000]),
            max_open_exposure_e6=rng.choice([1_000_000, 3_000_000]),
            max_daily_loss_e6=rng.choice([1_000_000, 2_000_000]),
            max_open_orders=rng.choice([1, 3]),
            max_transfer_per_order_e6=rng.choice([500_000, 2_000_000]),
            max_transfer_per_day_e6=rng.choice([1_000_000, 5_000_000]),
        )
        sh = {
            i: ShardState(
                i, rng.randrange(0, 4_000_000, 100), rng.random() < 0.9, rng.random() < 0.9
            )
            for i in range(4)
        }
        tickers = ["T1", "T2"]
        markets = {
            t: MarketState(rng.randint(-400, 400), rng.randint(0, 200), rng.randint(0, 200))
            for t in tickers
        }
        s = AccountSnapshot(
            as_of=NOW - timedelta(seconds=rng.randint(-8, 14)),
            shards=sh,
            markets=markets,
            open_orders=rng.randint(0, 3),
            exposure_e6=rng.randrange(0, 2_000_000, 100),
            realized_pnl_today_e6=rng.randint(-3_000_000, 500_000),
            transferred_today_e6=rng.randrange(0, 4_000_000, 100),
            stream_ok=rng.random() < 0.95,
        )
        intent = OrderIntent(
            rng.choice(tickers),
            rng.randint(0, 4),
            rng.choice(list(Action)),
            rng.choice(list(Outcome)),
            rng.choice([rng.randint(1, 300), rng.randint(1, 2_500)]),
            rng.randint(1, 999_999),
            rng.choice([M1, M1, M1, 2 * M1, None]),
        )
        v = check(intent, s, policy, now=NOW, production=False)
        if not v.ok:
            refused += 1
            assert v.funding == () and v.approved is None and v.refusals
            continue
        approved += 1
        a = v.approved
        assert a is not None
        m = markets[intent.ticker]
        shard = sh[intent.exchange_index]
        cost = a.worst_case_cost_e6
        assert intent.count_e2 <= policy.max_order_count_e2
        assert cost <= policy.max_order_notional_e6
        assert s.exposure_e6 + cost <= policy.max_open_exposure_e6
        assert s.open_orders + 1 <= policy.max_open_orders
        assert s.realized_pnl_today_e6 > -policy.max_daily_loss_e6
        assert s.stream_ok and shard.trading_active
        assert timedelta(0) <= NOW - s.as_of <= timedelta(
            seconds=policy.snapshot_max_age_seconds
        ) or (timedelta(seconds=-5) <= NOW - s.as_of)
        if intent.adds_yes:
            assert m.position_e2 + m.resting_yes_e2 + intent.count_e2 <= policy.max_position_e2
        else:
            assert -m.position_e2 + m.resting_no_e2 + intent.count_e2 <= policy.max_position_e2
        moved = sum(t.amount_e6 for t in a.funding)
        assert shard.balance_e6 + moved >= cost
        assert moved <= policy.max_transfer_per_order_e6
        assert s.transferred_today_e6 + moved <= policy.max_transfer_per_day_e6
        per_source: dict[int, int] = {}
        for t in a.funding:
            assert t.destination == intent.exchange_index and t.source != t.destination
            assert sh[t.source].transfers_active and t.amount_e6 > 0 and t.amount_e6 % 100 == 0
            per_source[t.source] = per_source.get(t.source, 0) + t.amount_e6
        assert all(amount <= sh[i].balance_e6 for i, amount in per_source.items())
        if moved:
            assert shard.transfers_active
    assert approved > 200 and refused > 200  # both branches are exercised
