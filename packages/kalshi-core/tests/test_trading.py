import base64
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from kalshi_core.auth import HEADER_KEY, HEADER_SIGNATURE, HEADER_TIMESTAMP, KalshiSigner
from kalshi_core.config import KalshiSettings
from kalshi_core.ratelimit import TokenBucket, write_bucket
from kalshi_core.rest import KalshiAPIError
from kalshi_core.trading import (
    CancelTarget,
    CreateOrder,
    KalshiTradingClient,
    Side,
    TimeInForce,
    TradingRefused,
    WriteOutcomeUnknown,
    fixed_point,
)

BASE = "https://external-api.demo.kalshi.co/trade-api/v2"
ORDERS = f"{BASE}/portfolio/events/orders"
COID = "11111111-2222-3333-4444-555555555555"
CREATED = {
    "order_id": "o-1",
    "client_order_id": COID,
    "fill_count": "0.00",
    "remaining_count": "2.00",
    "ts_ms": 1791908624000,
}
RESTING = {
    "order_id": "o-1",
    "client_order_id": COID,
    "ticker": "KXA-1",
    "status": "resting",
    "action": "buy",
    "outcome_side": "yes",
    "yes_price_dollars": "0.0100",
    "no_price_dollars": "0.9900",
    "initial_count_fp": "2.00",
    "fill_count_fp": "0.00",
    "remaining_count_fp": "2.00",
}
TRADING_KEY = Ed25519PrivateKey.generate()


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    for name in ("KALSHI_ENV", "KALSHI_KEY_ID", "KALSHI_PRIVATE_KEY_PATH"):
        monkeypatch.delenv(name, raising=False)


class Pauses:
    def __init__(self) -> None:
        self.sleeps: list[float] = []

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)


def trading_signer() -> KalshiSigner:
    return KalshiSigner("trade-id", TRADING_KEY, purpose="trading")


def client(pauses: Pauses | None = None, **kw: Any) -> KalshiTradingClient:
    pauses = pauses or Pauses()
    return KalshiTradingClient(
        KalshiSettings(), trading_signer(), sleep=pauses.sleep, wall_clock=lambda: 1000.0, **kw
    )


def order(**kw: Any) -> CreateOrder:
    base = dict(
        ticker="KXA-1",
        side=Side.BID,
        count=Decimal("2"),
        price=Decimal("0.01"),
        exchange_index=0,
        client_order_id=COID,
    )
    return CreateOrder(**{**base, **kw})


def body(route: respx.Route, index: int = -1) -> dict[str, Any]:
    parsed: dict[str, Any] = json.loads(route.calls[index].request.content)
    return parsed


# --- who may write, and where -----------------------------------------------------------------


def test_only_a_trading_signer_can_make_a_trading_client() -> None:
    with pytest.raises(TradingRefused, match="trading key"):
        KalshiTradingClient(KalshiSettings(), KalshiSigner("data-id", TRADING_KEY))


def test_production_needs_an_explicit_opt_in() -> None:
    prod = KalshiSettings(env="production")  # type: ignore[arg-type]
    with pytest.raises(TradingRefused, match="production"):
        KalshiTradingClient(prod, trading_signer())
    KalshiTradingClient(prod, trading_signer(), allow_production=True)


@respx.mock
@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/markets"),
        ("GET", "/portfolio/balance"),
        ("POST", "/portfolio/orders"),  # the legacy create endpoint
        ("DELETE", "/portfolio/orders/o-1"),
        ("POST", "/portfolio/events/orders/o-1/anything"),
        ("POST", "/portfolio/events/orders/o-1/amend/extra"),
        ("PUT", "/portfolio/events/orders"),
        ("POST", "/portfolio/subaccounts/transfer"),
        ("POST", "/account/api_usage_level/upgrade"),
        ("DELETE", "/portfolio/events/orders/a/b"),
    ],
)
async def test_nothing_outside_the_allowed_routes_can_be_sent(method: str, path: str) -> None:
    route = respx.route().respond(json={})
    async with client() as c:
        with pytest.raises(TradingRefused, match="not an allowed trading route"):
            await c._http.request(method, BASE + path)
    assert not route.called


# --- requests ---------------------------------------------------------------------------------


@respx.mock
async def test_create_sends_exactly_the_documented_body_signed_with_the_trading_key() -> None:
    route = respx.post(ORDERS).respond(201, json=CREATED)
    async with client() as c:
        created = await c.create_order(order())
    assert body(route) == {
        "ticker": "KXA-1",
        "client_order_id": COID,
        "side": "bid",
        "count": "2.00",
        "price": "0.0100",
        "time_in_force": "good_till_canceled",
        "self_trade_prevention_type": "taker_at_cross",
        "exchange_index": 0,
    }
    assert created.order_id == "o-1" and created.fill_count == 0 and not created.recovered
    req = route.calls.last.request
    assert req.headers[HEADER_KEY] == "trade-id"
    ts = req.headers[HEADER_TIMESTAMP]
    TRADING_KEY.public_key().verify(
        base64.b64decode(req.headers[HEADER_SIGNATURE]),
        f"{ts}POST/trade-api/v2/portfolio/events/orders".encode(),
    )


@respx.mock
async def test_optional_flags_are_sent_only_when_set() -> None:
    route = respx.post(ORDERS).respond(201, json=CREATED)
    async with client() as c:
        await c.create_order(order(post_only=True, expiration_time=1800000000))
        await c.create_order(order(time_in_force=TimeInForce.IMMEDIATE_OR_CANCEL, reduce_only=True))
    first, second = body(route, 0), body(route, 1)
    assert first["post_only"] is True and first["expiration_time"] == 1800000000
    assert "reduce_only" not in first
    assert second["reduce_only"] is True and second["time_in_force"] == "immediate_or_cancel"
    assert "post_only" not in second and "expiration_time" not in second


@pytest.mark.parametrize(
    ("change", "error"),
    [
        (dict(price=0.01), TypeError),
        (dict(count=2), TypeError),
        (dict(price=Decimal("0.00001")), ValueError),
        (dict(count=Decimal("1.001")), ValueError),
        (dict(price=Decimal(0)), ValueError),
        (dict(price=Decimal(1)), ValueError),
        (dict(count=Decimal(0)), ValueError),
        (dict(client_order_id=""), ValueError),
        (dict(reduce_only=True), ValueError),  # needs IOC
        (dict(time_in_force=TimeInForce.FILL_OR_KILL, expiration_time=1), ValueError),
    ],
)
async def test_a_malformed_order_never_reaches_the_network(
    change: dict[str, Any], error: type
) -> None:
    with respx.mock(assert_all_called=False) as mock:
        route = mock.route().respond(json={})
        async with client() as c:
            with pytest.raises(error):
                await c.create_order(order(**change))
        assert not route.called


def test_fixed_point_is_exact() -> None:
    assert fixed_point(Decimal("1E+2"), 2, "x") == "100.00"
    assert fixed_point(Decimal("0.5"), 4, "x") == "0.5000"
    with pytest.raises(ValueError):
        fixed_point(Decimal("NaN"), 2, "x")
    with pytest.raises(ValueError):
        fixed_point(Decimal("-1"), 2, "x")


# --- a lost answer ----------------------------------------------------------------------------


@respx.mock
@pytest.mark.parametrize("failure", ["500", "503", "502", "504", "timeout", "reset"])
async def test_an_ambiguous_create_is_sent_once_then_looked_up_and_found(failure: str) -> None:
    effect: Any = {
        "500": httpx.Response(500),
        "502": httpx.Response(502),
        "503": httpx.Response(503),
        "504": httpx.Response(504),
        "timeout": httpx.ReadTimeout("slow"),
        "reset": httpx.ReadError("reset"),
    }[failure]
    post = respx.post(ORDERS)
    post.side_effect = [effect]
    listing = respx.get(f"{BASE}/portfolio/orders").respond(
        json={"orders": [{**RESTING, "order_id": "other", "client_order_id": "x"}, RESTING]}
    )
    async with client() as c:
        created = await c.create_order(order())
    assert post.call_count == 1  # never sent twice
    assert created.recovered and created.order_id == "o-1" and created.remaining_count == 2
    params = listing.calls.last.request.url.params
    assert params["ticker"] == "KXA-1" and params["min_ts"] == "995"  # 5 s before the attempt


@respx.mock
async def test_an_order_that_cannot_be_found_is_reported_unknown_not_assumed_lost() -> None:
    post = respx.post(ORDERS)
    post.side_effect = [httpx.ReadTimeout("slow")]
    listing = respx.get(f"{BASE}/portfolio/orders").respond(json={"orders": [], "cursor": ""})
    pauses = Pauses()
    async with client(pauses) as c:
        with pytest.raises(WriteOutcomeUnknown) as info:
            await c.create_order(order())
    assert info.value.ref == COID and info.value.kind == "create_order"
    assert post.call_count == 1 and listing.call_count == 4
    assert pauses.sleeps == [0.5, 1.0, 2.0]


@respx.mock
async def test_a_stateful_exchange_that_drops_the_answer_ends_with_exactly_one_order() -> None:
    placed: list[dict[str, Any]] = []

    def post(request: httpx.Request) -> httpx.Response:
        sent = json.loads(request.content)
        if all(o["client_order_id"] != sent["client_order_id"] for o in placed):
            placed.append({**RESTING, "client_order_id": sent["client_order_id"]})
        raise httpx.ReadTimeout("the answer was lost")

    respx.post(ORDERS).mock(side_effect=post)
    respx.get(f"{BASE}/portfolio/orders").mock(
        side_effect=lambda r: httpx.Response(200, json={"orders": placed})
    )
    async with client() as c:
        created = await c.create_order(order())
    assert len(placed) == 1 and created.recovered and created.client_order_id == COID


@respx.mock
async def test_lookup_follows_cursors() -> None:
    respx.post(ORDERS).mock(side_effect=[httpx.ReadTimeout("slow")])
    listing = respx.get(f"{BASE}/portfolio/orders")
    listing.side_effect = [
        httpx.Response(200, json={"orders": [], "cursor": "c2"}),
        httpx.Response(200, json={"orders": [RESTING], "cursor": ""}),
    ]
    async with client() as c:
        assert (await c.create_order(order())).recovered
    assert listing.calls[1].request.url.params["cursor"] == "c2"


@respx.mock
async def test_a_definite_rejection_is_not_looked_up_or_retried() -> None:
    post = respx.post(ORDERS).respond(400, json={"error": "insufficient balance"})
    listing = respx.get(f"{BASE}/portfolio/orders").respond(json={"orders": []})
    async with client() as c:
        with pytest.raises(KalshiAPIError) as info:
            await c.create_order(order())
    assert info.value.status == 400 and post.call_count == 1 and not listing.called


@respx.mock
async def test_a_conflict_is_resolved_by_looking_for_the_existing_order() -> None:
    respx.post(ORDERS).respond(409, json={"error": "exists"})
    respx.get(f"{BASE}/portfolio/orders").respond(json={"orders": [RESTING]})
    async with client() as c:
        assert (await c.create_order(order())).recovered
    respx.get(f"{BASE}/portfolio/orders").respond(json={"orders": []})
    async with client() as c:
        with pytest.raises(KalshiAPIError):
            await c.create_order(order())


@respx.mock
async def test_rate_limiting_is_retried_a_bounded_number_of_times() -> None:
    post = respx.post(ORDERS)
    post.side_effect = [httpx.Response(429), httpx.Response(429), httpx.Response(201, json=CREATED)]
    pauses = Pauses()
    async with client(pauses) as c:
        assert (await c.create_order(order())).order_id == "o-1"
    assert post.call_count == 3 and pauses.sleeps == [0.5, 1.0]

    post.side_effect = [httpx.Response(429)] * 10
    post.reset()
    async with client(Pauses()) as c:
        with pytest.raises(KalshiAPIError) as info:
            await c.create_order(order())
    assert info.value.status == 429 and post.call_count == 4  # first try + three retries


# --- cancels ----------------------------------------------------------------------------------

TARGET = CancelTarget("o-1", 3, "KXA-1")


@respx.mock
async def test_cancel_names_the_shard_and_market() -> None:
    route = respx.delete(f"{ORDERS}/o-1").respond(
        json={"order_id": "o-1", "client_order_id": COID, "reduced_by": "2.00", "ts_ms": 1}
    )
    async with client() as c:
        result = await c.cancel_order(TARGET)
    assert result.reduced_by == 2 and not result.already_gone
    params = route.calls.last.request.url.params
    assert params["exchange_index"] == "3" and params["market_ticker"] == "KXA-1"


@respx.mock
async def test_cancel_after_a_lost_answer_is_retried_and_a_404_then_means_done() -> None:
    route = respx.delete(f"{ORDERS}/o-1")
    route.side_effect = [httpx.ReadTimeout("slow"), httpx.Response(404, json={})]
    async with client() as c:
        result = await c.cancel_order(TARGET)
    assert result.already_gone and route.call_count == 2

    route.side_effect = [httpx.Response(404, json={})]
    route.reset()
    async with client() as c:
        with pytest.raises(KalshiAPIError):  # first-try 404: there was no such order
            await c.cancel_order(TARGET)

    route.side_effect = [httpx.Response(503), httpx.Response(200, json={"order_id": "o-1"})]
    route.reset()
    async with client() as c:
        assert not (await c.cancel_order(TARGET)).already_gone


@respx.mock
async def test_cancel_gives_up_as_unknown_after_repeated_ambiguity() -> None:
    route = respx.delete(f"{ORDERS}/o-1")
    route.side_effect = [httpx.ReadTimeout("slow")] * 10
    async with client(Pauses()) as c:
        with pytest.raises(WriteOutcomeUnknown):
            await c.cancel_order(TARGET)
    assert route.call_count == 4


@respx.mock
async def test_batch_cancel_reports_each_order_and_cancel_all_sends_nothing_else() -> None:
    batch = respx.delete(f"{ORDERS}/batched").respond(
        json={
            "orders": [
                {"order_id": "o-1", "reduced_by": "2.00"},
                {"order_id": "o-2", "error": {"code": "not_found"}},
            ]
        }
    )
    every = respx.delete(ORDERS).respond(204)
    async with client() as c:
        results = await c.cancel_orders([TARGET, CancelTarget("o-2", 0, "KXB-1")])
        await c.cancel_all()
        assert await c.cancel_orders([]) == []
    assert [r.error is None for r in results] == [True, False]
    assert body(batch) == {
        "orders": [
            {"order_id": "o-1", "exchange_index": 3, "market_ticker": "KXA-1"},
            {"order_id": "o-2", "exchange_index": 0, "market_ticker": "KXB-1"},
        ]
    }
    assert every.call_count == 1 and not every.calls.last.request.content


# --- amend, decrease, transfer ----------------------------------------------------------------


@respx.mock
async def test_amend_and_decrease_send_the_documented_bodies() -> None:
    amend = respx.post(f"{ORDERS}/o-1/amend").respond(
        json={"order_id": "o-1", "remaining_count": "3.00", "fill_count": "0.00"}
    )
    decrease = respx.post(f"{ORDERS}/o-1/decrease").respond(
        json={"order_id": "o-1", "remaining_count": "1.00"}
    )
    async with client() as c:
        amended = await c.amend_order(
            "o-1",
            ticker="KXA-1",
            side=Side.ASK,
            price=Decimal("0.5"),
            count=Decimal("3"),
            exchange_index=3,
        )
        by = await c.decrease_order(
            "o-1", exchange_index=3, market_ticker="KXA-1", reduce_by=Decimal("1")
        )
        to = await c.decrease_order(
            "o-1", exchange_index=3, market_ticker="KXA-1", reduce_to=Decimal("1")
        )
    assert body(amend) == {
        "ticker": "KXA-1",
        "side": "ask",
        "price": "0.5000",
        "count": "3.00",
        "exchange_index": 3,
    }
    assert amended.remaining_count == 3 and by.remaining_count == 1 and to.order_id == "o-1"
    assert body(decrease, 0)["reduce_by"] == "1.00" and "reduce_to" not in body(decrease, 0)
    assert body(decrease, 1)["reduce_to"] == "1.00" and "reduce_by" not in body(decrease, 1)


async def test_decrease_needs_exactly_one_target() -> None:
    async with client() as c:
        for kw in ({}, {"reduce_by": Decimal(1), "reduce_to": Decimal(1)}):
            with pytest.raises(ValueError, match="exactly one"):
                await c.decrease_order("o-1", exchange_index=0, market_ticker="KXA-1", **kw)


@respx.mock
async def test_amend_and_decrease_are_never_resent_after_a_lost_answer() -> None:
    amend = respx.post(f"{ORDERS}/o-1/amend")
    amend.side_effect = [httpx.ReadTimeout("slow")]
    decrease = respx.post(f"{ORDERS}/o-1/decrease")
    decrease.side_effect = [httpx.Response(503)]
    async with client() as c:
        with pytest.raises(WriteOutcomeUnknown) as a:
            await c.amend_order(
                "o-1",
                ticker="KXA-1",
                side=Side.BID,
                price=Decimal("0.1"),
                count=Decimal(1),
                exchange_index=0,
            )
        with pytest.raises(WriteOutcomeUnknown) as d:
            await c.decrease_order(
                "o-1", exchange_index=0, market_ticker="KXA-1", reduce_by=Decimal(1)
            )
    assert (a.value.ref, d.value.ref) == ("o-1", "o-1")
    assert amend.call_count == 1 and decrease.call_count == 1


@respx.mock
async def test_a_transfer_is_in_centicents() -> None:
    route = respx.post(f"{BASE}/portfolio/intra_exchange_instance_transfer")
    route.side_effect = [httpx.Response(200, json={"transfer_id": "t-9"})]
    async with client(Pauses()) as c:
        tid = await c.transfer(
            source_shard=0, destination_shard=3, amount_e6=2_500_100, transfer_id="tr-1"
        )
    assert tid == "t-9"
    assert body(route) == {
        "source": "event_contract",
        "destination": "event_contract",
        "amount": 25_001,
        "client_transfer_id": "tr-1",
        "source_exchange_shard": 0,
        "destination_exchange_shard": 3,
    }


@respx.mock
@pytest.mark.parametrize(
    "failure", [httpx.Response(503), httpx.Response(500), httpx.ReadTimeout("x")]
)
async def test_a_transfer_with_no_clear_answer_is_never_sent_again(failure: Any) -> None:
    """Measured live: the same client_transfer_id sent twice moved the money twice."""
    route = respx.post(f"{BASE}/portfolio/intra_exchange_instance_transfer")
    route.side_effect = [failure, httpx.Response(200, json={"transfer_id": "t-9"})]
    async with client(Pauses()) as c:
        with pytest.raises(WriteOutcomeUnknown) as info:
            await c.transfer(
                source_shard=0, destination_shard=3, amount_e6=1_000_000, transfer_id="tr-1"
            )
    assert info.value.kind == "transfer" and info.value.ref == "tr-1"
    assert route.call_count == 1


@respx.mock
async def test_a_refused_transfer_is_an_api_error_and_a_rate_limit_is_waited_out() -> None:
    route = respx.post(f"{BASE}/portfolio/intra_exchange_instance_transfer")
    route.side_effect = [httpx.Response(429), httpx.Response(200, json={"transfer_id": "t-1"})]
    async with client(Pauses()) as c:
        assert (
            await c.transfer(source_shard=0, destination_shard=1, amount_e6=100, transfer_id="a")
            == "t-1"
        )
    route.side_effect = [httpx.Response(400, json={"error": "no"})]
    route.reset()
    async with client(Pauses()) as c:
        with pytest.raises(KalshiAPIError):
            await c.transfer(source_shard=0, destination_shard=1, amount_e6=100, transfer_id="b")


async def test_bad_transfers_are_refused_before_the_network() -> None:
    async with client() as c:
        for amount in (0, -100, 150, 1):
            with pytest.raises(ValueError):
                await c.transfer(
                    source_shard=0, destination_shard=1, amount_e6=amount, transfer_id="t"
                )
        with pytest.raises(ValueError):
            await c.transfer(source_shard=1, destination_shard=1, amount_e6=100, transfer_id="t")


# --- the write budget -------------------------------------------------------------------------


class FakeTime:
    def __init__(self) -> None:
        self.now = 0.0
        self.waits: list[float] = []

    def clock(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.now += seconds


@respx.mock
async def test_writes_spend_the_documented_token_costs_from_the_write_bucket() -> None:
    ft = FakeTime()
    bucket = TokenBucket(90, 90, clock=ft.clock, sleep=ft.sleep)  # 100/s x 0.9
    respx.post(ORDERS).respond(201, json=CREATED)
    respx.delete(f"{ORDERS}/o-1").respond(json={"order_id": "o-1"})
    respx.delete(f"{ORDERS}/batched").respond(json={"orders": []})
    async with client(bucket=bucket) as c:
        for _ in range(9):  # 9 x 10 = 90 tokens: the whole bucket
            await c.create_order(order())
        assert ft.waits == []
        await c.create_order(order())  # needs 10 more: waits 10/90 s
        assert ft.waits == pytest.approx([10 / 90])
        ft.waits.clear()
        for _ in range(5):  # cancels are 2 tokens each
            await c.cancel_order(TARGET)
        await c.cancel_orders([TARGET] * 3)  # billed per order: 6 tokens
    assert sum(ft.waits) == pytest.approx((5 * 2 + 3 * 2) / 90, rel=0.01)


def test_the_write_bucket_follows_the_account_tier() -> None:
    bucket = write_bucket(KalshiSettings(write_tokens_per_second=300))
    assert bucket._capacity == pytest.approx(270)
    assert write_bucket(KalshiSettings())._capacity == pytest.approx(90)
