"""The trading routes over HTTP and WebSocket: who can reach them, what they accept, and how every
failure is reported."""

import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from kalshi_core.rest import KalshiAPIError
from kalshi_core.trading import WriteOutcomeUnknown
from kalshiterm_client.risk import RiskPolicy
from kalshiterm_client.ui.app import create_app
from kalshiterm_client.ui.security import new_token
from starlette.testclient import TestClient
from trading_fakes import FakeRest, balance, order
from ui_trading_fixtures import PHRASE_DEMO, PHRASE_PROD, Harness, make

PORT = 51237
TOKEN = new_token()
READ = {"Authorization": f"Bearer {TOKEN}"}
WRITE = {**READ, "Origin": f"http://127.0.0.1:{PORT}"}
ORDER = {"ticker": "KXA-1", "action": "buy", "outcome": "yes", "count": "2", "price": "0.40"}
TYPED = "KXA-1 buy yes 2 0.40"

World = tuple[TestClient, Harness]


def build(tmp_path: Path, **kw: Any) -> Iterator[World]:
    h = make(tmp_path, **kw)
    app = create_app(
        port=PORT,
        token=TOKEN,
        environment="production" if h.session.production else "demo",
        assets_dir=tmp_path / "none",
        trading=h.session,
    )
    with TestClient(app, base_url=f"http://127.0.0.1:{PORT}") as client:
        yield client, h


@pytest.fixture
def world(tmp_path: Path) -> Iterator[World]:
    yield from build(tmp_path)


@pytest.fixture
def demo(tmp_path: Path) -> Iterator[World]:
    yield from build(tmp_path, production=False)


def enable(client: TestClient, phrase: str = PHRASE_PROD) -> Any:
    return client.post("/api/mode/enable", headers=WRITE, json={"phrase": phrase})


# --- who can reach it ----------------------------------------------------------------------------

STATE_CHANGING = [
    ("post", "/api/mode/enable", {"phrase": PHRASE_PROD}),
    ("post", "/api/mode/disable", {"cancel_open_orders": False}),
    ("post", "/api/trading/orders/prepare", ORDER),
    ("post", "/api/trading/orders/o-1/amend", {"price": "0.5"}),
    ("post", "/api/trading/orders/execute", {"prepare_id": "x" * 16}),
    ("delete", "/api/trading/orders/o-1", None),
    ("post", "/api/trading/kill", None),
    ("post", "/api/trading/resume", {"confirm": True}),
]


@pytest.mark.parametrize(("method", "path", "body"), STATE_CHANGING)
def test_every_trading_route_needs_the_token_and_the_right_origin(
    world: World, method: str, path: str, body: Any
) -> None:
    client, h = world
    send = getattr(client, method)
    kwargs = {} if body is None else {"json": body}
    assert send(path, **kwargs).status_code == 403  # neither: the Origin is checked first
    assert send(path, headers={"Origin": WRITE["Origin"]}, **kwargs).status_code == 401  # no token
    assert send(path, headers=READ, **kwargs).status_code == 403  # token but no Origin
    hostile = {**READ, "Origin": "https://attacker.example"}
    assert send(path, headers=hostile, **kwargs).status_code == 403
    wrong_host = {**WRITE, "Host": "attacker.example"}
    assert send(path, headers=wrong_host, **kwargs).status_code == 400
    assert h.modes == [] and h.trading.calls == [] and h.session.mode == "read-only"


def test_the_account_and_mode_reads_need_the_token(world: World) -> None:
    client, _ = world
    assert client.get("/api/mode").status_code == 401
    assert client.get("/api/trading/account").status_code == 401


# --- off by default -----------------------------------------------------------------------------


def test_everything_that_trades_answers_403_while_trading_is_off(world: World) -> None:
    client, h = world
    off = {"error": "trading_off"}
    assert client.get("/api/trading/account", headers=READ).json()["error"] == off["error"]
    for method, path, body in STATE_CHANGING[2:]:
        if path.endswith("/kill"):
            continue  # the kill switch is the one exception, tested below
        kwargs = {} if body is None else {"json": body}
        response = getattr(client, method)(path, headers=WRITE, **kwargs)
        assert response.status_code == 403 and response.json()["error"] == "trading_off", path
    assert h.modes == [] and h.trading.calls == []


def test_the_mode_endpoint_describes_the_switch(world: World) -> None:
    client, _ = world
    info = client.get("/api/mode", headers=READ).json()
    assert info["mode"] == "read-only" and info["production"] is True and info["permitted"] is True
    assert (
        info["enable_phrase"] == PHRASE_PROD and info["limits"]["max_order_notional"] == "1.000000"
    )
    assert info["idle_minutes"] == 30 and info["disconnect_seconds"] == 60
    assert client.get("/api/session", headers=READ).json()["mode"] == "read-only"


def test_the_kill_switch_works_with_trading_off(tmp_path: Path) -> None:
    rest = FakeRest()
    rest.orders = [order("o-1", "KXA-1")]
    for client, h in build(tmp_path, rest=rest):
        response = client.post("/api/trading/kill", headers=WRITE)
        assert response.status_code == 200
        assert response.json()["clean"] is True and response.json()["halted"] is True
        assert h.modes == ["exit"] and h.state.halted and rest.orders == []
        assert client.get("/api/session", headers=READ).json()["mode"] == "read-only"


# --- switching on and off -----------------------------------------------------------------------


def test_the_wrong_phrase_and_missing_permission_are_refused_clearly(tmp_path: Path) -> None:
    for client, h in build(tmp_path):
        wrong = enable(client, "enable it")
        assert wrong.status_code == 422 and wrong.json()["error"] == "wrong_phrase"
        assert h.modes == []
    for client, h in build(tmp_path / "np", policy=RiskPolicy(production_enabled=False)):
        refused = enable(client)
        assert refused.status_code == 403 and refused.json()["error"] == "production_not_permitted"
        assert "risk.toml" in refused.json()["detail"] and h.modes == []
        assert client.get("/api/mode", headers=READ).json()["permitted"] is False


def test_switching_on_then_using_then_off_again(world: World) -> None:
    client, h = world
    on = enable(client)
    assert on.status_code == 200 and on.json()["mode"] == "trading"
    assert client.get("/api/session", headers=READ).json()["mode"] == "trading"
    assert client.get("/api/trading/account", headers=READ).status_code == 200
    assert enable(client).status_code == 200 and h.modes == ["trade"]  # idempotent
    off = client.post("/api/mode/disable", headers=WRITE, json={"cancel_open_orders": False})
    assert off.status_code == 200 and off.json()["mode"] == "read-only"
    assert client.get("/api/trading/account", headers=READ).status_code == 403


def test_switching_on_without_a_key_or_with_kalshi_down_is_a_clear_error(world: World) -> None:
    from kalshi_core.auth import AuthError

    client, h = world
    h.fail = AuthError("KALSHI_TRADING_KEY_ID is not set")
    key = enable(client)
    assert key.status_code == 409 and key.json()["error"] == "no_trading_key"
    h.fail = None
    assert enable(client).status_code == 200


# --- an order over HTTP -------------------------------------------------------------------------


def test_prepare_then_execute_with_the_typed_order(world: World) -> None:
    client, h = world
    enable(client)
    prepared = client.post("/api/trading/orders/prepare", headers=WRITE, json=ORDER)
    assert prepared.status_code == 200
    view = prepared.json()
    assert view["approved"] and view["prepare_id"] and view["requires_phrase"] is True
    assert view["lines"][0].startswith("BUY 2 YES KXA-1") and view["worst_case_cost"] == "0.840000"
    assert h.trading.calls == []

    body = {"prepare_id": view["prepare_id"]}
    missing = client.post("/api/trading/orders/execute", headers=WRITE, json=body)
    assert missing.status_code == 422 and missing.json()["error"] == "wrong_order_phrase"
    wrong = client.post(
        "/api/trading/orders/execute",
        headers=WRITE,
        json={**body, "phrase": "KXA-1 buy yes 3 0.40"},
    )
    assert wrong.status_code == 422 and h.trading.calls == []
    done = client.post("/api/trading/orders/execute", headers=WRITE, json={**body, "phrase": TYPED})
    assert (
        done.status_code == 200
        and done.json()["order_id"] == "o-new"
        and done.json()["kind"] == "order"
    )
    assert h.trading.names() == ["create"]
    again = client.post(
        "/api/trading/orders/execute", headers=WRITE, json={**body, "phrase": TYPED}
    )
    assert again.status_code == 404 and again.json()["error"] == "prepare_expired"
    assert h.trading.names() == ["create"]  # still exactly one order


def test_a_refused_order_comes_back_with_every_reason_and_no_id(world: World) -> None:
    client, _ = world
    enable(client)
    refused = client.post(
        "/api/trading/orders/prepare", headers=WRITE, json={**ORDER, "count": "50", "price": "0.90"}
    ).json()
    assert refused["approved"] is False and refused["prepare_id"] is None
    assert {r["code"] for r in refused["refusals"]} >= {"order_too_large", "notional_too_large"}


def test_a_price_finer_than_kalshi_takes_is_a_422_and_nothing_is_stored(world: World) -> None:
    client, h = world
    enable(client)
    response = client.post(
        "/api/trading/orders/prepare", headers=WRITE, json={**ORDER, "price": "0.123456"}
    )
    assert response.status_code == 422 and "decimal places" in str(response.json())
    assert h.session._prepared == {}


@pytest.mark.parametrize(
    "change",
    [
        {"count": 2},  # a JSON number: precision lost, refused
        {"price": 0.4},
        {"count": "-1"},
        {"count": "1e3"},
        {"count": ""},
        {"price": "0.1.2"},
        {"ticker": "bad ticker!"},
        {"ticker": "x" * 129},
        {"action": "hold"},
        {"outcome": "maybe"},
        {"time_in_force": "fill_or_kill"},
        {"surprise": True},
        {"post_only": "yes"},
    ],
)
def test_malformed_orders_are_refused_before_the_service_hears_of_them(
    world: World, change: dict[str, Any]
) -> None:
    client, h = world
    enable(client)
    response = client.post("/api/trading/orders/prepare", headers=WRITE, json={**ORDER, **change})
    assert response.status_code == 422
    assert h.session._prepared == {} and h.trading.calls == []


def test_the_demo_needs_no_typed_order(demo: World) -> None:
    client, _ = demo
    assert enable(client, PHRASE_DEMO).status_code == 200
    view = client.post("/api/trading/orders/prepare", headers=WRITE, json=ORDER).json()
    assert view["requires_phrase"] is False
    done = client.post(
        "/api/trading/orders/execute", headers=WRITE, json={"prepare_id": view["prepare_id"]}
    )
    assert done.status_code == 200


def test_amend_cancel_and_resume_over_http(tmp_path: Path) -> None:
    rest = FakeRest()
    rest.balance_value = balance("10.3490", "0", "0", "0")
    rest.orders = [order("o-1", "KXA-1", yes_price="0.4000"), order("o-2", "KXA-1")]
    for client, h in build(tmp_path, production=False, rest=rest):
        enable(client, PHRASE_DEMO)
        prepared = client.post(
            "/api/trading/orders/o-1/amend", headers=WRITE, json={"price": "0.45"}
        ).json()
        assert prepared["kind"] == "amend" and prepared["approved"]
        done = client.post(
            "/api/trading/orders/execute",
            headers=WRITE,
            json={"prepare_id": prepared["prepare_id"]},
        )
        assert done.json()["kind"] == "amend" and h.trading.names() == ["amend"]
        assert client.delete("/api/trading/orders/o-2", headers=WRITE).json() == {
            "order_id": "o-2", "ok": True, "message": "cancelled 1 left",
        }  # fmt: skip
        h.state.halt("test")
        assert client.post("/api/trading/resume", headers=WRITE, json={}).status_code == 422
        assert (
            client.post("/api/trading/resume", headers=WRITE, json={"confirm": False}).status_code
            == 422
        )
        assert h.state.halted
        assert client.post("/api/trading/resume", headers=WRITE, json={"confirm": True}).json() == {
            "halted": False
        }
        assert not h.state.halted


@pytest.mark.parametrize("order_id", ["a b", "x" * 65, "a;b", "a%2Fb", ".."])
def test_a_bad_order_id_never_reaches_kalshi(demo: World, order_id: str) -> None:
    client, h = demo
    enable(client, PHRASE_DEMO)
    assert client.delete(f"/api/trading/orders/{order_id}", headers=WRITE).status_code in (404, 422)
    assert client.post(
        f"/api/trading/orders/{order_id}/amend", headers=WRITE, json={"price": "0.5"}
    ).status_code in (404, 422)
    assert h.trading.calls == []


# --- every way an order can fail is reported, never swallowed -----------------------------------


def test_failures_become_clear_statuses(demo: World) -> None:
    client, h = demo
    enable(client, PHRASE_DEMO)

    def go() -> Any:
        view = client.post("/api/trading/orders/prepare", headers=WRITE, json=ORDER).json()
        return client.post(
            "/api/trading/orders/execute", headers=WRITE, json={"prepare_id": view["prepare_id"]}
        )

    h.trading.create_error = WriteOutcomeUnknown("create_order", "coid-9")
    unknown = go()
    assert unknown.status_code == 502 and unknown.json()["error"] == "outcome_unknown"
    assert unknown.json()["ref"] == "coid-9" and "orders and fills" in unknown.json()["detail"]
    h.trading.create_error = KalshiAPIError(400, '{"error": "secret internal details"}')
    kalshi = go()
    assert kalshi.status_code == 502 and kalshi.json()["error"] == "kalshi_error"
    assert kalshi.json()["kalshi_status"] == 400 and "secret" not in kalshi.text
    h.trading.create_error = None
    h.state.halt("kill switch")
    view = client.post("/api/trading/orders/prepare", headers=WRITE, json=ORDER).json()
    assert view["approved"] is False and view["prepare_id"] is None


def test_a_refusal_after_preparing_is_a_422_with_the_reasons(demo: World) -> None:
    client, h = demo
    enable(client, PHRASE_DEMO)
    view = client.post("/api/trading/orders/prepare", headers=WRITE, json=ORDER).json()
    h.state.halt("kill switch")  # fired while the user was reading
    response = client.post(
        "/api/trading/orders/execute", headers=WRITE, json={"prepare_id": view["prepare_id"]}
    )
    assert response.status_code == 422 and response.json()["error"] == "order_refused"
    assert response.json()["refusals"][0]["code"] == "halted" and h.trading.calls == []


def test_a_changed_plan_is_a_409(demo: World) -> None:
    client, h = demo
    enable(client, PHRASE_DEMO)
    view = client.post(
        "/api/trading/orders/prepare", headers=WRITE, json={**ORDER, "ticker": "KXT-1"}
    ).json()
    assert view["funding"]
    h.rest.balance_value = balance("0.0000", "10.3490", "0.0000", "0.0000")
    response = client.post(
        "/api/trading/orders/execute", headers=WRITE, json={"prepare_id": view["prepare_id"]}
    )
    assert response.status_code == 409 and response.json()["error"] == "plan_changed"
    assert h.trading.calls == []


# --- the live feed --------------------------------------------------------------------------------


def socket(client: TestClient) -> Any:
    return client.websocket_connect(
        "/api/ws", headers={"host": f"127.0.0.1:{PORT}", "origin": f"http://127.0.0.1:{PORT}"}
    )


def collect(ws: Any) -> list[dict[str, Any]]:
    """Everything the page has been sent so far. A ping goes in behind it and the replies are read
    up to the pong, so this can never wait forever for a message that is not coming."""
    ws.send_json({"type": "ping"})
    seen: list[dict[str, Any]] = []
    for _ in range(60):
        message = ws.receive_json()
        if message["type"] == "pong":
            return seen
        seen.append(message)
    raise AssertionError("no pong")


def find(seen: list[dict[str, Any]], kind: str, **fields: Any) -> dict[str, Any] | None:
    for message in seen:
        if message["type"] == kind and all(message.get(k) == v for k, v in fields.items()):
            return message
    return None


def test_every_page_hears_when_trading_is_switched_on_and_off(world: World) -> None:
    client, _ = world
    with socket(client) as ws:
        ws.send_json({"type": "auth", "token": TOKEN})
        assert ws.receive_json() == {
            "type": "hello",
            "mode": "read-only",
            "environment": "production",
        }
        enable(client)
        assert find(collect(ws), "mode") == {"type": "mode", "mode": "trading", "reason": "user"}
        client.post("/api/mode/disable", headers=WRITE, json={"cancel_open_orders": False})
        assert find(collect(ws), "mode", mode="read-only") == {
            "type": "mode", "mode": "read-only", "reason": "user",
        }  # fmt: skip
    with socket(client) as late:  # a page opened while trading is on is told at once
        enable(client)
        late.send_json({"type": "auth", "token": TOKEN})
        assert late.receive_json()["mode"] == "trading"


def test_the_account_feed_is_pushed_to_pages_that_ask_and_ends_when_trading_does(
    world: World,
) -> None:
    client, _ = world
    with socket(client) as ws:
        ws.send_json({"type": "auth", "token": TOKEN})
        ws.receive_json()
        ws.send_json({"type": "account", "enabled": True})  # asked while off: nothing to send yet
        assert find(collect(ws), "account") is None
        enable(client)
        time.sleep(0.3)  # the push (0.01 s here) follows switching on
        message = find(collect(ws), "account")
        assert message is not None and message["view"]["synced"]
        assert message["view"]["shards"][0]["cash"] == "10.349000"
        client.post("/api/mode/disable", headers=WRITE, json={"cancel_open_orders": False})
        assert find(collect(ws), "account", view=None) == {"type": "account", "view": None}


def test_a_page_that_did_not_ask_gets_no_account_updates(world: World) -> None:
    client, _ = world
    with socket(client) as ws:
        ws.send_json({"type": "auth", "token": TOKEN})
        ws.receive_json()
        enable(client)
        time.sleep(0.3)  # long enough for the account push to have been sent to anyone who asked
        seen = collect(ws)
        assert find(seen, "mode", mode="trading") is not None  # it did hear about the mode
        assert find(seen, "account") is None  # but not the account


def test_a_page_that_asks_while_trading_is_already_on_gets_the_view_at_once(world: World) -> None:
    client, _ = world
    enable(client)
    time.sleep(0.2)  # the push that followed switching on has come and gone
    with socket(client) as ws:
        ws.send_json({"type": "auth", "token": TOKEN})
        assert ws.receive_json()["mode"] == "trading"
        ws.send_json({"type": "account", "enabled": True})
        message = find(collect(ws), "account")
        assert message is not None and message["view"]["shards"][0]["cash"] == "10.349000"


def test_a_touch_keeps_trading_alive_and_silence_lets_it_lapse(tmp_path: Path) -> None:
    for client, h in build(tmp_path, policy=RiskPolicy(production_enabled=True, ui_idle_minutes=1)):
        with socket(client) as ws:
            ws.send_json({"type": "auth", "token": TOKEN})
            ws.receive_json()
            enable(client)
            h.clock.now += 50
            assert client.post("/api/session/touch", headers=WRITE).json() == {"ok": True}
            h.clock.now += 50
            time.sleep(0.1)
            assert client.get("/api/session", headers=READ).json()["mode"] == "trading"
            h.clock.now += 11
            time.sleep(0.2)  # the watcher checks every 0.01 s
            lapse = find(collect(ws), "mode", mode="read-only")
            assert lapse == {"type": "mode", "mode": "read-only", "reason": "idle"}


def test_the_kill_switch_over_http_while_trading_uses_the_live_runtime(tmp_path: Path) -> None:
    rest = FakeRest()
    rest.orders = [order("o-1", "KXA-1")]
    for client, h in build(tmp_path, rest=rest):
        enable(client)
        response = client.post("/api/trading/kill", headers=WRITE)
        assert response.status_code == 200 and response.json()["halted"] is True
        assert h.modes == ["trade"] and h.state.halted and rest.orders == []
        assert client.get("/api/session", headers=READ).json()["mode"] == "trading"  # still on


def test_shutting_the_app_down_switches_trading_off(tmp_path: Path) -> None:
    for client, h in build(tmp_path):
        enable(client)
        assert h.session.mode == "trading"
    # leaving the `with TestClient(...)` block ran the app's shutdown
    assert h.session.mode == "read-only" and h.session._runtime is None and h.session._stack is None
