import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient
from kalshiterm_server import auth, db
from kalshiterm_server.analytics.base import AnalysisContext
from kalshiterm_server.analytics.builtin.alert_evaluator import AlertEvaluator
from kalshiterm_server.api.app import create_app
from kalshiterm_server.config import ServerSettings
from sqlalchemy import text

pytestmark = pytest.mark.db

NOW = datetime.now(UTC)


async def seed(url: str) -> dict[str, str]:
    engine = db.make_engine(url)
    try:
        async with engine.begin() as conn:
            for event, exclusive in (("KX-E1", True), ("KX-NOEXCL", False)):
                await conn.execute(
                    text(
                        "INSERT INTO events (event_ticker, series_ticker, title, "
                        "mutually_exclusive) VALUES (:e, 'KX', 'An event', :x)"
                    ),
                    {"e": event, "x": exclusive},
                )
            for ticker in ("KX-E1-A", "KX-E1-B", "KXMVECROSS-S1-A"):
                await conn.execute(
                    text(
                        "INSERT INTO markets (ticker, event_ticker, market_type, status) "
                        "VALUES (:t, 'KX-E1', 'binary', 'active')"
                    ),
                    {"t": ticker},
                )
        tokens = {}
        for name in ("alice", "bob"):
            await auth.add_user(engine, name)
            tokens[name] = (await auth.create_token(engine, name, "read"))[1]
        return tokens
    finally:
        await engine.dispose()


@pytest.fixture
def world(migrated_db_url: str) -> Any:
    tokens = asyncio.run(seed(migrated_db_url))
    settings = ServerSettings(
        db_url=migrated_db_url, alert_rules_max_per_user=3, alert_min_cooldown_seconds=120
    )
    with TestClient(create_app(settings)) as client:
        yield client, tokens, migrated_db_url


def call(world: Any, who: str, method: str, path: str, **kw: Any) -> Any:
    client, tokens, _ = world
    return client.request(method, path, headers={"Authorization": f"Bearer {tokens[who]}"}, **kw)


def watch(world: Any, who: str, ticker: str) -> None:
    assert call(world, who, "PUT", f"/v1/watchlist/{ticker}").status_code == 200


def new_rule(world: Any, who: str = "alice", **overrides: Any) -> Any:
    body = {"metric": "market_moves.change_15m", "ticker": "KX-E1-A", "op": "abs_gt",
            "threshold": "0.10", **overrides}  # fmt: skip
    return call(world, who, "POST", "/v1/alert-rules", json=body)


def sql(world: Any, statement: str, **params: Any) -> Any:
    async def go() -> Any:
        engine = db.make_engine(world[2])
        try:
            async with engine.begin() as conn:
                result = await conn.execute(text(statement), params)
                return result.all() if result.returns_rows else None
        finally:
            await engine.dispose()

    return asyncio.run(go())


def test_everything_needs_a_token(world: Any) -> None:
    client = world[0]
    for method, path in (
        ("GET", "/v1/alert-metrics"), ("POST", "/v1/alert-rules"), ("GET", "/v1/alert-rules"),
        ("GET", "/v1/alert-rules/1"), ("DELETE", "/v1/alert-rules/1"), ("GET", "/v1/alerts"),
        ("POST", "/v1/alerts/1/acknowledge"),
    ):  # fmt: skip
        assert client.request(method, path).status_code == 401, (method, path)


def test_the_catalog_says_what_can_be_alerted_on(world: Any) -> None:
    metrics = {
        m["metric"]: m for m in call(world, "alice", "GET", "/v1/alert-metrics").json()["metrics"]
    }
    assert metrics["market_moves.change_15m"]["ops"] == ["gt", "lt", "abs_gt"]
    assert metrics["market_moves.change_15m"]["unit"] == "dollars"
    assert metrics["arbitrage_bid.profit"]["subject"] == "event"
    assert metrics["market_quotes.bid_size"]["unit"] == "contracts"


def test_a_market_rule_needs_the_market_on_your_watchlist(world: Any) -> None:
    refused = new_rule(world)
    assert refused.status_code == 422
    assert refused.json() == {"error": "not_watched", "ticker": "KX-E1-A"}
    watch(world, "alice", "KX-E1-A")
    created = new_rule(world, name="big move")
    assert created.status_code == 201
    rule = created.json()
    assert rule["threshold"] == "0.100000" and rule["unit"] == "dollars"
    assert rule["metric"] == "market_moves.change_15m" and rule["ticker"] == "KX-E1-A"
    assert rule["cooldown_seconds"] == 900 and rule["enabled"] is True and rule["active"] is False
    assert (
        rule["last_fired_at"] is None and rule["name"] == "big move" and "event_ticker" not in rule
    )


def test_an_event_rule_names_an_event(world: Any) -> None:
    ok = new_rule(world, metric="arbitrage_bid.profit", ticker=None, event_ticker="KX-E1",
                  op="gt", threshold="0")  # fmt: skip
    assert ok.status_code == 201 and ok.json()["event_ticker"] == "KX-E1"
    not_exclusive = new_rule(world, metric="arbitrage_bid.profit", ticker=None,
                             event_ticker="KX-NOEXCL", op="gt", threshold="0")  # fmt: skip
    assert not_exclusive.status_code == 422 and not_exclusive.json()["error"] == "not_alertable"


@pytest.mark.parametrize(
    ("changes", "status", "error"),
    [
        ({"metric": "market_moves.nope"}, 422, "unknown_metric"),
        ({"metric": "nonsense"}, 422, "invalid_parameter"),
        ({"op": "lt", "metric": "market_moves.volume_1h"}, 422, "invalid_comparison"),
        ({"op": "between"}, 422, "invalid_parameter"),
        ({"threshold": "0.1234567"}, 422, "invalid_parameter"),  # finer than a millionth
        ({"threshold": "abc"}, 422, "invalid_parameter"),
        ({"threshold": "1e30"}, 422, "invalid_parameter"),
        ({"threshold": "0"}, 422, "invalid_parameter"),  # abs_gt needs a positive threshold
        ({"cooldown_seconds": 119}, 422, "cooldown_too_short"),
        ({"ticker": None, "event_ticker": "KX-E1"}, 422, "invalid_parameter"),  # wrong subject
        ({"event_ticker": "KX-E1"}, 422, "invalid_parameter"),  # both subjects
        ({"ticker": "KX-NOPE"}, 404, "not_found"),
        ({"ticker": "KXMVECROSS-S1-A"}, 422, "not_alertable"),
        ({"surprise": 1}, 422, "invalid_parameter"),
    ],
)
def test_bad_rules_are_refused_with_a_reason_and_nothing_is_created(
    world: Any, changes: dict[str, Any], status: int, error: str
) -> None:
    for ticker in ("KX-E1-A", "KXMVECROSS-S1-A"):
        call(world, "alice", "PUT", f"/v1/watchlist/{ticker}")
    response = new_rule(world, **changes)
    assert response.status_code == status and response.json()["error"] == error
    assert call(world, "alice", "GET", "/v1/alert-rules").json()["rules"] == []


def test_the_rule_limit_is_per_user(world: Any) -> None:
    for who in ("alice", "bob"):
        watch(world, who, "KX-E1-A")
    for n in range(3):
        assert new_rule(world, threshold=f"0.{n + 1}0").status_code == 201
    over = new_rule(world, threshold="0.90")
    assert over.status_code == 409 and over.json() == {"error": "rule_limit", "limit": 3}
    assert new_rule(world, "bob").status_code == 201
    assert call(world, "alice", "GET", "/v1/alert-rules").json()["limit"] == 3


def test_users_cannot_see_or_touch_each_others_rules(world: Any) -> None:
    watch(world, "alice", "KX-E1-A")
    rule_id = new_rule(world).json()["id"]
    assert call(world, "bob", "GET", "/v1/alert-rules").json()["rules"] == []
    for method in ("GET", "DELETE"):
        assert call(world, "bob", method, f"/v1/alert-rules/{rule_id}").status_code == 404
    assert (
        call(
            world, "bob", "PATCH", f"/v1/alert-rules/{rule_id}", json={"enabled": False}
        ).status_code
        == 404
    )  # noqa: E501
    assert call(world, "alice", "GET", f"/v1/alert-rules/{rule_id}").json()["enabled"] is True


def test_changing_a_rule(world: Any) -> None:
    watch(world, "alice", "KX-E1-A")
    rule_id = new_rule(world).json()["id"]
    sql(world, "update alert_rules set active = true")
    url = f"/v1/alert-rules/{rule_id}"
    renamed = call(world, "alice", "PATCH", url, json={"name": "renamed", "cooldown_seconds": 600})
    assert renamed.json()["name"] == "renamed" and renamed.json()["cooldown_seconds"] == 600
    assert renamed.json()["active"] is True  # not re-armed by a rename
    moved = call(world, "alice", "PATCH", url, json={"threshold": "0.25"}).json()
    assert moved["threshold"] == "0.250000" and moved["active"] is False  # re-armed
    off = call(world, "alice", "PATCH", url, json={"enabled": False}).json()
    assert off["enabled"] is False
    assert (
        call(world, "alice", "PATCH", url, json={"cooldown_seconds": 5}).json()["error"]
        == "cooldown_too_short"
    )  # noqa: E501
    assert (
        call(world, "alice", "PATCH", url, json={"threshold": "0"}).json()["error"]
        == "invalid_parameter"
    )  # noqa: E501
    assert call(world, "alice", "PATCH", url, json={}).status_code == 200  # nothing to change


def test_deleting_a_rule(world: Any) -> None:
    watch(world, "alice", "KX-E1-A")
    rule_id = new_rule(world).json()["id"]
    assert call(world, "alice", "DELETE", f"/v1/alert-rules/{rule_id}").json() == {
        "id": rule_id,
        "deleted": True,
    }  # noqa: E501
    assert call(world, "alice", "GET", f"/v1/alert-rules/{rule_id}").status_code == 404
    assert call(world, "alice", "DELETE", f"/v1/alert-rules/{rule_id}").status_code == 404


def fire(world: Any, who: str, n: int) -> None:
    """Insert ``n`` alerts for a user, as the evaluator would have."""
    ids = sql(world, "select id from users where name = :n", n=who)
    for i in range(n):
        sql(
            world,
            "insert into alerts (user_id, fired_at, rule_name, analyzer, metric, event_ticker, "
            "op, threshold_e6, value_e6) values "
            "(:u, :t, :n, 'arbitrage_bid', 'profit', 'KX-E1', 'gt', 0, 20000)",
            u=ids[0][0], t=NOW + timedelta(seconds=i), n=f"{who}-{i}",
        )  # fmt: skip


def test_alerts_are_listed_newest_first_in_pages_and_only_the_callers(world: Any) -> None:
    fire(world, "alice", 5)
    fire(world, "bob", 2)
    first = call(world, "alice", "GET", "/v1/alerts", params={"limit": 2}).json()
    assert [a["rule_name"] for a in first["alerts"]] == ["alice-4", "alice-3"]
    assert first["alerts"][0]["value"] == "0.020000" and first["alerts"][0]["unit"] == "dollars"
    assert (
        first["alerts"][0]["event_ticker"] == "KX-E1"
        and first["alerts"][0]["metric"] == "arbitrage_bid.profit"
    )  # noqa: E501
    second = call(
        world, "alice", "GET", "/v1/alerts", params={"limit": 2, "cursor": first["next_cursor"]}
    ).json()  # noqa: E501
    assert [a["rule_name"] for a in second["alerts"]] == ["alice-2", "alice-1"]
    last = call(
        world, "alice", "GET", "/v1/alerts", params={"limit": 2, "cursor": second["next_cursor"]}
    ).json()  # noqa: E501
    assert [a["rule_name"] for a in last["alerts"]] == ["alice-0"] and last["next_cursor"] is None
    assert len(call(world, "bob", "GET", "/v1/alerts").json()["alerts"]) == 2
    for bad in ("garbage", "bm90YW51bWJlcg"):  # not ours / not a number
        assert call(world, "alice", "GET", "/v1/alerts", params={"cursor": bad}).status_code == 422
    assert call(world, "alice", "GET", "/v1/alerts", params={"limit": 0}).status_code == 422


def test_acknowledging_is_idempotent_private_and_filters_the_list(world: Any) -> None:
    fire(world, "alice", 3)
    alerts = call(world, "alice", "GET", "/v1/alerts").json()["alerts"]
    target = alerts[0]["id"]
    assert call(world, "bob", "POST", f"/v1/alerts/{target}/acknowledge").status_code == 404
    assert call(world, "alice", "POST", f"/v1/alerts/{target}/acknowledge").json() == {
        "id": target,
        "acknowledged": True,
    }  # noqa: E501
    first_time = call(world, "alice", "GET", "/v1/alerts").json()["alerts"][0]["acknowledged_at"]
    assert first_time is not None
    call(world, "alice", "POST", f"/v1/alerts/{target}/acknowledge")
    assert (
        call(world, "alice", "GET", "/v1/alerts").json()["alerts"][0]["acknowledged_at"]
        == first_time
    )  # noqa: E501
    open_ones = call(world, "alice", "GET", "/v1/alerts", params={"unacknowledged": "true"}).json()
    assert [a["id"] for a in open_ones["alerts"]] == [a["id"] for a in alerts[1:]]
    assert call(world, "alice", "POST", "/v1/alerts/999999/acknowledge").status_code == 404


def test_from_a_rule_to_an_alert_you_can_read_and_acknowledge(world: Any) -> None:
    """The whole path: create a rule over HTTP, let the evaluator see a matching result."""
    watch(world, "alice", "KX-E1-A")
    rule = new_rule(world, metric="market_quotes.spread", op="gt", threshold="0.05",
                    name="wide spread").json()  # fmt: skip
    sql(
        world,
        "insert into analysis_results (ts, analyzer, metric, market_id, value_e6) "
        "select :t, 'market_quotes', 'spread', id, 80000 from markets where ticker = 'KX-E1-A'",
        t=NOW,
    )

    async def evaluate() -> None:
        engine = db.make_engine(world[2])
        try:
            await AlertEvaluator().run(AnalysisContext(engine, NOW + timedelta(seconds=5)))
        finally:
            await engine.dispose()

    asyncio.run(evaluate())
    [alert] = call(world, "alice", "GET", "/v1/alerts").json()["alerts"]
    assert alert["rule_id"] == rule["id"] and alert["rule_name"] == "wide spread"
    assert alert["ticker"] == "KX-E1-A" and alert["value"] == "0.080000"
    assert alert["threshold"] == "0.050000" and alert["acknowledged_at"] is None
    assert call(world, "alice", "GET", f"/v1/alert-rules/{rule['id']}").json()["active"] is True
    assert call(world, "bob", "GET", "/v1/alerts").json()["alerts"] == []
    call(world, "alice", "POST", f"/v1/alerts/{alert['id']}/acknowledge")
    assert (
        call(world, "alice", "GET", "/v1/alerts", params={"unacknowledged": "true"}).json()[
            "alerts"
        ]
        == []
    )  # noqa: E501
