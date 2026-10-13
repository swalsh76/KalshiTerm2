"""``kterm watchlist ...`` and ``kterm alerts ...`` against a scripted HTTPS server: what each
command sends (verb, path, body), what it prints, and how it fails."""

import json
from collections.abc import Iterator
from typing import Any

import pytest
from fake_server import FakeServer, make_certificate
from kalshiterm_client import secrets
from kalshiterm_client.cli import app
from kalshiterm_client.pinning import fingerprint_of_pem
from kalshiterm_client.profiles import ProfileStore
from typer.testing import CliRunner

TOKEN = "kt_7_" + "Zy9" * 14
WHEN = "2026-10-12T18:00:00.000000+00:00"
runner = CliRunner()


@pytest.fixture
def server() -> Iterator[FakeServer]:
    cert = make_certificate()
    fake = FakeServer(cert=cert).start()
    store = ProfileStore()
    store.add("lab", fake.url)
    store.save_pin("lab", cert[0], fingerprint_of_pem(cert[0]))
    secrets.set_token("lab", TOKEN)
    yield fake
    fake.stop()


def run(*args: str) -> tuple[int, str, str]:
    result = runner.invoke(app, list(args))
    return result.exit_code, result.stdout, result.stderr


def writes(server: FakeServer) -> list[tuple[str, str, Any]]:
    return [c for c in server.calls if c[0] != "GET"]


def rule(**changes: Any) -> dict[str, Any]:
    return {
        "id": 7,
        "name": "wide",
        "metric": "market_quotes.spread",
        "op": "gt",
        "threshold": "0.050000",
        "unit": "dollars",
        "cooldown_seconds": 900,
        "enabled": True,
        "active": False,
        "last_fired_at": None,
        "created_at": WHEN,
        "ticker": "KXA-E1-X",
        **changes,
    }


def alert(n: int, **changes: Any) -> dict[str, Any]:
    return {
        "id": n,
        "rule_id": 7,
        "rule_name": "wide",
        "fired_at": WHEN,
        "metric": "market_quotes.spread",
        "op": "gt",
        "threshold": "0.050000",
        "value": "0.080000",
        "unit": "dollars",
        "acknowledged_at": None,
        "ticker": "KXA-E1-X",
        **changes,
    }


CATALOG = {
    "metrics": [
        {
            "metric": "market_quotes.spread",
            "subject": "market",
            "unit": "dollars",
            "ops": ["gt", "lt"],
            "meaning": "spread",
        },
        {
            "metric": "arbitrage_bid.profit",
            "subject": "event",
            "unit": "dollars",
            "ops": ["gt"],
            "meaning": "net profit",
        },
    ]
}


# ------------------------------------------------------------------ watchlist


WATCHLIST = {
    "markets": [
        {
            "ticker": "KXA-E1-X",
            "event_title": "An event",
            "status": "active",
            "added_at": WHEN,
            "capturing": True,
        }
    ],
    "limit": 50,
}


def test_watchlist_shows_your_markets_whether_or_not_you_say_list(server: FakeServer) -> None:
    server.routes["/v1/watchlist"] = WATCHLIST
    for args in (["watchlist"], ["watchlist", "list"]):
        code, out, _ = run(*args)
        assert (
            code == 0 and "KXA-E1-X" in out and "capturing" in out and "1 of 50 places used" in out
        )
    code, out, _ = run("watchlist", "list", "--json")
    assert json.loads(out)["limit"] == 50 and json.loads(out)["markets"][0]["capturing"] is True


def test_adding_and_removing_reports_each_ticker_and_continues_after_a_failure(
    server: FakeServer,
) -> None:
    server.routes["PUT /v1/watchlist/A"] = {"ticker": "A", "added": True}
    server.routes["PUT /v1/watchlist/B"] = {"ticker": "B", "added": False}
    server.routes["PUT /v1/watchlist/NOPE"] = (404, {"error": "not_found"})
    server.routes["PUT /v1/watchlist/C"] = {"ticker": "C", "added": True}
    code, out, err = run("watchlist", "add", "A", "B", "NOPE", "C")
    assert code == 1  # one failed...
    assert (
        "added: A" in out and "already on your list: B" in out and "added: C" in out
    )  # ...the rest ran
    assert "NOPE: not found (not_found)" in err
    assert [(m, p) for m, p, _ in writes(server)] == [
        ("PUT", "/v1/watchlist/A"),
        ("PUT", "/v1/watchlist/B"),
        ("PUT", "/v1/watchlist/NOPE"),
        ("PUT", "/v1/watchlist/C"),
    ]
    server.routes["DELETE /v1/watchlist/A"] = {"ticker": "A", "removed": True}
    server.routes["DELETE /v1/watchlist/B"] = {"ticker": "B", "removed": False}
    code, out, _ = run("watchlist", "remove", "A", "B")
    assert code == 0 and "removed: A" in out and "was not on your list: B" in out


def test_a_full_watchlist_is_explained(server: FakeServer) -> None:
    server.routes["PUT /v1/watchlist/A"] = (409, {"error": "watchlist_full", "limit": 50})
    code, _, err = run("watchlist", "add", "A")
    assert code == 1 and "watchlist_full" in err and "A:" in err


# ------------------------------------------------------------------ alerts: reading


def test_alerts_list_marks_new_and_acknowledged_ones(server: FakeServer) -> None:
    server.routes["/v1/alerts"] = {
        "alerts": [alert(2), alert(1, acknowledged_at=WHEN)],
        "next_cursor": None,
    }
    for args in (["alerts"], ["alerts", "list"]):
        code, out, _ = run(*args)
        assert (
            code == 0
            and "NEW" in out
            and "acknowledged" in out
            and "> 0.05" in out
            and "0.08" in out
        )
    code, out, _ = run("alerts", "list", "-u", "--limit", "5")
    assert code == 0
    params = server.requests[-1][0]
    assert "unacknowledged=true" in params and "limit=5" in params
    server.routes["/v1/alerts"] = {"alerts": [], "next_cursor": None}
    assert "no unacknowledged alerts" in run("alerts", "list", "-u")[1]
    assert "no alerts" in run("alerts", "list")[1]


def test_rules_and_the_catalog_are_listed(server: FakeServer) -> None:
    server.routes["/v1/alert-rules"] = {
        "rules": [rule(), rule(id=8, enabled=False), rule(id=9, active=True)],
        "limit": 50,
    }
    server.routes["/v1/alert-metrics"] = CATALOG
    code, out, _ = run("alerts", "rules")
    assert (
        code == 0
        and "armed" in out
        and "disabled" in out
        and "triggered" in out
        and "3 of 50 rules used" in out
    )
    code, out, _ = run("alerts", "metrics")
    assert (
        code == 0
        and "market_quotes.spread" in out
        and "gt, lt" in out
        and "arbitrage_bid.profit" in out
    )
    assert json.loads(run("alerts", "rules", "--json")[1])["rules"][0]["threshold"] == "0.050000"


# ------------------------------------------------------------------ alerts: writing


def test_ack_sends_one_post_per_id_and_reports_problems(server: FakeServer) -> None:
    server.routes["POST /v1/alerts/1/acknowledge"] = {"id": 1, "acknowledged": True}
    server.routes["POST /v1/alerts/2/acknowledge"] = (404, {"error": "not_found"})
    code, out, err = run("alerts", "ack", "1", "2")
    assert code == 1 and "acknowledged 1 alert(s)" in out and "alert 2: not found" in err
    assert [(m, p) for m, p, _ in writes(server)] == [
        ("POST", "/v1/alerts/1/acknowledge"),
        ("POST", "/v1/alerts/2/acknowledge"),
    ]


def test_ack_all_acknowledges_every_unacknowledged_alert_and_needs_a_clear_request(
    server: FakeServer,
) -> None:
    server.routes["/v1/alerts"] = {"alerts": [alert(3), alert(2)], "next_cursor": None}
    for n in (2, 3):
        server.routes[f"POST /v1/alerts/{n}/acknowledge"] = {"id": n, "acknowledged": True}
    code, out, _ = run("alerts", "ack", "--all")
    assert code == 0 and "acknowledged 2 alert(s)" in out
    assert sorted(p for _, p, _ in writes(server)) == [
        "/v1/alerts/2/acknowledge",
        "/v1/alerts/3/acknowledge",
    ]
    before = len(server.calls)
    assert run("alerts", "ack")[0] == 1 and run("alerts", "ack", "1", "--all")[0] == 1
    assert len(server.calls) == before  # nothing was asked


def test_add_picks_ticker_or_event_from_the_catalog_and_sends_the_exact_body(
    server: FakeServer,
) -> None:
    server.routes["/v1/alert-metrics"] = CATALOG
    server.routes["POST /v1/alert-rules"] = (201, rule())
    code, out, _ = run(
        "alerts",
        "add",
        "market_quotes.spread",
        "KXA-E1-X",
        "gt",
        "0.05",
        "--name",
        "wide",
        "--cooldown",
        "600",
    )
    assert code == 0 and "rule 7: market_quotes.spread of KXA-E1-X > 0.05" in out
    [(_, _, body)] = writes(server)
    assert body == {
        "metric": "market_quotes.spread",
        "op": "gt",
        "threshold": "0.05",
        "ticker": "KXA-E1-X",
        "cooldown_seconds": 600,
        "name": "wide",
    }
    server.calls.clear()
    server.routes["POST /v1/alert-rules"] = (
        201,
        rule(metric="arbitrage_bid.profit", ticker=None, event_ticker="KX-E1"),
    )
    assert run("alerts", "add", "arbitrage_bid.profit", "KX-E1", "gt", "0")[0] == 0
    [(_, _, event_body)] = writes(server)
    assert event_body["event_ticker"] == "KX-E1" and "ticker" not in event_body


@pytest.mark.parametrize(
    ("args", "fragment"),
    [
        (["market_quotes.nope", "T", "gt", "1"], "unknown metric"),
        (["market_quotes.spread", "T", "gt", "0.1234567"], "six decimal places"),
        (["market_quotes.spread", "T", "gt", "abc"], "not a number"),
    ],
)
def test_add_refuses_the_obviously_wrong_before_creating_anything(
    server: FakeServer, args: list[str], fragment: str
) -> None:
    server.routes["/v1/alert-metrics"] = CATALOG
    code, _, err = run("alerts", "add", *args)
    assert code == 1 and fragment in err and writes(server) == []


@pytest.mark.parametrize(
    ("refusal", "fragment"),
    [
        ((422, {"error": "not_watched", "ticker": "KXA-E1-X"}), "kterm watchlist add KXA-E1-X"),
        (
            (422, {"error": "invalid_comparison", "allowed": ["gt", "lt"]}),
            "allowed for this metric: gt, lt",
        ),
        ((422, {"error": "cooldown_too_short", "minimum": 300}), "at least 300 seconds"),
        ((409, {"error": "rule_limit", "limit": 50}), "you can have 50 rules"),
        ((404, {"error": "not_found"}), "KXA-E1-X was not found"),
    ],
)
def test_add_turns_the_servers_refusals_into_what_to_do_next(
    server: FakeServer, refusal: tuple[int, dict[str, Any]], fragment: str
) -> None:
    server.routes["/v1/alert-metrics"] = CATALOG
    server.routes["POST /v1/alert-rules"] = refusal
    code, _, err = run("alerts", "add", "market_quotes.spread", "KXA-E1-X", "gt", "0.05")
    assert code == 1 and fragment in err and TOKEN not in err


def test_set_sends_only_what_changes(server: FakeServer) -> None:
    server.routes["PATCH /v1/alert-rules/7"] = rule(enabled=False, threshold="0.200000")
    code, out, _ = run("alerts", "set", "7", "--disable", "--threshold", "0.2")
    assert code == 0 and "disabled" in out
    assert writes(server) == [
        ("PATCH", "/v1/alert-rules/7", {"enabled": False, "threshold": "0.2"})
    ]
    server.calls.clear()
    run("alerts", "set", "7", "--enable", "--name", "renamed")
    assert writes(server)[0][2] == {"enabled": True, "name": "renamed"}


def test_set_with_nothing_to_change_or_a_bad_number_asks_nothing(server: FakeServer) -> None:
    assert run("alerts", "set", "7")[0] == 1
    code, _, err = run("alerts", "set", "7", "--threshold", "1e-9")
    assert code == 1 and "six decimal places" in err and server.calls == []


def test_rm_deletes_each_rule_and_reports_the_missing_ones(server: FakeServer) -> None:
    server.routes["DELETE /v1/alert-rules/7"] = {"id": 7, "deleted": True}
    server.routes["DELETE /v1/alert-rules/8"] = (404, {"error": "not_found"})
    code, out, err = run("alerts", "rm", "7", "8")
    assert code == 1 and "deleted rule 7" in out and "rule 8: not found" in err


def test_writes_are_not_retried_when_the_server_fails(server: FakeServer) -> None:
    server.routes["PUT /v1/watchlist/A"] = (500, {"error": "boom"})
    code, _, err = run("watchlist", "add", "A")
    assert code == 1 and "the server answered 500" in err
    assert len(writes(server)) == 1  # one attempt: a retry could add it twice
