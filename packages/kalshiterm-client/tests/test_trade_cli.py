import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from kalshi_core.auth import AuthError
from kalshi_core.config import KalshiSettings
from kalshi_core.portfolio import Fill
from kalshi_core.rest import KalshiAPIError
from kalshi_core.trading import WriteOutcomeUnknown
from kalshiterm_client import trade_cli
from kalshiterm_client.account import AccountTracker
from kalshiterm_client.cli import app
from kalshiterm_client.orders import OrderService
from kalshiterm_client.risk import RiskPolicy
from kalshiterm_client.trade_runtime import Mode, Runtime
from kalshiterm_client.trading_state import TradingState
from trading_fakes import (
    T0,
    FakeRest,
    FakeSocket,
    FakeTrading,
    balance,
    order,
    position,
)
from typer.testing import CliRunner

runner = CliRunner()


@dataclass
class Harness:
    rest: FakeRest
    trading: FakeTrading
    state: TradingState
    modes: list[str]


def install(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    production: bool = False,
    policy: RiskPolicy | None = None,
    rest: FakeRest | None = None,
    start_error: Exception | None = None,
) -> Harness:
    monkeypatch.setenv("KTERM_CONFIG_DIR", str(tmp_path))
    for name in ("KALSHI_ENV", "KALSHI_KEY_ID", "KALSHI_PRIVATE_KEY_PATH"):
        monkeypatch.delenv(name, raising=False)
    if rest is None:
        rest = FakeRest()
        rest.balance_value = balance("10.3490", "0.0000", "0.0000", "0.0000")
    state = TradingState.load(tmp_path / "trading_state.json")
    trading = FakeTrading(rest, state)
    harness = Harness(rest, trading, state, [])
    settings = KalshiSettings(env="production" if production else "demo")  # type: ignore[arg-type]

    @asynccontextmanager
    async def factory(mode: Mode) -> AsyncIterator[Runtime]:
        harness.modes.append(mode)
        if start_error:
            raise start_error
        ws = FakeSocket()
        tracker = AccountTracker(rest, ws, state, clock=lambda: T0, balance_interval=3600)
        used = policy or RiskPolicy()
        service = OrderService(
            rest,
            trading,
            tracker,
            used,
            production=production,
            sleep=instant,
            clock=lambda: __import__("datetime").datetime.fromtimestamp(
                T0, __import__("datetime").UTC
            ),
        )
        runtime = Runtime(settings, used, state, rest, service, tracker, trading)  # type: ignore[arg-type]
        if mode in ("trade", "preview"):
            await tracker.start()
        try:
            yield runtime
        finally:
            await tracker.stop()

    monkeypatch.setattr(trade_cli, "runtime_factory", factory)
    return harness


async def instant(_: float) -> None:
    await asyncio.sleep(0)


def run(*args: str, input_: str | None = None) -> Any:
    return runner.invoke(app, list(args), input=input_)


# --- placing ----------------------------------------------------------------------------------


def test_a_dry_run_shows_what_would_happen_and_sends_nothing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    h = install(monkeypatch, tmp_path)
    result = run("buy", "KXA-1", "2", "0.40", "--dry-run")
    assert result.exit_code == 0, result.output
    assert "BUY 2 YES KXA-1 at $0.4000 limit" in result.output
    assert "worst-case cost $0.8" in result.output
    assert "dry run: nothing was sent" in result.output
    assert h.trading.calls == []


def test_a_dry_run_on_an_unfunded_shard_shows_the_transfer(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    h = install(monkeypatch, tmp_path)
    result = run("buy", "KXT-1", "2", "0.40", "--dry-run")
    assert result.exit_code == 0
    assert "first moves $0.8" in result.output and "from shard 0 to shard 3" in result.output
    assert h.trading.calls == []


def test_a_refused_order_lists_every_reason_and_exits_1(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    h = install(monkeypatch, tmp_path)
    result = run("buy", "KXA-1", "50", "0.90", "--yes")
    assert result.exit_code == 1
    assert result.output.count("REFUSED:") >= 2  # too many contracts and too much money
    assert h.trading.calls == []


def test_in_the_demo_an_order_asks_first_and_yes_skips_the_question(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    h = install(monkeypatch, tmp_path)
    declined = run("buy", "KXA-1", "2", "0.40", input_="n\n")
    assert declined.exit_code == 1 and "not sent" in declined.output
    assert h.trading.calls == []
    accepted = run("buy", "KXA-1", "2", "0.40", input_="y\n")
    assert accepted.exit_code == 0, accepted.output
    assert "order o-new placed" in accepted.output and h.trading.names() == ["create"]
    h.trading.calls.clear()
    assert run("buy", "KXA-1", "1", "0.30", "--yes").exit_code == 0
    assert h.trading.names() == ["create"]


def test_sell_and_the_no_side_are_described_and_sent_on_the_right_book(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    h = install(monkeypatch, tmp_path)
    rest = h.rest
    rest.positions = [position("KXA-1", "-2.00")]  # long 2 NO
    result = run("sell", "KXA-1", "1", "0.30", "--side", "no", "--yes")
    assert result.exit_code == 0, result.output
    assert "SELL 1 NO KXA-1 at $0.3000" in result.output
    sent = h.trading.calls[0][1]
    assert (sent.side.value, sent.price) == ("bid", Decimal("0.7"))  # sell NO 30c = buy YES 70c


def test_ioc_and_post_only_reach_the_order(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    h = install(monkeypatch, tmp_path)
    assert run("buy", "KXA-1", "1", "0.30", "--ioc", "--yes").exit_code == 0
    assert h.trading.calls[0][1].time_in_force.value == "immediate_or_cancel"
    h.trading.calls.clear()
    assert run("buy", "KXA-1", "1", "0.30", "--post-only", "--yes").exit_code == 0
    assert h.trading.calls[0][1].post_only is True


def test_the_dry_run_can_be_read_by_a_script(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    install(monkeypatch, tmp_path)
    result = run("buy", "KXT-1", "2", "0.40", "--dry-run", "--json")
    data = json.loads(result.output.split("\n", 1)[0] and result.output[result.output.index("{") :])
    assert data["approved"] is True and data["shard"] == 3
    assert data["funding"][0]["source"] == 0 and data["worst_case_cost_e6"] > 800_000
    refused = run("buy", "KXA-1", "50", "0.90", "--dry-run", "--json")
    assert refused.exit_code == 1
    assert json.loads(refused.output[refused.output.index("{") :])["approved"] is False


@pytest.mark.parametrize(
    "args", [("abc", "0.4"), ("0", "0.4"), ("-1", "0.4"), ("2", "x"), ("2", "0"), ("2", "nan")]
)
def test_nonsense_numbers_are_refused_before_anything_starts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, args: tuple[str, str]
) -> None:
    h = install(monkeypatch, tmp_path)
    result = run("buy", "KXA-1", *args, "--yes")
    assert result.exit_code in (1, 2)  # 2 is the argument parser objecting (a leading "-")
    assert h.modes == [] and h.trading.calls == []  # not even the runtime was opened


# --- production -------------------------------------------------------------------------------


def test_production_is_refused_unless_risk_toml_allows_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    h = install(monkeypatch, tmp_path, production=True)
    result = run("buy", "KXA-1", "2", "0.40", "--yes")
    assert result.exit_code == 1
    assert "PRODUCTION" in result.output and "production_enabled" in result.output
    assert h.trading.calls == []


def test_production_needs_the_order_typed_out_and_yes_does_not_skip_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    allowed = RiskPolicy(production_enabled=True)
    h = install(monkeypatch, tmp_path, production=True, policy=allowed)
    for typed in (
        "",
        "yes",
        "KXA-1 buy yes 2 0.45",
        "KXA-2 buy yes 2 0.4",
        "KXA-1 sell yes 2 0.4",
        "KXA-1 buy no 2 0.4",
        "KXA-1 buy yes 3 0.4",
        "KXA-1 buy yes 2",
    ):
        result = run("buy", "KXA-1", "2", "0.40", "--yes", input_=typed + "\n")
        assert result.exit_code == 1, (typed, result.output)
        assert "nothing was sent" in result.output
        assert "--yes does not skip" in result.output
    assert run("buy", "KXA-1", "2", "0.40", "--yes").exit_code == 1  # no input at all
    assert h.trading.calls == []
    ok = run("buy", "KXA-1", "2", "0.40", input_="KXA-1 BUY Yes 2.00 0.4\n")
    assert ok.exit_code == 0, ok.output
    assert h.trading.names() == ["create"] and "PRODUCTION: real money" in ok.output


# --- failures are one clear line --------------------------------------------------------------


def test_kalshi_rejecting_an_order_is_reported_with_its_reason(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    h = install(monkeypatch, tmp_path)
    h.trading.create_error = KalshiAPIError(400, '{"error": "price not on the tick"}')
    result = run("buy", "KXA-1", "2", "0.40", "--yes")
    assert result.exit_code == 1 and "Kalshi said 400" in result.output
    assert "price not on the tick" in result.output


def test_an_unknown_outcome_exits_2_and_names_the_id_to_look_for(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    h = install(monkeypatch, tmp_path)
    h.trading.create_error = WriteOutcomeUnknown("create_order", "coid-77")
    result = run("buy", "KXA-1", "2", "0.40", "--yes")
    assert result.exit_code == 2
    assert "coid-77" in result.output and "do not repeat" in result.output


def test_a_missing_key_is_a_clear_message(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    install(monkeypatch, tmp_path, start_error=AuthError("KALSHI_TRADING_KEY_ID is not set"))
    result = run("buy", "KXA-1", "2", "0.40", "--dry-run")
    assert result.exit_code == 1 and "KALSHI_TRADING_KEY_ID is not set" in result.output


# --- amend ------------------------------------------------------------------------------------


def test_amend_shows_the_replacement_and_sends_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    rest = FakeRest()
    rest.balance_value = balance("10.3490", "0.0000", "0.0000", "0.0000")
    rest.orders = [order("o-1", "KXA-1")]
    h = install(monkeypatch, tmp_path, rest=rest)
    dry = run("amend", "o-1", "--price", "0.45", "--dry-run")
    assert dry.exit_code == 0 and "amend order o-1" in dry.output and h.trading.calls == []
    done = run("amend", "o-1", "--price", "0.45", "--yes")
    assert done.exit_code == 0, done.output
    assert "order o-1 amended" in done.output and h.trading.names() == ["amend"]
    assert run("amend", "o-1").exit_code == 1  # nothing to change
    assert run("amend", "nope", "--price", "0.4").exit_code == 1


# --- getting out ------------------------------------------------------------------------------


def test_cancel_reports_each_order_and_exits_1_if_any_could_not_be_cancelled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    rest = FakeRest()
    rest.orders = [order("o-1", "KXA-1"), order("o-done", "KXA-1", status_="executed")]
    h = install(monkeypatch, tmp_path, rest=rest)
    ok = run("cancel", "o-1")
    assert ok.exit_code == 0 and "o-1: cancelled" in ok.output
    mixed = run("cancel", "o-done", "nope")
    assert mixed.exit_code == 1
    assert "already executed" in mixed.output and "404" in mixed.output
    assert h.modes[0] == "exit"  # no tracker, no WebSocket needed to get out


def test_cancel_still_works_while_trading_is_halted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    rest = FakeRest()
    rest.orders = [order("o-1", "KXA-1")]
    h = install(monkeypatch, tmp_path, rest=rest)
    h.state.halt("test")
    assert run("cancel", "o-1").exit_code == 0


def test_kill_halts_cancels_and_says_so(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    rest = FakeRest()
    rest.orders = [order("o-1", "KXA-1"), order("o-2", "KXA-1")]
    h = install(monkeypatch, tmp_path, rest=rest)
    result = run("kill")
    assert result.exit_code == 0, result.output
    assert "HALTED" in result.output and "no orders are resting" in result.output
    assert h.state.halted and h.modes == ["exit"]
    assert run("buy", "KXA-1", "1", "0.30", "--dry-run").exit_code == 1  # now refused
    as_json = run("kill", "--json")
    assert json.loads(as_json.stdout)["clean"] is True


def test_kill_that_cannot_clear_the_orders_exits_1_and_names_them(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    rest = FakeRest()
    rest.orders = [order("stuck", "KXA-1")]
    h = install(monkeypatch, tmp_path, rest=rest)
    h.trading.cancel_all_clears = False

    async def nothing(targets: Any) -> list[Any]:
        return []

    h.trading.cancel_orders = nothing  # type: ignore[method-assign]
    result = run("kill")
    assert result.exit_code == 1 and "STILL RESTING: stuck" in result.output
    assert h.state.halted


def test_resume_clears_the_halt_after_asking(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    h = install(monkeypatch, tmp_path)
    assert "not halted" in run("resume").output
    h.state.halt("kill switch (kterm kill)")
    declined = run("resume", input_="n\n")
    assert declined.exit_code == 1 and "still halted" in declined.output
    assert TradingState.load(h.state.path).halted
    done = run("resume", input_="y\n")
    assert done.exit_code == 0 and "halt cleared" in done.output
    assert not TradingState.load(h.state.path).halted
    h.state.halt("again")
    assert run("resume", "--yes").exit_code == 0


# --- looking ----------------------------------------------------------------------------------


def fill(trade_id: str, ts: int, **kw: Any) -> Fill:
    base = {
        "fill_id": trade_id,
        "trade_id": trade_id,
        "order_id": "o-1",
        "ticker": "KXA-1",
        "action": "buy",
        "outcome_side": "yes",
        "book_side": "bid",
        "count_fp": "2.00",
        "yes_price_dollars": "0.4000",
        "no_price_dollars": "0.6000",
        "fee_cost": "0.020000",
        "is_taker": True,
        "ts": ts,
        "created_time": "2026-10-13T12:00:00Z",
    }
    return Fill.model_validate({**base, **kw})


def test_balance_shows_every_shard_and_a_halt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    rest = FakeRest()
    rest.balance_value = balance("10.3490", "0.0000", "0.0000", "2.5000")
    h = install(monkeypatch, tmp_path, rest=rest)
    result = run("balance")
    assert result.exit_code == 0
    assert "10.34" in result.output and "2.50" in result.output and "12.84" in result.output
    assert "HALTED" not in result.output
    h.state.halt("daily loss limit reached")
    assert "TRADING HALTED (daily loss limit reached)" in run("balance").output
    data = json.loads(run("balance", "--json").stdout)
    assert data["halted"] is True and data["balance"]["balance_dollars"] == "12.8490"
    assert h.modes[0] == "read"


def test_positions_hide_flat_markets_unless_asked(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    rest = FakeRest()
    rest.positions = [
        position("KXA-1", "3.00", "1.20", "-0.25"),
        position("KXB-1", "-2.00", "0.80"),
        position("KXOLD-1", "0.00", "0", "0.30"),
    ]
    install(monkeypatch, tmp_path, rest=rest)
    out = run("positions").output
    assert "KXA-1" in out and "3 YES" in out and "KXB-1" in out and "2 NO" in out
    assert "KXOLD-1" not in out
    assert "KXOLD-1" in run("positions", "--all").output
    assert len(json.loads(run("positions", "--json").stdout)) == 2


def test_orders_show_resting_by_default_newest_first_and_can_be_limited(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    rest = FakeRest()
    old = order("o-old", "KXA-1")
    new = order("o-new", "KXB-1", outcome="no", book="ask", yes_price="0.3000")
    old = old.model_copy(
        update={
            "created_time": __import__("datetime").datetime(
                2026, 10, 1, tzinfo=__import__("datetime").UTC
            )
        }
    )
    new = new.model_copy(
        update={
            "created_time": __import__("datetime").datetime(
                2026, 10, 2, tzinfo=__import__("datetime").UTC
            )
        }
    )
    rest.orders = [old, new, order("o-done", "KXA-1", status_="executed")]
    install(monkeypatch, tmp_path, rest=rest)
    out = run("orders").output
    assert out.index("o-new") < out.index("o-old") and "o-done" not in out
    assert "ASK" in out and "0.30" in out and "0.70" in out  # the YES and the NO price
    assert "o-done" in run("orders", "--status", "all").output
    assert "o-old" not in run("orders", "--limit", "1").output
    assert run("orders", "--status", "bogus").exit_code == 1


def test_fills_are_newest_first(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    rest = FakeRest()
    rest.fills = [
        fill("t-1", 100),
        fill("t-2", 300, outcome_side="no", book_side="ask"),
        fill("t-3", 200, is_taker=False),
    ]
    install(monkeypatch, tmp_path, rest=rest)
    result = run("fills", "--json")
    assert [f["fill_id"] for f in json.loads(result.stdout)] == ["t-2", "t-3", "t-1"]
    table = run("fills", "--limit", "2").output
    assert "ASK" in table and "maker" in table and "taker" in table
    assert "0.60" in table and "0.40" in table  # both prices are shown, YES and NO
    # Kalshi labelled a sale of YES at 1c "sell, no, 0.99" (seen live): that label is never shown
    assert "SELL NO" not in table


def test_nothing_in_the_tables_is_ever_cut_so_ids_can_be_copied_into_cancel(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    long_id = "01a0fb02-44a8-776b-9801-c464cfe31239"
    long_ticker = "KXATPCHALLENGERMATCH-26OCT14PAVSCH-PAV"
    rest = FakeRest()
    rest.orders = [order(long_id, long_ticker, outcome="no", book="ask", status_="resting")]
    rest.fills = [fill("t-1", 100, ticker=long_ticker)]
    rest.positions = [position(long_ticker, "3.00", "1.2")]
    install(monkeypatch, tmp_path, rest=rest)
    for args in (("orders",), ("fills",), ("positions",)):
        out = run(*args).output
        assert "\u2026" not in out, (args, out)  # no ellipsis anywhere
        assert long_ticker in out
    orders_out = run("orders").output
    assert long_id in orders_out and "ASK" in orders_out and "resting" in orders_out
    assert "YES ($)" in orders_out and "CREATED (UTC)" in orders_out


def test_on_a_narrow_terminal_the_order_id_is_still_whole() -> None:
    from io import StringIO

    from kalshiterm_client import trade_views
    from rich.console import Console

    long_id = "01a0fb02-44a8-776b-9801-c464cfe31239"
    table = trade_views.orders_table([order(long_id, "KXATPCHALLENGERMATCH-26OCT14PAVSCH-PAV")])
    buffer = StringIO()
    Console(file=buffer, width=80, force_terminal=False).print(table)
    assert long_id in buffer.getvalue()  # other columns may be cut at the edge; the id never is


def test_dry_runs_open_the_preview_runtime_and_real_orders_the_trade_one(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    h = install(monkeypatch, tmp_path)
    run("buy", "KXA-1", "1", "0.30", "--dry-run")
    run("buy", "KXA-1", "1", "0.30", "--yes")
    run("sell", "KXA-1", "1", "0.30", "--dry-run", "--json")
    assert h.modes == ["preview", "trade", "preview"]


async def test_the_preview_client_cannot_send_anything() -> None:
    from kalshi_core.trading import TradingRefused
    from kalshiterm_client.trade_runtime import PreviewTrading

    preview = PreviewTrading()
    for call in (
        preview.create_order(None),
        preview.amend_order("o"),
        preview.cancel_order(None),
        preview.cancel_orders([]),
        preview.cancel_all(),
        preview.transfer(source_shard=0, destination_shard=1, amount_e6=100, transfer_id="t"),
    ):
        with pytest.raises(TradingRefused, match="dry run"):
            await call


@pytest.mark.parametrize(
    ("mode", "enabled", "allowed"),
    [
        ("trade", False, False),  # sending orders needs the switch
        ("trade", True, True),
        ("exit", False, True),  # cancelling and the kill switch never do
        ("exit", True, True),
    ],
)
def test_who_may_act_on_production(mode: str, enabled: bool, allowed: bool) -> None:
    from kalshiterm_client.trade_runtime import production_allowed

    assert production_allowed(mode, RiskPolicy(production_enabled=enabled)) is allowed  # type: ignore[arg-type]


def test_trading_works_with_the_kalshiterm_server_unreachable_and_never_touches_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Phase 6 deliverable: every trading command runs with the server gone. The server client,
    the profile store and the network blow up if anything so much as reaches for them."""
    import socket

    from kalshiterm_client.client import ServerClient
    from kalshiterm_client.profiles import ProfileStore

    rest = FakeRest()
    rest.balance_value = balance("10.3490", "0.0000", "0.0000", "0.0000")
    rest.orders = [order("o-1", "KXA-1"), order("o-2", "KXA-1")]
    rest.fills = [fill("t-1", 100)]
    rest.positions = [position("KXA-1", "2.00", "0.80")]
    h = install(monkeypatch, tmp_path, rest=rest)

    def touched(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("trading reached for the KalshiTerm server")

    monkeypatch.setattr(ServerClient, "for_profile", classmethod(touched))
    monkeypatch.setattr(ProfileStore, "resolve", touched)
    monkeypatch.setattr(ProfileStore, "__init__", touched)
    monkeypatch.setattr(socket.socket, "connect", touched)

    commands = [
        ("balance",),
        ("positions",),
        ("orders",),
        ("fills",),
        ("buy", "KXA-1", "1", "0.30", "--dry-run"),
        ("buy", "KXA-1", "1", "0.30", "--yes"),
        ("sell", "KXA-1", "1", "0.30", "--yes"),
        ("amend", "o-1", "--price", "0.35", "--yes"),
        ("cancel", "o-1"),
        ("kill",),
        ("resume", "--yes"),
    ]
    for args in commands:
        result = run(*args)
        assert result.exit_code == 0, (args, result.output)
    assert {"create", "amend", "cancel", "cancel_all"} <= set(h.trading.names())
