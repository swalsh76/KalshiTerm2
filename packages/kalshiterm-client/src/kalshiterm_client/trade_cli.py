"""``kterm buy / sell / amend / cancel / kill / resume`` and the account views.

Trading talks to Kalshi directly; none of this touches the KalshiTerm server. Every order goes
through ``OrderService`` (so through the risk layer); what would happen is always printed first,
including any shard funding and every reason for a refusal.

* ``--dry-run`` stops after printing; nothing is sent.
* In the demo environment an order asks ``y/N`` (``--yes`` skips it).
* In **production** the order is only sent after you type the order out (ticker, buy or sell, yes
  or no, count, price), and ``--yes`` does not skip that. If input is not available the order is
  not sent.
* Cancels, ``kill`` and ``resume`` are not risk-checked: nothing may stand between you and getting
  out.

Exit status: 0 done, 1 refused or failed (nothing happened that you did not see), 2 either a
usage mistake or, with an ``UNKNOWN:`` message, a write whose outcome is unknown (the message
names the id to look for).
"""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Annotated, Any, TypeVar

import httpx
import typer
from kalshi_core.auth import AuthError
from kalshi_core.rest import KalshiAPIError
from kalshi_core.trading import TimeInForce, TradingRefused, WriteOutcomeUnknown
from rich.measure import Measurement

from kalshiterm_client import trade_views
from kalshiterm_client.cli_common import emit_json, fail, make_console
from kalshiterm_client.orders import (
    FundingShort,
    OrderRefused,
    OrderRequest,
    PlanChanged,
    Prepared,
    TransferFailed,
    TransferUnknown,
    cancel_by_ids,
    describe,
)
from kalshiterm_client.profiles import ProfileError
from kalshiterm_client.risk import Action, Outcome, Verdict
from kalshiterm_client.trade_runtime import Mode, Runtime, open_runtime
from kalshiterm_client.trading_state import TradingState

T = TypeVar("T")
JsonOption = Annotated[bool, typer.Option("--json", help="Machine-readable output.")]
DryRunOption = Annotated[
    bool, typer.Option("--dry-run", help="Show what would happen; send nothing.")
]
YesOption = Annotated[
    bool, typer.Option("--yes", help="Skip the y/N question (never the production typed check).")
]

# Tests replace this to run the commands against fakes, with no network.
RuntimeFactory = Callable[[Mode], AbstractAsyncContextManager[Runtime]]
runtime_factory: RuntimeFactory = open_runtime


class Side(StrEnum):
    YES = "yes"
    NO = "no"


def run_trading(mode: Mode, work: Callable[[Runtime], Awaitable[T]]) -> T:
    """Open the runtime for ``mode``, run ``work``, and turn every problem a person can act on
    into one clear line and an exit status."""

    async def go() -> T:
        async with runtime_factory(mode) as runtime:
            return await work(runtime)

    try:
        return asyncio.run(go())
    except typer.Exit:
        raise
    except OrderRefused as exc:
        raise fail(str(exc)) from exc
    except PlanChanged:
        raise fail(
            "the cost or funding changed while you were deciding; run the command again"
        ) from None
    except TransferFailed as exc:
        raise fail(f"{exc}; the order was not placed") from exc
    except TransferUnknown as exc:
        raise fail(str(exc)) from exc
    except FundingShort as exc:
        raise fail(str(exc)) from exc
    except WriteOutcomeUnknown as exc:
        typer.echo(
            f"UNKNOWN: {exc}. Look for {exc.ref} in `kterm orders --status all` and "
            "`kterm fills` before trying again; do not repeat the command blindly.",
            err=True,
        )
        raise typer.Exit(code=2) from exc
    except TradingRefused as exc:
        raise fail(
            f"{exc} (production orders need KALSHI_ENV=production and "
            "production_enabled = true in risk.toml)"
        ) from exc
    except KalshiAPIError as exc:
        raise fail(f"Kalshi said {exc.status}: {exc.body[:300]}") from exc
    except httpx.TransportError as exc:
        raise fail(f"cannot reach Kalshi: {type(exc).__name__}") from exc
    except (AuthError, ProfileError) as exc:
        raise fail(str(exc)) from exc


def show(renderable: Any) -> None:
    """Print a table complete: when piped, widen the console to what the table needs (a terminal
    keeps its own width) so ids and tickers are never cut."""
    console = make_console()
    if not console.is_terminal:
        wide = console.options.update_width(10_000)  # measure() is capped at the console width
        console.width = max(console.width, Measurement.get(console, wide, renderable).maximum)
    console.print(renderable)


def decimal_arg(name: str, text: str) -> Decimal:
    try:
        value = Decimal(text)
    except InvalidOperation:
        raise fail(f"{name}: {text!r} is not a number") from None
    if not value.is_finite() or value <= 0:
        raise fail(f"{name}: must be a positive number, not {text!r}")
    return value


def banner(runtime: Runtime) -> None:
    if runtime.production:
        typer.echo("PRODUCTION: real money on your Kalshi account", err=True)
    else:
        typer.echo(f"environment: {runtime.settings.env.value}", err=True)


def verdict_data(prepared: Prepared) -> dict[str, Any]:
    v: Verdict = prepared.verdict
    return {
        "approved": v.ok,
        "shard": prepared.market.exchange_index,
        "fee_e6": v.fee_e6,
        "worst_case_cost_e6": v.worst_case_cost_e6,
        "funding": [
            {"source": t.source, "destination": t.destination, "amount_e6": t.amount_e6}
            for t in v.funding
        ],
        "refusals": [{"code": r.code.value, "message": r.message} for r in v.refusals],
        "lines": describe(prepared),
    }


def typed_phrase_matches(typed: str, request: OrderRequest) -> bool:
    """``TICKER buy|sell yes|no COUNT PRICE`` with the numbers equal as numbers (0.4 == 0.40)."""
    parts = typed.split()
    if len(parts) != 5:
        return False
    ticker, action, outcome, count, price = parts
    try:
        return (
            ticker == request.ticker
            and action.lower() == request.action.value
            and outcome.lower() == request.outcome.value
            and Decimal(count) == request.count
            and Decimal(price) == request.price
        )
    except InvalidOperation:
        return False


def confirm(runtime: Runtime, prepared: Prepared, yes: bool) -> None:
    """Ask before sending. Production needs the order typed out; demo needs y/N (or --yes)."""
    if runtime.production:
        if yes:
            typer.echo("note: --yes does not skip the production check", err=True)
        typer.echo("Type the order to send it:  TICKER buy|sell yes|no COUNT PRICE", err=True)
        try:
            typed = typer.prompt("order", err=True)
        except (typer.Abort, EOFError):
            raise fail("no confirmation given; nothing was sent") from None
        if not typed_phrase_matches(typed, prepared.request):
            raise fail("that is not the order shown above; nothing was sent")
        return
    if yes:
        return
    if not typer.confirm("Send this order?", default=False, err=True):
        raise fail("not sent")


def register(app: typer.Typer) -> None:
    # ------------------------------------------------------------------ placing orders

    def place(
        action: Action,
        ticker: str,
        count: str,
        price: str,
        side: Side,
        ioc: bool,
        post_only: bool,
        dry_run: bool,
        yes: bool,
        as_json: bool,
    ) -> None:
        request = OrderRequest(
            ticker,
            action,
            Outcome(side.value),
            decimal_arg("count", count),
            decimal_arg("price", price),
            TimeInForce.IMMEDIATE_OR_CANCEL if ioc else TimeInForce.GOOD_TILL_CANCELED,
            post_only,
        )

        async def work(runtime: Runtime) -> None:
            assert runtime.service is not None
            banner(runtime)
            prepared = await runtime.service.prepare(request)
            if as_json:
                emit_json(verdict_data(prepared))
            else:
                for line in describe(prepared):
                    typer.echo(line)
            if not prepared.verdict.ok:
                raise typer.Exit(code=1)
            if dry_run:
                if not as_json:
                    typer.echo("dry run: nothing was sent")
                return
            confirm(runtime, prepared, yes)
            placement = await runtime.service.execute(prepared)
            for line in trade_views.placement_lines(placement):
                typer.echo(line)

        run_trading("preview" if dry_run else "trade", work)

    @app.command("buy")
    def buy(
        ticker: Annotated[str, typer.Argument(help="Market ticker.")],
        count: Annotated[str, typer.Argument(help="Contracts, e.g. 2 or 0.5.")],
        price: Annotated[str, typer.Argument(help="Limit price in dollars of the side you buy.")],
        side: Annotated[Side, typer.Option(help="Which side to buy.")] = Side.YES,
        ioc: Annotated[bool, typer.Option("--ioc", help="Immediate-or-cancel.")] = False,
        post_only: Annotated[bool, typer.Option("--post-only")] = False,
        dry_run: DryRunOption = False,
        yes: YesOption = False,
        as_json: JsonOption = False,
    ) -> None:
        """Buy contracts at a limit price (checked against your risk limits first)."""
        place(Action.BUY, ticker, count, price, side, ioc, post_only, dry_run, yes, as_json)

    @app.command("sell")
    def sell(
        ticker: Annotated[str, typer.Argument(help="Market ticker.")],
        count: Annotated[str, typer.Argument(help="Contracts, e.g. 2 or 0.5.")],
        price: Annotated[str, typer.Argument(help="Limit price in dollars of the side you sell.")],
        side: Annotated[Side, typer.Option(help="Which side to sell.")] = Side.YES,
        ioc: Annotated[bool, typer.Option("--ioc", help="Immediate-or-cancel.")] = False,
        post_only: Annotated[bool, typer.Option("--post-only")] = False,
        dry_run: DryRunOption = False,
        yes: YesOption = False,
        as_json: JsonOption = False,
    ) -> None:
        """Sell contracts at a limit price (checked against your risk limits first)."""
        place(Action.SELL, ticker, count, price, side, ioc, post_only, dry_run, yes, as_json)

    @app.command("amend")
    def amend(
        order_id: Annotated[str, typer.Argument(help="The resting order to change.")],
        price: Annotated[
            str | None, typer.Option(help="New YES price in dollars, as `kterm orders` shows it.")
        ] = None,
        count: Annotated[str | None, typer.Option(help="New TOTAL count.")] = None,
        dry_run: DryRunOption = False,
        yes: YesOption = False,
        as_json: JsonOption = False,
    ) -> None:
        """Change a resting order's price and/or count (checked like a new order)."""
        new_price = decimal_arg("price", price) if price is not None else None
        new_count = decimal_arg("count", count) if count is not None else None

        async def work(runtime: Runtime) -> None:
            assert runtime.service is not None
            banner(runtime)
            try:
                prepared = await runtime.service.prepare_amend(
                    order_id, price=new_price, count=new_count
                )
            except (ValueError, TypeError) as exc:
                raise fail(str(exc)) from exc
            if as_json:
                emit_json(verdict_data(prepared))
            else:
                typer.echo(f"amend order {order_id}:")
                for line in describe(prepared):
                    typer.echo(line)
            if not prepared.verdict.ok:
                raise typer.Exit(code=1)
            if dry_run:
                if not as_json:
                    typer.echo("dry run: nothing was sent")
                return
            confirm(runtime, prepared, yes)
            amendment = await runtime.service.execute_amend(prepared)
            for line in trade_views.amendment_lines(amendment):
                typer.echo(line)

        run_trading("preview" if dry_run else "trade", work)

    # ------------------------------------------------------------------ getting out

    @app.command("cancel")
    def cancel(
        order_ids: Annotated[list[str], typer.Argument(help="Order ids (see kterm orders).")],
        as_json: JsonOption = False,
    ) -> None:
        """Cancel resting orders. Never refused by the risk layer, works while halted."""

        async def work(runtime: Runtime) -> list[Any]:
            assert runtime.trading is not None
            banner(runtime)
            return await cancel_by_ids(runtime.rest, runtime.trading, order_ids)

        outcomes = run_trading("exit", work)
        if as_json:
            emit_json(
                [{"order_id": o.order_id, "ok": o.ok, "message": o.message} for o in outcomes]
            )
        else:
            for o in outcomes:
                typer.echo(f"{o.order_id}: {o.message}")
        if not all(o.ok for o in outcomes):
            raise typer.Exit(code=1)

    @app.command("kill")
    def kill(as_json: JsonOption = False) -> None:
        """Halt new orders and cancel every open order, then check until none remain."""

        async def work(runtime: Runtime) -> Any:
            assert runtime.service is not None
            banner(runtime)
            return await runtime.service.kill("kill switch (kterm kill)")

        report = run_trading("exit", work)
        if as_json:
            emit_json(
                {
                    "cancelled": report.cancelled,
                    "remaining": report.remaining,
                    "errors": report.errors,
                    "clean": report.clean,
                }
            )
        else:
            typer.echo("trading is HALTED (new orders refused until `kterm resume`)")
            typer.echo(f"cancelled {len(report.cancelled)} order(s) in the follow-up passes")
            if report.remaining:
                typer.echo(f"STILL RESTING: {', '.join(report.remaining)}", err=True)
            for error in report.errors:
                typer.echo(f"problem: {error}", err=True)
            if report.clean:
                typer.echo("no orders are resting")
        if not report.clean:
            raise typer.Exit(code=1)

    @app.command("resume")
    def resume(yes: YesOption = False) -> None:
        """Clear the halt left by the kill switch so new orders are allowed again."""
        try:
            state = TradingState.load()
        except ProfileError as exc:
            raise fail(str(exc)) from exc
        if not state.halted:
            typer.echo("trading is not halted")
            return
        typer.echo(f"trading is halted: {state.halt_reason or 'kill switch'}")
        if not yes and not typer.confirm("Allow new orders again?", default=False):
            raise fail("still halted")
        state.clear_halt()
        typer.echo("halt cleared")

    # ------------------------------------------------------------------ looking

    @app.command("balance")
    def balance(as_json: JsonOption = False) -> None:
        """Your balance on each exchange shard."""

        async def work(runtime: Runtime) -> Any:
            return await runtime.rest.balance(), runtime.state

        result, state = run_trading("read", work)
        if as_json:
            return emit_json({"balance": result, "halted": state.halted})
        show(
            trade_views.balance_view(
                result, (state.halt_reason or "kill switch") if state.halted else None
            )
        )

    @app.command("positions")
    def positions(
        all_: Annotated[
            bool, typer.Option("--all", help="Include flat and settled markets.")
        ] = False,
        as_json: JsonOption = False,
    ) -> None:
        """Your market positions."""

        async def work(runtime: Runtime) -> list[Any]:
            params = {"settlement_status": "all"} if all_ else {}
            return [p async for p in runtime.rest.iter_positions(**params)]

        found = run_trading("read", work)
        shown = found if all_ else [p for p in found if p.position_fp != 0]
        if as_json:
            return emit_json(shown)
        show(trade_views.positions_table(shown))

    @app.command("orders")
    def orders(
        status: Annotated[
            str, typer.Option(help="resting, canceled, executed, or all.")
        ] = "resting",
        limit: Annotated[int, typer.Option(min=1, max=1000, help="Most recent N.")] = 50,
        as_json: JsonOption = False,
    ) -> None:
        """Your orders (resting ones by default)."""
        if status not in ("resting", "canceled", "executed", "all"):
            raise fail("status must be resting, canceled, executed or all")

        async def work(runtime: Runtime) -> list[Any]:
            params = {} if status == "all" else {"status": status}
            return [o async for o in runtime.rest.iter_orders(**params)]

        found = run_trading("read", work)
        found.sort(key=lambda o: o.created_time.timestamp() if o.created_time else 0, reverse=True)
        shown = found[:limit]
        if as_json:
            return emit_json(shown)
        show(trade_views.orders_table(shown))
        if shown:
            typer.echo(trade_views.ORDERS_NOTE)

    @app.command("fills")
    def fills(
        limit: Annotated[int, typer.Option(min=1, max=1000, help="Most recent N.")] = 20,
        as_json: JsonOption = False,
    ) -> None:
        """Your recent fills."""

        async def work(runtime: Runtime) -> list[Any]:
            return [f async for f in runtime.rest.iter_fills()]

        found = run_trading("read", work)
        found.sort(key=lambda f: f.ts or 0, reverse=True)
        shown = found[:limit]
        if as_json:
            return emit_json(shown)
        show(trade_views.fills_table(shown))
        if shown:
            typer.echo(trade_views.ORDERS_NOTE)
