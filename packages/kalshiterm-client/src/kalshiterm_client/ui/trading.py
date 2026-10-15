"""Trading inside the browser UI: when it is on, and every order that goes through it.

The session starts **read-only**: no trading key is loaded, no trading client exists, no private
WebSocket is open. Switching trading on is a deliberate act:

* ``risk.toml`` must *permit* trading here (``production_enabled`` for the production account).
  The page can never write that file, so it can ask for trading but not grant itself the right;
* the user types a phrase (``ENABLE PRODUCTION TRADING``) which the backend checks;
* only then is the key loaded and the account read (``open_runtime("trade")``).

Switching off drops the key and everything built on it. Trading also **lapses by itself**: after
``ui_idle_minutes`` without a deliberate action, after ``ui_disconnect_seconds`` with no browser
connected, and at shutdown.

An order is two steps. ``prepare`` runs the whole risk check and returns what would happen plus a
one-time id (valid 60 s, bound to exactly that order). ``execute`` takes the id, and in production
the order typed out again, and sends it through ``OrderService`` (which re-checks with fresh data).
A stray request cannot place an order: it has no id, or the id is spent, or the phrase is wrong.

The kill switch works in either mode (in read-only it opens a minimal runtime just to cancel).
"""

import asyncio
import contextlib
import logging
import secrets
import time
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
from kalshi_core.auth import AuthError
from kalshi_core.config import KalshiSettings
from kalshi_core.fixedpoint import E6
from kalshi_core.rest import KalshiAPIError
from kalshi_core.trading import TimeInForce, TradingRefused

from kalshiterm_client.orders import (
    Amendment,
    OrderRequest,
    Placement,
    Prepared,
    cancel_by_ids,
    describe,
    typed_phrase_matches,
)
from kalshiterm_client.profiles import ProfileError
from kalshiterm_client.risk import Action, Outcome, RiskPolicy
from kalshiterm_client.trade_runtime import Mode, Runtime, open_runtime
from kalshiterm_client.trading_state import TradingState, utc_day
from kalshiterm_client.ui import trading_models as tm

log = logging.getLogger(__name__)

PREPARE_TTL_SECONDS = 60
WATCH_INTERVAL_SECONDS = 5.0
ACCOUNT_PUSH_SECONDS = 0.5
RECENT_FILLS_SHOWN = 50
PHRASE_FORMAT = "TICKER buy|sell yes|no COUNT PRICE"

RuntimeFactory = Callable[[Mode], AbstractAsyncContextManager[Runtime]]


class TradingOff(Exception):
    """An order endpoint was used while trading is switched off."""


class SwitchRefused(Exception):
    """Trading could not be switched on (or the kill switch could not run); ``code`` says why."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


class PreparedGone(Exception):
    """The prepared order is unknown, spent or expired: prepare it again."""


class WrongOrderPhrase(Exception):
    """In production the order must be typed out; what was typed does not match."""


def money(value_e6: int) -> str:
    """Dollars with six places, exactly."""
    sign = "-" if value_e6 < 0 else ""
    whole, fraction = divmod(abs(value_e6), E6)
    return f"{sign}{whole}.{fraction:06d}"


def contracts(value_e2: int) -> str:
    sign = "-" if value_e2 < 0 else ""
    whole, fraction = divmod(abs(value_e2), 100)
    return f"{sign}{whole}.{fraction:02d}"


def limits_of(policy: RiskPolicy) -> tm.Limits:
    return tm.Limits(
        max_order_count=contracts(policy.max_order_count_e2),
        max_position=contracts(policy.max_position_e2),
        max_order_notional=money(policy.max_order_notional_e6),
        max_open_exposure=money(policy.max_open_exposure_e6),
        max_daily_loss=money(policy.max_daily_loss_e6),
        max_open_orders=policy.max_open_orders,
        max_transfer_per_order=money(policy.max_transfer_per_order_e6),
        max_transfer_per_day=money(policy.max_transfer_per_day_e6),
    )


@dataclass
class Stored:
    prepared: Prepared
    expires: float
    phrase_required: bool


class TradingSession:
    def __init__(
        self,
        *,
        settings: KalshiSettings,
        runtime_factory: RuntimeFactory = open_runtime,
        policy_loader: Callable[[], RiskPolicy],
        state_loader: Callable[[], TradingState] = TradingState.load,
        connection_count: Callable[[], int] = lambda: 1,
        announce_mode: Callable[[dict[str, Any]], None] = lambda message: None,
        announce_account: Callable[[dict[str, Any] | None], None] = lambda view: None,
        clock: Callable[[], float] = time.monotonic,
        watch_interval: float = WATCH_INTERVAL_SECONDS,
        account_push: float = ACCOUNT_PUSH_SECONDS,
    ) -> None:
        self._settings = settings
        self._factory = runtime_factory
        self._policy_loader = policy_loader
        self._state_loader = state_loader
        self._connection_count = connection_count
        self._announce_mode = announce_mode
        self._announce_account = announce_account
        self._clock = clock
        self._watch_interval = watch_interval
        self._account_push = account_push
        self._stack: AsyncExitStack | None = None
        self._runtime: Runtime | None = None
        self._prepared: dict[str, Stored] = {}
        self._last_activity = clock()
        self._no_browser_since: float | None = None
        self._watcher: asyncio.Task[None] | None = None
        self._flush: asyncio.TimerHandle | None = None
        self._switch_lock = asyncio.Lock()
        self._order_lock = asyncio.Lock()

    def bind(
        self,
        *,
        announce_mode: Callable[[dict[str, Any]], None],
        announce_account: Callable[[dict[str, Any] | None], None],
        connection_count: Callable[[], int],
    ) -> None:
        """Connect the session to the app's live feed (done once when the app is built)."""
        self._announce_mode = announce_mode
        self._announce_account = announce_account
        self._connection_count = connection_count

    # ------------------------------------------------------------------ the mode

    @property
    def production(self) -> bool:
        return self._settings.is_production

    @property
    def mode(self) -> str:
        return "trading" if self._runtime is not None else "read-only"

    @property
    def enable_phrase(self) -> str:
        return f"ENABLE {'PRODUCTION' if self.production else 'DEMO'} TRADING"

    def touch(self) -> None:
        """A deliberate action by the user: the idle clock starts again."""
        self._last_activity = self._clock()

    def info(self) -> tm.ModeInfo:
        policy = self._policy_loader()
        state = self._runtime.state if self._runtime else self._state_loader()
        today = utc_day(datetime.now(UTC))
        permitted = policy.production_enabled or not self.production
        idle = None
        if self._runtime is not None:
            remaining = policy.ui_idle_minutes * 60 - (self._clock() - self._last_activity)
            idle = max(0, int(remaining))
        return tm.ModeInfo(
            mode="trading" if self._runtime is not None else "read-only",
            environment=self._settings.env.value,
            production=self.production,
            permitted=permitted,
            permission_help=None
            if permitted
            else "set production_enabled = true under [trading] in risk.toml, then try again",
            enable_phrase=self.enable_phrase,
            halted=state.halted,
            halt_reason=state.halt_reason,
            idle_minutes=policy.ui_idle_minutes,
            disconnect_seconds=policy.ui_disconnect_seconds,
            seconds_until_idle=idle,
            limits=limits_of(policy),
            transferred_today=money(state.transferred_e6 if state.day == today else 0),
            day=today,
        )

    async def enable(self, phrase: str) -> tm.ModeInfo:
        async with self._switch_lock:
            if self._runtime is not None:
                return self.info()
            try:
                policy = self._policy_loader()
                self._state_loader()  # a damaged state file is an error now, not mid-order
            except ProfileError as exc:
                raise SwitchRefused(409, "config_problem", str(exc)) from exc
            if self.production and not policy.production_enabled:
                raise SwitchRefused(
                    403,
                    "production_not_permitted",
                    "risk.toml does not permit production trading: set production_enabled = true "
                    "under [trading] there (this page cannot change that file)",
                )
            if phrase.strip() != self.enable_phrase:
                raise SwitchRefused(422, "wrong_phrase", f"type exactly: {self.enable_phrase}")
            stack = AsyncExitStack()
            try:
                runtime = await stack.enter_async_context(self._factory("trade"))
            except AuthError as exc:
                raise SwitchRefused(409, "no_trading_key", str(exc)) from exc
            except ProfileError as exc:
                raise SwitchRefused(409, "config_problem", str(exc)) from exc
            except TradingRefused as exc:
                raise SwitchRefused(403, "trading_refused", str(exc)) from exc
            except (httpx.HTTPError, KalshiAPIError) as exc:
                raise SwitchRefused(
                    502, "kalshi_unreachable", f"could not reach Kalshi: {type(exc).__name__}"
                ) from exc
            assert runtime.service is not None and runtime.tracker is not None
            self._stack, self._runtime = stack, runtime
            runtime.tracker.on_change = self._on_change
            runtime.service.start_guard()  # the daily-loss kill switch, while trading is on
            self._last_activity = self._clock()
            self._no_browser_since = None
            self._watcher = asyncio.create_task(self._watch())
            self._announce_mode({"type": "mode", "mode": "trading", "reason": "user"})
            self._on_change()
            return self.info()

    async def disable(
        self, *, cancel_open_orders: bool = False, reason: str = "user"
    ) -> tm.ModeChange:
        async with self._switch_lock:
            runtime = self._runtime
            if runtime is None:
                return tm.ModeChange(mode="read-only")
            watcher, self._watcher = self._watcher, None
            if watcher is not None and watcher is not asyncio.current_task():
                watcher.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await watcher
            result = tm.ModeChange(mode="read-only")
            async with self._order_lock:  # an order in flight finishes first
                assert runtime.service is not None
                if cancel_open_orders:
                    try:
                        report = await runtime.service.cancel_everything()
                    except Exception as exc:
                        result.problems.append(f"cancelling: {type(exc).__name__}")
                    else:
                        result.cancelled = report.cancelled
                        result.problems += report.errors
                        result.problems += [f"still resting: {o}" for o in report.remaining]
                await runtime.service.stop_guard()
                if runtime.tracker is not None:
                    runtime.tracker.on_change = None
                self._prepared.clear()
                if self._flush is not None:
                    self._flush.cancel()
                    self._flush = None
                stack, self._stack, self._runtime = self._stack, None, None
                if stack is not None:
                    await stack.aclose()
            self._announce_mode({"type": "mode", "mode": "read-only", "reason": reason})
            self._announce_account(None)
            return result

    async def close(self) -> None:
        await self.disable(reason="shutdown")

    async def _watch(self) -> None:
        """Switch trading off by itself when nobody is using it."""
        while True:
            await asyncio.sleep(self._watch_interval)
            try:
                policy = self._policy_loader()
            except ProfileError:
                continue  # keep the last good timers rather than leave trading on or crash
            now = self._clock()
            if now - self._last_activity >= policy.ui_idle_minutes * 60:
                await self.disable(reason="idle")
                return
            if self._connection_count() == 0:
                self._no_browser_since = self._no_browser_since or now
                if now - self._no_browser_since >= policy.ui_disconnect_seconds:
                    await self.disable(reason="no browser")
                    return
            else:
                self._no_browser_since = None

    # ------------------------------------------------------------------ the account

    def _require(self) -> Runtime:
        if self._runtime is None:
            raise TradingOff
        return self._runtime

    def account(self) -> tm.AccountView:
        """The account page of the trading UI, formatted for display.

        Cash and free cash per shard, the non-zero positions, the resting orders and the most recent
        fills, plus today's realized loss, transfers and exposure and whether trading is halted.
        ``synced`` is false when the account stream is down or there is no snapshot yet. Needs
        trading to be switched on.
        """
        runtime = self._require()
        assert runtime.tracker is not None
        tracker, state = runtime.tracker, runtime.state
        snap = tracker.snapshot()
        free = {i: s.balance_e6 for i, s in snap.shards.items()} if snap else {}
        today = utc_day(datetime.now(UTC))
        shards = [
            tm.ShardView(
                index=index,
                cash=money(cash),
                free=money(free.get(index, 0)),
                trading=tracker.shard_flags.get(index, (False, False))[0],
                transfers=tracker.shard_flags.get(index, (False, False))[1],
            )
            for index, cash in sorted(tracker.balances_e6.items())
        ]
        positions = [
            tm.PositionView(
                ticker=ticker,
                position=contracts(p.position_e2),
                exposure=money(p.exposure_e6),
                realized=money(p.realized_e6),
            )
            for ticker, p in sorted(tracker.positions.items())
            if p.position_e2 != 0
        ]
        orders = [
            tm.OrderView(
                order_id=o.order_id,
                client_order_id=o.client_order_id,
                ticker=o.ticker,
                shard=o.exchange_index,
                book_side=o.book_side,
                yes_price=money(o.yes_price_e6),
                no_price=money(E6 - o.yes_price_e6),
                filled=contracts(o.fill_e2),
                remaining=contracts(o.remaining_e2),
                total=contracts(o.total_e2),
                status=o.status,
            )
            for o in sorted(tracker.resting_orders(), key=lambda o: o.order_id)
        ]
        fills = [
            tm.FillView(
                trade_id=f.trade_id,
                order_id=f.order_id,
                ticker=f.market_ticker,
                book_side=f.book_side,
                yes_price=money(int(f.yes_price_dollars * E6)),
                count=contracts(int(f.count_fp * 100)),
                fee=money(int(f.fee_cost * E6)),
                is_taker=f.is_taker,
                time_ms=f.ts_ms,
            )
            for f in list(tracker.fills)[-RECENT_FILLS_SHOWN:][::-1]
        ]
        return tm.AccountView(
            as_of=datetime.fromtimestamp(tracker.balance_at, UTC).isoformat()
            if tracker.balance_at is not None
            else None,
            synced=bool(snap and snap.stream_ok),
            halted=state.halted,
            halt_reason=state.halt_reason,
            shards=shards,
            positions=positions,
            orders=orders,
            fills=fills,
            realized_today=money(snap.realized_pnl_today_e6 if snap else 0),
            transferred_today=money(state.transferred_e6 if state.day == today else 0),
            exposure=money(snap.exposure_e6 if snap else 0),
            open_orders=len(orders),
            day=today,
        )

    def _on_change(self) -> None:
        """The tracker saw something change: push the new view, at most every half second."""
        if self._flush is not None or self._runtime is None:
            return
        with contextlib.suppress(RuntimeError):  # no running loop (a test calling it directly)
            self._flush = asyncio.get_running_loop().call_later(self._account_push, self._push)

    def _push(self) -> None:
        self._flush = None
        if self._runtime is None:
            return
        try:
            self._announce_account(self.account().model_dump(mode="json"))
        except Exception:
            log.exception("could not build the account view")

    # ------------------------------------------------------------------ orders

    def _view(self, stored_id: str | None, prepared: Prepared, kind: str) -> tm.PreparedView:
        verdict = prepared.verdict
        required = self.production
        return tm.PreparedView(
            prepare_id=stored_id,
            kind="amend" if kind == "amend" else "order",
            expires_in_seconds=PREPARE_TTL_SECONDS if stored_id else 0,
            lines=describe(prepared),
            approved=verdict.ok,
            refusals=[
                tm.RefusalView(code=r.code.value, message=r.message) for r in verdict.refusals
            ],
            shard=prepared.market.exchange_index,
            worst_case_cost=None
            if verdict.worst_case_cost_e6 is None
            else money(verdict.worst_case_cost_e6),
            fee=None if verdict.fee_e6 is None else money(verdict.fee_e6),
            funding=[
                tm.TransferView(
                    source=t.source, destination=t.destination, amount=money(t.amount_e6)
                )
                for t in verdict.funding
            ],
            requires_phrase=required and verdict.ok,
            phrase_format=PHRASE_FORMAT if required and verdict.ok else None,
        )

    def _remember(self, prepared: Prepared, kind: str) -> tm.PreparedView:
        self._purge()
        if not prepared.verdict.ok:
            return self._view(None, prepared, kind)  # nothing to execute
        prepare_id = secrets.token_urlsafe(16)
        self._prepared[prepare_id] = Stored(
            prepared, self._clock() + PREPARE_TTL_SECONDS, self.production
        )
        return self._view(prepare_id, prepared, kind)

    def _purge(self) -> None:
        now = self._clock()
        for key in [k for k, v in self._prepared.items() if v.expires <= now]:
            del self._prepared[key]

    async def prepare(self, body: tm.PrepareRequest) -> tm.PreparedView:
        runtime = self._require()
        assert runtime.service is not None
        self.touch()
        request = OrderRequest(
            body.ticker,
            Action(body.action),
            Outcome(body.outcome),
            body.count_value(),
            body.price_value(),
            TimeInForce(body.time_in_force),
            body.post_only,
        )
        return self._remember(await runtime.service.prepare(request), "order")

    async def prepare_amend(self, order_id: str, body: tm.AmendPrepareRequest) -> tm.PreparedView:
        runtime = self._require()
        assert runtime.service is not None
        self.touch()
        prepared = await runtime.service.prepare_amend(
            order_id,
            price=None if body.price is None else Decimal(body.price),
            count=None if body.count is None else Decimal(body.count),
        )
        return self._remember(prepared, "amend")

    async def execute(self, body: tm.ExecuteRequest) -> tm.ExecutedView:
        runtime = self._require()
        assert runtime.service is not None
        self.touch()
        self._purge()
        stored = self._prepared.get(body.prepare_id)
        if stored is None:
            raise PreparedGone
        if stored.phrase_required and not typed_phrase_matches(
            body.phrase or "", stored.prepared.request
        ):
            raise WrongOrderPhrase  # not consumed: the user may type it again within the minute
        del self._prepared[body.prepare_id]  # single use, even if the order then fails
        prepared = stored.prepared
        async with self._order_lock:
            if prepared.replaces is not None:
                amended = await runtime.service.execute_amend(prepared)
                return self._amended(amended)
            return self._placed(await runtime.service.execute(prepared))

    @staticmethod
    def _transfers(transfers: Any) -> list[tm.TransferView]:
        return [
            tm.TransferView(source=t.source, destination=t.destination, amount=money(t.amount_e6))
            for t in transfers
        ]

    def _placed(self, placement: Placement) -> tm.ExecutedView:
        created = placement.created
        return tm.ExecutedView(
            kind="order",
            order_id=created.order_id,
            client_order_id=placement.client_order_id,
            filled=str(created.fill_count),
            remaining=str(created.remaining_count),
            recovered=created.recovered,
            transfers=self._transfers(placement.transfers),
            lines=[f"order {created.order_id} placed"],
        )

    def _amended(self, amendment: Amendment) -> tm.ExecutedView:
        result = amendment.result
        return tm.ExecutedView(
            kind="amend",
            order_id=amendment.order_id,
            filled=None if result.fill_count is None else str(result.fill_count),
            remaining=None if result.remaining_count is None else str(result.remaining_count),
            transfers=self._transfers(amendment.transfers),
            lines=[f"order {amendment.order_id} amended"],
        )

    async def cancel(self, order_id: str) -> tm.CancelView:
        runtime = self._require()
        assert runtime.trading is not None
        self.touch()
        async with self._order_lock:
            (outcome,) = await cancel_by_ids(runtime.rest, runtime.trading, [order_id])
        return tm.CancelView(order_id=outcome.order_id, ok=outcome.ok, message=outcome.message)

    # ------------------------------------------------------------------ getting out

    async def kill(self) -> tm.KillView:
        """Halt and cancel everything; works with trading off too (a minimal runtime is opened)."""
        self.touch()
        if self._runtime is not None:
            assert self._runtime.service is not None
            report = await self._runtime.service.kill("kill switch (browser UI)")
            self._on_change()
        else:
            try:
                async with self._factory("exit") as runtime:
                    assert runtime.service is not None
                    report = await runtime.service.kill("kill switch (browser UI)")
            except AuthError as exc:
                raise SwitchRefused(409, "no_trading_key", str(exc)) from exc
            except ProfileError as exc:
                raise SwitchRefused(409, "config_problem", str(exc)) from exc
            except (httpx.HTTPError, KalshiAPIError) as exc:
                raise SwitchRefused(
                    502, "kalshi_unreachable", f"could not reach Kalshi: {type(exc).__name__}"
                ) from exc
        return tm.KillView(
            cancelled=report.cancelled,
            remaining=report.remaining,
            errors=report.errors,
            clean=report.clean,
            halted=True,
        )

    def resume(self) -> tm.Resumed:
        runtime = self._require()
        self.touch()
        runtime.state.clear_halt()
        self._on_change()
        return tm.Resumed(halted=False)
