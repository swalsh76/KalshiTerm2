"""The trading routes of the local backend.

Everything under ``/api/trading`` answers 403 ``trading_off`` unless the user has switched trading
on (``/api/mode/enable``). The one exception is the kill switch, which works either way. All of
these are state-changing requests, so they also pass the Origin and token checks of the security
layer; none of them can be reached by another web page.
"""

from typing import Annotated, Any

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Path, Request
from fastapi.responses import JSONResponse
from kalshi_core.rest import KalshiAPIError
from kalshi_core.trading import TradingRefused, WriteOutcomeUnknown

from kalshiterm_client.orders import (
    FundingShort,
    OrderNotFound,
    OrderRefused,
    PlanChanged,
    TransferFailed,
    TransferUnknown,
)
from kalshiterm_client.ui import trading_models as tm
from kalshiterm_client.ui.trading import (
    PreparedGone,
    SwitchRefused,
    TradingOff,
    TradingSession,
    WrongOrderPhrase,
)

OrderId = Annotated[str, Path(pattern=r"^[A-Za-z0-9._-]{1,64}$")]
router = APIRouter(prefix="/api")


def session_of(request: Request) -> TradingSession:
    session: TradingSession | None = request.app.state.trading
    if session is None:
        raise HTTPException(503, "trading is not available in this session")
    return session


Session = Annotated[TradingSession, Depends(session_of)]


# ------------------------------------------------------------------ the mode


@router.get("/mode", response_model=tm.ModeInfo)
async def mode(session: Session) -> Any:
    return session.info()


@router.post("/mode/enable", response_model=tm.ModeInfo)
async def enable(session: Session, body: tm.EnableRequest) -> Any:
    return await session.enable(body.phrase)


@router.post("/mode/disable", response_model=tm.ModeChange)
async def disable(session: Session, body: tm.DisableRequest) -> Any:
    return await session.disable(cancel_open_orders=body.cancel_open_orders)


# ------------------------------------------------------------------ the account and orders


@router.get("/trading/account", response_model=tm.AccountView)
async def account(session: Session) -> Any:
    return session.account()


@router.post("/trading/orders/prepare", response_model=tm.PreparedView)
async def prepare(session: Session, body: tm.PrepareRequest) -> Any:
    try:
        return await session.prepare(body)
    except (ValueError, TypeError) as exc:  # e.g. a price finer than Kalshi accepts
        raise HTTPException(422, str(exc)) from exc


@router.post("/trading/orders/{order_id}/amend", response_model=tm.PreparedView)
async def prepare_amend(session: Session, order_id: OrderId, body: tm.AmendPrepareRequest) -> Any:
    try:
        return await session.prepare_amend(order_id, body)
    except (ValueError, TypeError) as exc:
        raise HTTPException(422, str(exc)) from exc


@router.post("/trading/orders/execute", response_model=tm.ExecutedView)
async def execute(session: Session, body: tm.ExecuteRequest) -> Any:
    return await session.execute(body)


@router.delete("/trading/orders/{order_id}", response_model=tm.CancelView)
async def cancel(session: Session, order_id: OrderId) -> Any:
    return await session.cancel(order_id)


# ------------------------------------------------------------------ getting out


@router.post("/trading/kill", response_model=tm.KillView)
async def kill(session: Session) -> Any:
    """Halt and cancel everything. Works with trading switched off too."""
    return await session.kill()


@router.post("/trading/resume", response_model=tm.Resumed)
async def resume(session: Session, body: tm.ResumeRequest) -> Any:
    return session.resume()


# ------------------------------------------------------------------ errors


def _json(status: int, error: str, detail: str, **extra: Any) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": error, "detail": detail, **extra})


def register_trading_errors(app: FastAPI) -> None:
    """Turn the trading layer's exceptions into JSON errors the browser can act on.

    Each has a stable ``code`` and a message, and an HTTP status chosen per exception: 403 when
    trading is switched off, 404 when a prepared order has expired, 409 when the plan changed or
    funding fell short, 422 for a refused order (listing each refusal) or a wrong confirmation
    phrase, and so on for the rest below.
    """

    @app.exception_handler(TradingOff)
    async def _off(_: Request, exc: TradingOff) -> JSONResponse:
        return _json(403, "trading_off", "trading is switched off: switch it on first")

    @app.exception_handler(SwitchRefused)
    async def _switch(_: Request, exc: SwitchRefused) -> JSONResponse:
        return _json(exc.status, exc.code, exc.message)

    @app.exception_handler(PreparedGone)
    async def _gone(_: Request, exc: PreparedGone) -> JSONResponse:
        return _json(404, "prepare_expired", "that order is no longer prepared: prepare it again")

    @app.exception_handler(WrongOrderPhrase)
    async def _phrase(_: Request, exc: WrongOrderPhrase) -> JSONResponse:
        return _json(422, "wrong_order_phrase", "that is not the order shown: type it as shown")

    @app.exception_handler(OrderRefused)
    async def _refused(_: Request, exc: OrderRefused) -> JSONResponse:
        refusals = [{"code": r.code.value, "message": r.message} for r in exc.verdict.refusals]
        return _json(422, "order_refused", str(exc), refusals=refusals)

    @app.exception_handler(PlanChanged)
    async def _changed(_: Request, exc: PlanChanged) -> JSONResponse:
        return _json(409, "plan_changed", str(exc))

    @app.exception_handler(FundingShort)
    async def _short(_: Request, exc: FundingShort) -> JSONResponse:
        return _json(409, "funding_short", str(exc))

    @app.exception_handler(TransferFailed)
    async def _transfer(_: Request, exc: TransferFailed) -> JSONResponse:
        return _json(502, "transfer_failed", str(exc))

    @app.exception_handler(TransferUnknown)
    async def _transfer_unknown(_: Request, exc: TransferUnknown) -> JSONResponse:
        return _json(502, "transfer_unknown", str(exc))

    @app.exception_handler(WriteOutcomeUnknown)
    async def _unknown(_: Request, exc: WriteOutcomeUnknown) -> JSONResponse:
        return _json(
            502,
            "outcome_unknown",
            f"{exc}. Look in your orders and fills before trying again.",
            kind=exc.kind,
            ref=exc.ref,
        )

    @app.exception_handler(OrderNotFound)
    async def _no_order(_: Request, exc: OrderNotFound) -> JSONResponse:
        return _json(404, "order_not_found", str(exc))

    @app.exception_handler(TradingRefused)
    async def _trading_refused(_: Request, exc: TradingRefused) -> JSONResponse:
        return _json(403, "trading_refused", str(exc))

    @app.exception_handler(KalshiAPIError)
    async def _kalshi(_: Request, exc: KalshiAPIError) -> JSONResponse:
        return _json(502, "kalshi_error", f"Kalshi said {exc.status}", kalshi_status=exc.status)
