"""Assembling the trading machinery from settings, for the CLI (and later the local backend).

Trading talks to Kalshi directly and never to the KalshiTerm server, so nothing here needs a
profile or a server token. Three shapes, from least to most privileged:

* ``read``: only a REST client (balance, positions, orders, fills). It signs with the data key if
  one is configured, else the trading key.
* ``preview``: for ``--dry-run``. Everything is read (tracker started) but the trading client is a
  ``PreviewTrading`` that refuses every write, so a dry run cannot send anything, and it works
  before production trading is enabled.
* ``exit``: REST + trading client, with the tracker built but **not started**, so the kill switch
  never waits on a WebSocket or a full account read.
* ``trade``: everything, with the tracker started (subscribed and fully read) before it is handed
  out, so the risk layer always sees a synced snapshot.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal

from kalshi_core.auth import AuthError, KalshiSigner
from kalshi_core.config import KalshiSettings
from kalshi_core.rest import KalshiRestClient
from kalshi_core.trading import KalshiTradingClient, TradingRefused
from kalshi_core.ws import KalshiWebSocket

from kalshiterm_client.account import AccountTracker
from kalshiterm_client.orders import OrderService
from kalshiterm_client.risk import RiskPolicy
from kalshiterm_client.risk_config import load_policy
from kalshiterm_client.trading_state import TradingState

Mode = Literal["read", "preview", "exit", "trade"]


class PreviewTrading:
    """Stands in for the trading client in a dry run: any attempt to send raises."""

    async def _refuse(self, *args: Any, **kwargs: Any) -> Any:
        raise TradingRefused("dry run: nothing can be sent")

    create_order = amend_order = cancel_order = cancel_orders = cancel_all = transfer = _refuse


def production_allowed(mode: Mode, policy: RiskPolicy) -> bool:
    """May the trading client act on production? Getting out (cancels, kill) always may; sending
    orders and moving money needs ``production_enabled`` in ``risk.toml``."""
    return mode == "exit" or policy.production_enabled


@dataclass
class Runtime:
    settings: KalshiSettings
    policy: RiskPolicy
    state: TradingState
    rest: KalshiRestClient
    service: OrderService | None = None
    tracker: AccountTracker | None = None
    trading: KalshiTradingClient | None = None

    @property
    def production(self) -> bool:
        return self.settings.is_production


def read_signer(settings: KalshiSettings) -> KalshiSigner:
    """The key for read-only views: the data key if there is one, else the trading key."""
    if settings.key_id and settings.private_key_path is not None:
        return KalshiSigner.from_settings(settings)
    try:
        return KalshiSigner.for_trading(settings)
    except AuthError as exc:
        raise AuthError(
            "no key to read the account with: set KALSHI_KEY_ID and KALSHI_PRIVATE_KEY_PATH "
            f"(data key), or the trading key ({exc})"
        ) from exc


@asynccontextmanager
async def open_runtime(
    mode: Mode, settings: KalshiSettings | None = None
) -> AsyncIterator[Runtime]:
    """Open what a command needs for ``mode`` and close it again (an async context manager).

    ``read`` is the read-only key and a REST client, nothing else. ``preview`` tracks the account
    but swaps the trading client for one that refuses every send (a dry run). ``exit`` builds the
    order service with the real trading client but does not start the account stream. ``trade``
    starts the stream and sends for real. Production orders need ``production_enabled`` in
    ``risk.toml`` (``production_allowed``).
    """
    settings = settings or KalshiSettings()
    policy = load_policy()
    state = TradingState.load()
    if mode == "read":
        async with KalshiRestClient(settings, signer=read_signer(settings)) as rest:
            yield Runtime(settings, policy, state, rest)
        return

    signer = KalshiSigner.for_trading(settings)
    if mode == "preview":
        async with (
            KalshiRestClient(settings, signer=signer) as rest,
            KalshiWebSocket(settings, signer) as ws,
        ):
            tracker = AccountTracker(rest, ws, state)
            await tracker.start()
            try:
                service = OrderService(
                    rest, PreviewTrading(), tracker, policy, production=settings.is_production
                )
                yield Runtime(settings, policy, state, rest, service, tracker, None)
            finally:
                await tracker.stop()
        return
    trading = KalshiTradingClient(
        settings, signer, allow_production=production_allowed(mode, policy)
    )
    async with KalshiRestClient(settings, signer=signer) as rest, trading:
        if mode == "exit":
            ws = KalshiWebSocket(settings, signer)
            tracker = AccountTracker(rest, ws, state)
            service = OrderService(
                rest, trading, tracker, policy, production=settings.is_production
            )
            yield Runtime(settings, policy, state, rest, service, tracker, trading)
            return
        async with KalshiWebSocket(settings, signer) as ws:
            tracker = AccountTracker(rest, ws, state)
            await tracker.start()
            try:
                service = OrderService(
                    rest, trading, tracker, policy, production=settings.is_production
                )
                yield Runtime(settings, policy, state, rest, service, tracker, trading)
            finally:
                await tracker.stop()
