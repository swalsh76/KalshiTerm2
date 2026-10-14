"""Shared setup for the UI trading tests: a ``TradingSession`` on the scripted Kalshi."""

import asyncio
from pathlib import Path
from typing import Any

from kalshi_core.config import KalshiSettings
from kalshiterm_client.risk import RiskPolicy
from kalshiterm_client.trading_state import TradingState
from kalshiterm_client.ui import trading_models as tm
from kalshiterm_client.ui.trading import TradingSession
from trading_fakes import FakeRest, FakeTrading, balance, fake_runtime_factory

PHRASE_PROD = "ENABLE PRODUCTION TRADING"
PHRASE_DEMO = "ENABLE DEMO TRADING"


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class Harness:
    def __init__(
        self, tmp_path: Path, *, production: bool, policy: RiskPolicy, rest: FakeRest | None
    ):
        self.rest = rest or FakeRest()
        if rest is None:
            self.rest.balance_value = balance("10.3490", "0.0000", "0.0000", "0.0000")
        self.state = TradingState(tmp_path / "state.json")
        self.trading = FakeTrading(self.rest, self.state)
        self.settings = KalshiSettings(env="production" if production else "demo")  # type: ignore[arg-type]
        self.policy = policy
        self.modes: list[str] = []
        self.sockets: list[Any] = []
        self.clock = Clock()
        self.mode_messages: list[dict[str, Any]] = []
        self.account_messages: list[dict[str, Any] | None] = []
        self.connections = 1
        self.fail: Exception | None = None

        def factory(mode: str) -> Any:
            return fake_runtime_factory(
                self.rest, self.trading, self.state, self.settings, self.policy,
                modes=self.modes, fail=self.fail, sockets=self.sockets,
            )(mode)  # fmt: skip

        self.session = TradingSession(
            settings=self.settings,
            runtime_factory=factory,
            policy_loader=lambda: self.policy,
            state_loader=lambda: self.state,
            connection_count=lambda: self.connections,
            announce_mode=self.mode_messages.append,
            announce_account=self.account_messages.append,
            clock=self.clock,
            watch_interval=0.01,
            account_push=0.01,
        )


def make(
    tmp_path: Path,
    *,
    production: bool = True,
    policy: RiskPolicy | None = None,
    rest: FakeRest | None = None,
) -> Harness:
    return Harness(
        tmp_path,
        production=production,
        policy=policy or RiskPolicy(production_enabled=True),
        rest=rest,
    )


def buy_request(**kw: Any) -> tm.PrepareRequest:
    base: dict[str, Any] = {
        "ticker": "KXA-1",
        "action": "buy",
        "outcome": "yes",
        "count": "2",
        "price": "0.40",
    }
    return tm.PrepareRequest(**{**base, **kw})


async def on(h: Harness) -> None:
    await h.session.enable(PHRASE_PROD if h.session.production else PHRASE_DEMO)


async def settle(seconds: float = 0.1) -> None:
    await asyncio.sleep(seconds)
