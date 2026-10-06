"""Client-side rate limiting that mirrors Kalshi's token-bucket model.

Kalshi budgets *tokens* per second (Basic tier: 200 read, 100 write); most requests cost 10.
Read and write budgets are independent buckets holding at most one second of budget (Basic).
Exceeding the budget returns a bare HTTP 429 with no Retry-After, so we pace ourselves.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable

from kalshi_core.config import KalshiSettings

DEFAULT_REQUEST_COST = 10
_EPSILON = 1e-9  # float slack: a computed sleep can leave the bucket ~1e-15 short


class TokenBucket:
    """Async token bucket. Starts full; waiters are served first-come, first-served."""

    def __init__(
        self,
        rate: float,
        capacity: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if rate <= 0 or capacity <= 0:
            raise ValueError("rate and capacity must be positive")
        self._rate = rate
        self._capacity = capacity
        self._clock = clock
        self._sleep = sleep
        self._tokens = capacity
        self._updated = clock()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = self._clock()
        self._tokens = min(self._capacity, self._tokens + (now - self._updated) * self._rate)
        self._updated = now

    async def acquire(self, cost: float = DEFAULT_REQUEST_COST) -> None:
        """Wait until ``cost`` tokens are available, then spend them."""
        if cost <= 0 or cost > self._capacity:
            raise ValueError(f"cost must be in (0, {self._capacity}]")
        async with self._lock:
            self._refill()
            while self._tokens < cost - _EPSILON:
                await self._sleep((cost - self._tokens) / self._rate)
                self._refill()
            self._tokens = max(0.0, self._tokens - cost)


def read_bucket(settings: KalshiSettings) -> TokenBucket:
    """Bucket for read requests: tier budget scaled by the safety margin."""
    budget = settings.read_tokens_per_second * settings.rate_limit_margin
    return TokenBucket(rate=budget, capacity=budget)
