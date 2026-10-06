import asyncio
from pathlib import Path

import pytest
from kalshi_core.config import KalshiSettings
from kalshi_core.ratelimit import TokenBucket, read_bucket


class FakeTime:
    """Deterministic clock whose sleep just advances time."""

    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def clock(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


def bucket(t: FakeTime, rate: float = 100, capacity: float = 100) -> TokenBucket:
    return TokenBucket(rate, capacity, clock=t.clock, sleep=t.sleep)


async def test_burst_up_to_capacity_is_immediate() -> None:
    t = FakeTime()
    b = bucket(t)
    for _ in range(10):
        await b.acquire(10)
    assert t.slept == []


async def test_waits_for_refill_when_empty() -> None:
    t = FakeTime()
    b = bucket(t)
    for _ in range(10):
        await b.acquire(10)
    await b.acquire(10)
    assert t.now == pytest.approx(0.1)  # 10 tokens at 100/s


async def test_sustained_throughput_matches_rate() -> None:
    t = FakeTime()
    b = bucket(t)
    for _ in range(100):  # 1000 tokens
        await b.acquire(10)
    # 100 tokens free from the initial bucket, 900 refilled at 100/s
    assert t.now == pytest.approx(9.0)


async def test_refill_is_capped_at_capacity() -> None:
    t = FakeTime()
    b = bucket(t)
    await b.acquire(100)
    t.now += 60  # long idle; bucket must not exceed capacity
    await b.acquire(100)
    assert t.slept == []
    await b.acquire(10)
    assert t.now == pytest.approx(60.1)


async def test_cost_validation() -> None:
    b = bucket(FakeTime())
    with pytest.raises(ValueError):
        await b.acquire(101)
    with pytest.raises(ValueError):
        await b.acquire(0)


def test_construction_validation() -> None:
    with pytest.raises(ValueError):
        TokenBucket(0, 10)
    with pytest.raises(ValueError):
        TokenBucket(10, 0)


async def test_waiters_served_in_order() -> None:
    t = FakeTime()
    b = bucket(t, rate=10, capacity=10)
    await b.acquire(10)
    order: list[int] = []

    async def worker(i: int) -> None:
        await b.acquire(10)
        order.append(i)

    await asyncio.gather(*(worker(i) for i in range(5)))
    assert order == [0, 1, 2, 3, 4]
    assert t.now == pytest.approx(5.0)


def test_read_bucket_uses_margin(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("KALSHI_READ_TOKENS_PER_SECOND", raising=False)
    monkeypatch.delenv("KALSHI_RATE_LIMIT_MARGIN", raising=False)
    s = KalshiSettings()
    assert s.read_tokens_per_second == 200
    assert s.rate_limit_margin == 0.9
    assert read_bucket(s)._rate == pytest.approx(180)
    monkeypatch.setenv("KALSHI_READ_TOKENS_PER_SECOND", "600")
    assert read_bucket(KalshiSettings())._rate == pytest.approx(540)


def test_margin_must_not_exceed_budget(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("KALSHI_RATE_LIMIT_MARGIN", "1.5")
    with pytest.raises(ValueError):
        KalshiSettings()
