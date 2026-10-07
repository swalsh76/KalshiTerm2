import math
import random
from email.utils import formatdate
from pathlib import Path

import httpx
import pytest
import respx
from kalshi_core.clock import (
    ClockCheckError,
    ClockSample,
    ClockSkew,
    estimate_skew,
    parse_http_date,
)
from kalshi_core.config import KalshiSettings
from kalshi_core.rest import KalshiRestClient

BASE = "https://external-api.demo.kalshi.co/trade-api/v2"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    for name in ("KALSHI_ENV", "KALSHI_KEY_ID", "KALSHI_PRIVATE_KEY_PATH"):
        monkeypatch.delenv(name, raising=False)


def simulate(offset: float, phases: list[float], rtt: float = 0.05) -> list[ClockSample]:
    """Samples against a server whose clock is ``offset`` seconds behind ours (local - server)."""
    out = []
    for phase in phases:
        sent = 1_700_000_000 + phase
        received = sent + rtt
        server_at_stamp = (sent + received) / 2 - offset
        out.append(ClockSample(sent, received, math.floor(server_at_stamp)))  # Date truncates
    return out


@pytest.mark.parametrize("offset", [0.0, 0.3, -0.8, 3.7, -4.2, 60.0])
def test_interval_always_contains_the_true_offset(offset: float) -> None:
    rng = random.Random(7)
    for _ in range(50):
        phases = sorted(rng.uniform(0, 10) for _ in range(5))
        skew = estimate_skew(simulate(offset, phases))
        assert skew.consistent
        assert skew.low <= offset <= skew.high


def test_more_samples_across_second_boundaries_narrow_the_interval() -> None:
    one = estimate_skew(simulate(0.3, [0.1]))
    many = estimate_skew(simulate(0.3, [0.0, 0.25, 0.5, 0.75, 1.0]))
    assert many.uncertainty < one.uncertainty
    assert one.uncertainty == pytest.approx(0.525, abs=0.01)  # ~(1 + rtt) / 2
    assert many.uncertainty < 0.2


@pytest.mark.parametrize(
    ("offset", "status"),
    [(0.2, "ok"), (-0.9, "ok"), (3.7, "skewed"), (-4.2, "skewed"), (60.0, "skewed")],
)
def test_status_against_threshold(offset: float, status: str) -> None:
    skew = estimate_skew(simulate(offset, [0.0, 0.25, 0.5, 0.75, 1.0]), threshold=2.0)
    assert skew.status == status


def test_near_threshold_is_uncertain_not_guessed() -> None:
    skew = estimate_skew(simulate(2.0, [0.1]), threshold=2.0)
    assert skew.status == "uncertain"


def test_inconsistent_samples_fall_back_and_are_flagged() -> None:
    good = simulate(0.0, [0.1])[0]
    stale = ClockSample(good.sent + 1, good.received + 1, good.server - 10)  # e.g. cached reply
    skew = estimate_skew([good, stale])
    assert not skew.consistent
    assert "low confidence" in skew.describe()


def test_no_samples_is_an_error() -> None:
    with pytest.raises(ClockCheckError):
        estimate_skew([])


def test_describe_reads_naturally() -> None:
    ahead = ClockSkew(3.0, 0.2, 2.8, 3.2, 5, True, 2.0).describe()
    behind = ClockSkew(-1.0, 0.2, -1.2, -0.8, 5, True, 2.0).describe()
    assert "3.00s ahead of Kalshi" in ahead and ahead.endswith("skewed")
    assert "1.00s behind Kalshi" in behind and behind.endswith("ok")


def test_http_date_parsing() -> None:
    assert parse_http_date("Wed, 07 Oct 2026 01:01:31 GMT") == 1791334891.0
    with pytest.raises(ClockCheckError):
        parse_http_date("not a date")


class FakeClock:
    def __init__(self, start: float) -> None:
        self.now = start

    def __call__(self) -> float:
        self.now += 0.03  # each reading advances a little, like real time
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds


@respx.mock
async def test_rest_clock_skew_measures_a_skewed_local_clock() -> None:
    clock = FakeClock(1_700_000_000.0)
    true_offset = 4.0  # local clock is 4 s ahead of the server

    def reply(request: httpx.Request) -> httpx.Response:
        server_now = clock.now - true_offset
        date = formatdate(math.floor(server_now), usegmt=True)
        return httpx.Response(200, json={}, headers={"date": date})

    respx.get(f"{BASE}/exchange/status").mock(side_effect=reply)
    async with KalshiRestClient(KalshiSettings(), sleep=clock.sleep, wall_clock=clock) as c:
        skew = await c.clock_skew(samples=5, spacing=0.25)
    assert skew.consistent and skew.samples == 5
    assert skew.low <= true_offset <= skew.high
    assert skew.status == "skewed"


@respx.mock
async def test_rest_clock_skew_with_a_good_clock_is_ok_and_spaces_samples() -> None:
    clock = FakeClock(1_700_000_000.0)
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)
        await clock.sleep(seconds)

    respx.get(f"{BASE}/exchange/status").mock(
        side_effect=lambda r: httpx.Response(
            200, json={}, headers={"date": formatdate(math.floor(clock.now), usegmt=True)}
        )
    )
    async with KalshiRestClient(KalshiSettings(), sleep=sleep, wall_clock=clock) as c:
        skew = await c.clock_skew(samples=4, spacing=0.3)
    assert skew.status == "ok"
    assert slept == [0.3, 0.3, 0.3]  # between samples only


@respx.mock
async def test_age_header_is_added_to_the_server_time() -> None:
    clock = FakeClock(1_700_000_000.0)
    # A cached reply: generated 3 s ago (Date is old) but Age says so.
    stale_date = formatdate(math.floor(clock.now) - 3, usegmt=True)
    respx.get(f"{BASE}/exchange/status").respond(json={}, headers={"date": stale_date, "age": "3"})
    async with KalshiRestClient(KalshiSettings(), sleep=clock.sleep, wall_clock=clock) as c:
        skew = await c.clock_skew(samples=1)
    assert skew.status == "ok"  # without Age this would look 3 s skewed


@respx.mock
async def test_missing_date_header_is_a_clear_error() -> None:
    respx.get(f"{BASE}/exchange/status").mock(return_value=httpx.Response(200, json={}))
    async with KalshiRestClient(KalshiSettings()) as c:
        with pytest.raises(ClockCheckError, match="Date"):
            await c.clock_skew(samples=1)
