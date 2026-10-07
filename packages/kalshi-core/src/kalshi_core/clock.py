"""Clock-skew estimation against Kalshi.

Signed requests embed a timestamp, so a badly skewed local clock makes every request fail.
Kalshi publishes no time endpoint and no tolerance, so we estimate from the HTTP ``Date``
header, which only has one-second resolution. Each sample therefore yields an *interval* for
the offset; intersecting several spaced samples narrows it. Offsets are ``local - server``
(positive: the local clock is ahead).
"""

import statistics
from dataclasses import dataclass
from datetime import UTC
from email.utils import parsedate_to_datetime
from typing import Literal

DEFAULT_THRESHOLD = 2.0  # seconds; our own choice, Kalshi's tolerance is undocumented

Status = Literal["ok", "skewed", "uncertain"]


class ClockCheckError(Exception):
    """The clock could not be measured (e.g. no usable ``Date`` header)."""


@dataclass(frozen=True, slots=True)
class ClockSample:
    sent: float  # local epoch seconds just before the request
    received: float  # local epoch seconds just after the response
    server: float  # earliest server time implied by the response (``Date`` + ``Age``)


@dataclass(frozen=True, slots=True)
class ClockSkew:
    offset: float  # best estimate of local - server, seconds
    uncertainty: float  # half-width of the interval the true offset lies in
    low: float
    high: float
    samples: int
    consistent: bool  # False if the samples' intervals did not overlap
    threshold: float

    @property
    def status(self) -> Status:
        if self.low > self.threshold or self.high < -self.threshold:
            return "skewed"  # even the closest edge of the interval is beyond the threshold
        if -self.threshold <= self.low and self.high <= self.threshold:
            return "ok"
        return "uncertain"

    def describe(self) -> str:
        direction = "ahead of" if self.offset >= 0 else "behind"
        quality = "" if self.consistent else " (samples disagreed; low confidence)"
        return (
            f"local clock is {abs(self.offset):.2f}s {direction} Kalshi "
            f"(±{self.uncertainty:.2f}s, {self.samples} samples){quality}: {self.status}"
        )


def parse_http_date(value: str) -> float:
    """HTTP ``Date`` header to epoch seconds."""
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError) as exc:
        raise ClockCheckError(f"unparseable Date header: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()


def estimate_skew(samples: list[ClockSample], threshold: float = DEFAULT_THRESHOLD) -> ClockSkew:
    if not samples:
        raise ClockCheckError("no samples")
    # The server stamped its clock at some local instant in [sent, received]; its true time
    # is in [server, server + 1) because Date is truncated to whole seconds.
    intervals = [(s.sent - s.server - 1.0, s.received - s.server) for s in samples]
    low = max(lo for lo, _ in intervals)
    high = min(hi for _, hi in intervals)
    if low <= high:
        return ClockSkew(
            (low + high) / 2, (high - low) / 2, low, high, len(samples), True, threshold
        )
    # Inconsistent (e.g. a cached response): fall back to the median, with the widest interval.
    mids = [(lo + hi) / 2 for lo, hi in intervals]
    half = max((hi - lo) / 2 for lo, hi in intervals)
    offset = statistics.median(mids)
    return ClockSkew(offset, half, offset - half, offset + half, len(samples), False, threshold)
