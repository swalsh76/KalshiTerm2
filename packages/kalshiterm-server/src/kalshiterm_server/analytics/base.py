"""What an analyzer is, what it may return, and the checks both must pass.

A third-party analyzer is a Python object with these attributes, published through the
``kalshiterm.analyzers`` entry-point group::

    class SpreadWatcher:
        name = "spread_watcher"                    # lowercase, unique, 1-48 characters
        interval = timedelta(minutes=5)            # how often to run (10 s to 1 day)
        timeout = timedelta(minutes=1)             # a run is cancelled after this

        async def run(self, ctx: AnalysisContext) -> Sequence[Result]:
            ...

Plugins run inside the analytics process with its database credentials: installing one is
trusting it (sandboxing is parked in PLAN §13). Values are ``int``: ``value_e6`` is the
quantity in millionths (decision 14), never a float.
"""

import inspect
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol, runtime_checkable

from sqlalchemy.ext.asyncio import AsyncEngine

NAME = re.compile(r"[a-z][a-z0-9_-]{0,47}")
METRIC = re.compile(r"[a-z][a-z0-9_.]{0,62}")
MIN_INTERVAL = timedelta(seconds=10)
MAX_INTERVAL = timedelta(days=1)
INT64 = 2**63


class AnalyzerError(Exception):
    """An analyzer or one of its results broke the rules; the message says which."""


@dataclass(frozen=True, slots=True)
class Result:
    """One number about one market or one event."""

    metric: str
    value_e6: int
    market_id: int | None = None
    event_ticker: str | None = None
    ts: datetime | None = None  # default: when the run started


@dataclass(frozen=True, slots=True)
class AnalysisContext:
    engine: AsyncEngine  # read access to the whole database
    now: datetime  # when this run started


@runtime_checkable
class Analyzer(Protocol):
    name: str
    interval: timedelta
    timeout: timedelta

    async def run(self, ctx: AnalysisContext) -> Sequence[Result]: ...


def check_analyzer(obj: Any) -> Analyzer:
    """Raise ``AnalyzerError`` unless ``obj`` is a usable analyzer."""
    name = getattr(obj, "name", None)
    if not isinstance(name, str) or not NAME.fullmatch(name):
        raise AnalyzerError(
            f"bad analyzer name {name!r}: 1-48 of a-z, 0-9, '-' or '_', starting with a letter"
        )
    interval, timeout = getattr(obj, "interval", None), getattr(obj, "timeout", None)
    if not isinstance(interval, timedelta) or not MIN_INTERVAL <= interval <= MAX_INTERVAL:
        raise AnalyzerError(f"{name}: interval must be a timedelta between 10 s and 1 day")
    if not isinstance(timeout, timedelta) or not timedelta(seconds=1) <= timeout <= interval:
        raise AnalyzerError(f"{name}: timeout must be a timedelta between 1 s and the interval")
    if not inspect.iscoroutinefunction(getattr(obj, "run", None)):
        raise AnalyzerError(f"{name}: run must be an `async def run(self, ctx)` method")
    return obj  # type: ignore[no-any-return]


def check_result(analyzer: str, result: Any) -> Result:
    """Raise ``AnalyzerError`` unless ``result`` is storable."""
    if not isinstance(result, Result):
        raise AnalyzerError(f"{analyzer}: returned {type(result).__name__}, not Result")
    if not METRIC.fullmatch(result.metric):
        raise AnalyzerError(f"{analyzer}: bad metric name {result.metric!r}")
    value = result.value_e6
    if isinstance(value, bool) or not isinstance(value, int) or not -INT64 <= value < INT64:
        raise AnalyzerError(
            f"{analyzer}/{result.metric}: value_e6 must be an int that fits 64 bits, "
            f"got {value!r} (store millionths; floats are never stored)"
        )
    if (result.market_id is None) == (result.event_ticker is None):
        raise AnalyzerError(
            f"{analyzer}/{result.metric}: give exactly one of market_id, event_ticker"
        )
    if result.market_id is not None and (
        isinstance(result.market_id, bool)
        or not isinstance(result.market_id, int)
        or not 0 < result.market_id < 2**31
    ):
        raise AnalyzerError(f"{analyzer}/{result.metric}: bad market_id {result.market_id!r}")
    if result.event_ticker is not None and (
        not isinstance(result.event_ticker, str) or not 0 < len(result.event_ticker) <= 100
    ):
        raise AnalyzerError(f"{analyzer}/{result.metric}: bad event_ticker")
    if result.ts is not None and (not isinstance(result.ts, datetime) or result.ts.tzinfo is None):
        raise AnalyzerError(f"{analyzer}/{result.metric}: ts must be a timezone-aware datetime")
    return result
