"""Finding analyzers: everything published under the ``kalshiterm.analyzers`` entry points.

Built-in analyzers register themselves the same way third-party ones do (see this package's
pyproject), so there is exactly one path to test. A plugin that fails to import, or breaks the
rules in ``base``, is reported and skipped; it never stops the others.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from importlib.metadata import EntryPoint, entry_points
from typing import Any

from kalshiterm_server.analytics.base import Analyzer, AnalyzerError, check_analyzer

GROUP = "kalshiterm.analyzers"
log = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoadFailure:
    entry_point: str
    error: str


@dataclass
class Discovery:
    analyzers: dict[str, Analyzer] = field(default_factory=dict)
    sources: dict[str, str] = field(default_factory=dict)  # analyzer name -> "dist: module:attr"
    failures: list[LoadFailure] = field(default_factory=list)


def _instantiate(loaded: Any) -> Any:
    """An entry point may name an instance, a class, or a zero-argument factory."""
    if hasattr(loaded, "run") and not isinstance(loaded, type):
        return loaded
    return loaded()


def discover(
    disabled: set[str] | frozenset[str] = frozenset(),
    find: Callable[[], list[EntryPoint]] | None = None,
) -> Discovery:
    found = Discovery()
    points = find() if find else list(entry_points(group=GROUP))
    for point in sorted(points, key=lambda p: p.name):
        label = f"{point.dist.name if point.dist else '?'}: {point.value}"
        try:
            analyzer = check_analyzer(_instantiate(point.load()))
            if analyzer.name != point.name:
                raise AnalyzerError(
                    f"entry point is named {point.name!r} but the analyzer calls itself "
                    f"{analyzer.name!r}; they must match"
                )
            if analyzer.name in found.analyzers:
                raise AnalyzerError(
                    f"two plugins provide {analyzer.name!r}: "
                    f"{found.sources[analyzer.name]} and {label}"
                )
        except Exception as exc:  # a broken plugin must not stop the others
            log.warning("analyzer %s not loaded: %s", point.name, exc)
            found.failures.append(LoadFailure(point.name, f"{exc.__class__.__name__}: {exc}"))
            continue
        if analyzer.name in disabled:
            continue
        found.analyzers[analyzer.name] = analyzer
        found.sources[analyzer.name] = label
    return found
