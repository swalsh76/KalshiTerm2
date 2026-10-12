from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from kalshiterm_server.alerts import OPS, AlertError, metric_for, triggered
from kalshiterm_server.analytics import builtin
from kalshiterm_server.analytics.base import METRIC, NAME
from kalshiterm_server.analytics.builtin.alert_evaluator import LOOKBACK, decide
from kalshiterm_server.analytics.catalog import CATALOG

NOW = datetime(2026, 10, 12, 12, 0, tzinfo=UTC)
COOLDOWN = timedelta(minutes=5)


@pytest.mark.parametrize(
    ("op", "value", "threshold", "expected"),
    [
        ("gt", 11, 10, True), ("gt", 10, 10, False), ("gt", 9, 10, False),  # strict
        ("lt", 9, 10, True), ("lt", 10, 10, False), ("lt", 11, 10, False),
        ("abs_gt", 11, 10, True), ("abs_gt", -11, 10, True),
        ("abs_gt", 10, 10, False), ("abs_gt", -10, 10, False), ("abs_gt", -9, 10, False),
        ("gt", -5, -10, True), ("lt", -11, -10, True), ("gt", 0, 0, False),
    ],
)  # fmt: skip
def test_the_comparisons_are_strict_and_abs_gt_looks_both_ways(
    op: str, value: int, threshold: int, expected: bool
) -> None:
    assert triggered(op, value, threshold) is expected


def test_an_unknown_comparison_is_an_error_not_false() -> None:
    with pytest.raises(ValueError):
        triggered("ge", 1, 1)


def call(**kw: object) -> str | None:
    base: dict[str, object] = {
        "op": "gt", "threshold": 10, "value": 20, "active": False,
        "last_fired_at": None, "cooldown": COOLDOWN, "now": NOW,
    }  # fmt: skip
    return decide(**{**base, **kw})  # type: ignore[arg-type]


def test_a_rule_fires_when_its_condition_becomes_true() -> None:
    assert call() == "fire"
    assert call(last_fired_at=NOW - COOLDOWN) == "fire"  # exactly one cooldown ago is enough


def test_it_does_not_fire_again_while_the_condition_stays_true() -> None:
    assert call(active=True) is None
    assert call(active=True, value=1_000) is None  # even if the value gets worse


def test_it_is_re_armed_when_the_condition_stops_or_the_result_disappears() -> None:
    assert call(active=True, value=5) == "rearm"
    assert call(active=True, value=None) == "rearm"  # no fresh result: not met
    assert call(active=False, value=5) is None and call(active=False, value=None) is None


def test_a_condition_that_returns_waits_out_the_cooldown() -> None:
    recently = NOW - COOLDOWN + timedelta(seconds=1)
    assert call(last_fired_at=recently) is None  # true again, but too soon
    assert call(last_fired_at=recently, now=NOW + timedelta(seconds=1)) == "fire"


def test_an_absolute_move_rule_fires_on_a_fall_too() -> None:
    assert call(op="abs_gt", value=-20) == "fire"
    assert call(op="gt", value=-20) is None


# ------------------------------------------------------------------ the catalog


def test_the_catalog_entries_are_well_formed() -> None:
    assert CATALOG
    for key, m in CATALOG.items():
        assert key == f"{m.analyzer}.{m.metric}"
        assert NAME.fullmatch(m.analyzer) and METRIC.fullmatch(m.metric)
        assert m.subject in ("market", "event") and m.unit in ("dollars", "contracts", "seconds")
        assert m.ops and set(m.ops) <= set(OPS) and m.fresh_seconds > 0 and m.meaning


def test_every_catalog_metric_is_one_its_analyzer_really_produces() -> None:
    """A rule on a metric nobody emits would never fire; the metric names appear as literals in
    the analyzer's own source."""
    modules = {
        "market_quotes": "market_metrics.py",
        "market_moves": "market_metrics.py",
        "market_volatility": "market_metrics.py",
        "arbitrage_bid": "arbitrage.py",
    }
    folder = Path(builtin.__file__).parent
    for m in CATALOG.values():
        source = (folder / modules[m.analyzer]).read_text()
        assert f'"{m.metric}"' in source or f'f"{m.metric[:-3]}' in source, m.key


def test_the_evaluator_looks_back_far_enough_for_the_slowest_metric() -> None:
    assert timedelta(seconds=max(m.fresh_seconds for m in CATALOG.values())) == LOOKBACK
    assert timedelta(minutes=15) <= LOOKBACK  # volatility runs every 5 minutes


def test_an_unknown_metric_is_a_clear_error() -> None:
    with pytest.raises(AlertError) as caught:
        metric_for("market_moves.change_7y")
    assert caught.value.status == 422 and caught.value.code == "unknown_metric"
    assert metric_for("market_moves.change_15m").subject == "market"
