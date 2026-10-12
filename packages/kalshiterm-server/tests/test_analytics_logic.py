import sys
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from importlib.metadata import EntryPoint
from pathlib import Path
from textwrap import dedent

import pytest
from kalshiterm_server.analytics.base import (
    AnalysisContext,
    AnalyzerError,
    Result,
    check_analyzer,
    check_result,
)
from kalshiterm_server.analytics.registry import discover

BUILTIN = {"market_quotes", "market_moves", "market_volatility", "arbitrage_bid"}


def test_the_built_in_analyzers_come_through_the_same_entry_points_as_plugins() -> None:
    found = discover()
    assert set(found.analyzers) >= BUILTIN and found.failures == []
    assert all(found.sources[name].startswith("kalshiterm-server: ") for name in BUILTIN)
    assert found.analyzers["market_volatility"].interval == timedelta(seconds=300)
    assert set(discover(disabled=BUILTIN).analyzers) == set()


class Good:
    name = "good"
    interval = timedelta(minutes=1)
    timeout = timedelta(seconds=30)

    async def run(self, ctx: AnalysisContext) -> list[Result]:
        return []


def variant(**changes: object) -> object:
    attrs = {"name": "good", "interval": timedelta(minutes=1), "timeout": timedelta(seconds=30)}
    attrs.update(changes)
    return type("Variant", (Good,), attrs)()


def test_a_well_formed_analyzer_is_accepted() -> None:
    assert check_analyzer(Good()).name == "good"


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"name": "Bad Name"}, "bad analyzer name"),
        ({"name": ""}, "bad analyzer name"),
        ({"name": "x" * 49}, "bad analyzer name"),
        ({"interval": timedelta(seconds=5)}, "interval"),
        ({"interval": timedelta(days=2)}, "interval"),
        ({"interval": 60}, "interval"),
        ({"timeout": timedelta(minutes=5)}, "timeout"),
        ({"timeout": timedelta(0)}, "timeout"),
    ],
)
def test_bad_analyzer_attributes_are_refused(changes: dict[str, object], message: str) -> None:
    with pytest.raises(AnalyzerError, match=message):
        check_analyzer(variant(**changes))


def test_a_synchronous_run_method_is_refused() -> None:
    class Sync(Good):
        def run(self, ctx: AnalysisContext) -> list[Result]:  # type: ignore[override]
            return []

    with pytest.raises(AnalyzerError, match="async def run"):
        check_analyzer(Sync())


def test_good_results_pass_and_floats_never_do() -> None:
    assert check_result("a", Result("spread.cents", 1_500_000, market_id=7))
    assert check_result("a", Result("arb.profit", -5, event_ticker="KXA-E1"))
    assert check_result("a", Result("m", 1, market_id=1, ts=datetime(2026, 1, 1, tzinfo=UTC)))
    for value in (0.5, "5", True, None, 2**63, -(2**63) - 1):
        with pytest.raises(AnalyzerError, match="value_e6"):
            check_result("a", Result("m", value, market_id=1))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "result",
    [
        Result("Bad Metric", 1, market_id=1),
        Result("m", 1),  # no subject
        Result("m", 1, market_id=1, event_ticker="E"),  # two subjects
        Result("m", 1, market_id=0),
        Result("m", 1, market_id=2**31),
        Result("m", 1, event_ticker=""),
        Result("m", 1, event_ticker="x" * 101),
        Result("m", 1, market_id=1, ts=datetime(2026, 1, 1)),  # naive
    ],
)
def test_malformed_results_are_refused(result: Result) -> None:
    with pytest.raises(AnalyzerError):
        check_result("a", result)


def test_something_that_is_not_a_result_is_refused() -> None:
    with pytest.raises(AnalyzerError, match="not Result"):
        check_result("a", {"metric": "m", "value_e6": 1})


# ------------------------------------------------------------------ discovery

PLUGIN = dedent(
    """
    from datetime import timedelta

    class Spread:
        name = "spread"
        interval = timedelta(minutes=5)
        timeout = timedelta(minutes=1)
        async def run(self, ctx):
            return []

    class WrongName(Spread):
        name = "something_else"

    class TooFast(Spread):
        name = "too_fast"
        interval = timedelta(seconds=1)

    instance = Spread()
    def factory():
        return Spread()
    raise_on_import = None
    """
)


def install(
    root: Path, dist: str, entries: dict[str, str], module: str = "kterm_fake_plugin"
) -> None:
    """A package that looks installed to importlib.metadata: a .dist-info with entry points."""
    (root / f"{module}.py").write_text(PLUGIN)
    info = root / f"{dist}-1.0.dist-info"
    info.mkdir()
    (info / "METADATA").write_text(f"Metadata-Version: 2.1\nName: {dist}\nVersion: 1.0\n")
    lines = "\n".join(f"{name} = {value}" for name, value in entries.items())
    (info / "entry_points.txt").write_text(f"[kalshiterm.analyzers]\n{lines}\n")


@pytest.fixture
def plugins(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    monkeypatch.syspath_prepend(str(tmp_path))
    yield tmp_path
    sys.modules.pop("kterm_fake_plugin", None)


def test_a_plugin_published_through_an_entry_point_is_found_whether_class_instance_or_factory(
    plugins: Path,
) -> None:
    install(plugins, "kterm-fake", {"spread": "kterm_fake_plugin:Spread"})
    found = discover()
    assert "spread" in found.analyzers and found.analyzers["spread"].interval == timedelta(
        minutes=5
    )
    assert found.sources["spread"].startswith("kterm-fake: kterm_fake_plugin:Spread")
    assert found.failures == []


def test_instances_and_factories_work_too(plugins: Path) -> None:
    install(plugins, "kterm-fake", {"spread": "kterm_fake_plugin:instance"})
    assert "spread" in discover().analyzers
    sys.modules.pop("kterm_fake_plugin", None)
    (plugins / "kterm-fake-1.0.dist-info" / "entry_points.txt").write_text(
        "[kalshiterm.analyzers]\nspread = kterm_fake_plugin:factory\n"
    )
    assert "spread" in discover().analyzers


def test_a_broken_plugin_is_reported_and_the_good_ones_still_load(plugins: Path) -> None:
    install(
        plugins,
        "kterm-fake",
        {
            "spread": "kterm_fake_plugin:Spread",
            "missing": "kterm_no_such_module:Thing",
            "wrong": "kterm_fake_plugin:WrongName",
            "too_fast": "kterm_fake_plugin:TooFast",
        },
    )
    found = discover()
    assert set(found.analyzers) == {"spread"} | BUILTIN
    reasons = {f.entry_point: f.error for f in found.failures}
    assert "ModuleNotFoundError" in reasons["missing"]
    assert "must match" in reasons["wrong"]
    assert "interval" in reasons["too_fast"]


def test_two_entry_points_providing_the_same_analyzer_do_not_both_load(plugins: Path) -> None:
    """importlib keeps one entry point per name, so exercise the guard through ``find``."""
    install(plugins, "kterm-fake", {"spread": "kterm_fake_plugin:Spread"})
    point = EntryPoint("spread", "kterm_fake_plugin:Spread", "kalshiterm.analyzers")
    found = discover(find=lambda: [point, point])
    assert list(found.analyzers) == ["spread"]
    assert any("two plugins provide 'spread'" in f.error for f in found.failures)


def test_disabled_analyzers_are_left_out_without_being_failures(plugins: Path) -> None:
    install(plugins, "kterm-fake", {"spread": "kterm_fake_plugin:Spread"})
    found = discover(disabled={"spread"})
    assert set(found.analyzers) == BUILTIN and found.failures == []
