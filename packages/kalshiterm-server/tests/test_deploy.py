import json
import os
import re
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml
from kalshiterm_server.ingest.discovery import discovery_loop
from kalshiterm_server.status import host_check

DEPLOY = Path(__file__).resolve().parents[3] / "deploy"
SCRIPT = DEPLOY / "host" / "check-data-drive.sh"
NOW = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)
posix_only = pytest.mark.skipif(sys.platform == "win32", reason="needs bash and POSIX permissions")


def load(name: str) -> dict[str, Any]:
    loaded = yaml.safe_load((DEPLOY / name).read_text())
    assert isinstance(loaded, dict)
    return loaded


@pytest.fixture(scope="module")
def prod() -> dict[str, Any]:
    return load("docker-compose.yml")


# ---------------------------------------------------------------- the production compose file


@pytest.mark.parametrize("name", ["docker-compose.yml", "docker-compose.setup.yml"])
def test_nothing_is_published_to_the_network(name: str) -> None:
    for service_name, service in load(name)["services"].items():
        assert "ports" not in service, f"{service_name} publishes a port"
        assert service.get("network_mode") != "host", service_name


def test_the_database_has_no_route_to_the_internet_and_ingest_reaches_both_sides(
    prod: dict[str, Any],
) -> None:
    assert prod["networks"]["internal"]["internal"] is True
    assert prod["services"]["db"]["networks"] == ["internal"]
    assert set(prod["services"]["ingest"]["networks"]) == {"internal", "default"}


def test_the_database_image_matches_the_one_the_tests_run_against(prod: dict[str, Any]) -> None:
    dev = load("docker-compose.dev.yml")["services"]["timescaledb"]["image"]
    assert prod["services"]["db"]["image"] == dev  # what we test is what we deploy


def test_the_database_does_not_phone_home_and_keeps_data_in_a_named_volume(
    prod: dict[str, Any],
) -> None:
    db = prod["services"]["db"]
    assert "timescaledb.telemetry_level=off" in db["command"]
    assert db["volumes"] == ["kterm_pgdata:/var/lib/postgresql"]  # a named volume, not a bind
    assert "kterm_pgdata" in prod["volumes"]


def test_both_services_have_health_checks_and_restart_and_ingest_waits_for_the_database(
    prod: dict[str, Any],
) -> None:
    for name in ("db", "ingest"):
        service = prod["services"][name]
        assert "healthcheck" in service and service["restart"] == "unless-stopped", name
    assert prod["services"]["ingest"]["depends_on"]["db"]["condition"] == "service_healthy"
    assert prod["services"]["ingest"]["healthcheck"]["test"] == ["CMD", "kterm-server", "health"]


def test_logs_are_rotated_so_they_cannot_fill_the_data_disk(prod: dict[str, Any]) -> None:
    for name in ("db", "ingest"):
        logging = prod["services"][name]["logging"]
        assert logging["driver"] == "json-file"
        assert logging["options"]["max-size"] and logging["options"]["max-file"]


def test_ingest_gets_its_key_and_config_read_only_and_migrates_before_starting(
    prod: dict[str, Any],
) -> None:
    ingest = prod["services"]["ingest"]
    assert "./secrets:/run/secrets:ro" in ingest["volumes"]
    assert "./config:/config:ro" in ingest["volumes"]
    assert "./state:/hoststate:ro" in ingest["volumes"]
    command = " ".join(ingest["command"])
    assert command.index("db upgrade") < command.index("ingest")
    assert "--watchlist /config/watchlist.toml" in command and "--discover-every" in command
    assert ingest["environment"]["KALSHI_ENV"].startswith("${KALSHI_ENV:-production")


def test_no_secret_is_written_into_the_compose_file(prod: dict[str, Any]) -> None:
    text = (DEPLOY / "docker-compose.yml").read_text()
    assert not re.search(r"PRIVATE KEY|password\s*[:=]\s+[^$\s]", text, re.IGNORECASE)
    # every credential comes from the generated .env, with a message if init was not run
    assert "${POSTGRES_PASSWORD:?" in text and "${KALSHI_KEY_ID:?" in text


def test_setup_is_a_separate_file_with_no_network_so_the_main_file_can_stay_strict() -> None:
    setup = load("docker-compose.setup.yml")["services"]["init"]
    assert setup["entrypoint"][:2] == ["kterm-server", "init"]
    assert "--key-file" in setup["entrypoint"] and "command" not in setup  # args are appended
    assert setup["network_mode"] == "none"
    assert "init" not in load("docker-compose.yml")["services"]
    assert any(v.endswith(":/input/key.pem:ro") for v in setup["volumes"])  # key mounted read-only


# ---------------------------------------------------------------- the host-side drive check


def run_script(*args: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["bash", str(SCRIPT), *map(str, args)], capture_output=True, text=True)


@posix_only
def test_a_writable_directory_is_reported_healthy_with_free_space(tmp_path: Path) -> None:
    out = tmp_path / "host.json"
    result = run_script(tmp_path, out, "--allow-plain-directory")
    assert result.returncode == 0, result.stderr
    state = json.loads(out.read_text())
    assert state["mounted"] is True and state["writable"] is True and state["error"] == ""
    assert 0 < state["free_pct"] <= 100 and state["path"] == str(tmp_path)
    assert abs(state["checked_at"] - datetime.now(UTC).timestamp()) < 30
    assert not list(tmp_path.glob(".kterm-hostcheck.*")) and not list(tmp_path.glob("*.tmp.*"))


@posix_only
def test_a_missing_drive_is_a_result_not_a_crash(tmp_path: Path) -> None:
    out = tmp_path / "host.json"
    result = run_script(tmp_path / "Volumes" / "KalshiData", out)
    assert result.returncode == 0
    state = json.loads(out.read_text())
    assert state["mounted"] is False and state["writable"] is False
    assert "does not exist" in state["error"] and state["free_pct"] is None


@posix_only
def test_an_ordinary_folder_is_not_mistaken_for_a_mounted_drive(tmp_path: Path) -> None:
    folder = tmp_path / "KalshiData"  # same disk as its parent: the unplugged-drive look-alike
    folder.mkdir()
    out = tmp_path / "host.json"
    run_script(folder, out)
    state = json.loads(out.read_text())
    assert state["mounted"] is False and "not a mount point" in state["error"]


@posix_only
@pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0, reason="root can always write")
def test_a_read_only_drive_is_reported_as_not_writable(tmp_path: Path) -> None:
    drive = tmp_path / "ro"
    drive.mkdir()
    drive.chmod(0o500)
    out = tmp_path / "host.json"
    try:
        run_script(drive, out, "--allow-plain-directory")
    finally:
        drive.chmod(0o700)
    state = json.loads(out.read_text())
    assert state["mounted"] is True and state["writable"] is False
    assert "cannot write" in state["error"]


@posix_only
def test_odd_characters_in_the_path_still_produce_valid_json(tmp_path: Path) -> None:
    drive = tmp_path / 'we"ird\\ name'
    drive.mkdir()
    out = tmp_path / "host.json"
    run_script(drive, out, "--allow-plain-directory")
    assert json.loads(out.read_text())["mounted"] is True


# ---------------------------------------------------------------- reading it in status


def report(**fields: Any) -> dict[str, Any]:
    base = {
        "checked_at": int((NOW - timedelta(seconds=20)).timestamp()),
        "path": "/Volumes/KalshiData",
        "mounted": True,
        "writable": True,
        "free_pct": 62.5,
        "error": "",
    }
    return {**base, **fields}


def check(tmp_path: Path, state: dict[str, Any] | None) -> list[str]:
    path = tmp_path / "host.json"
    if state is not None:
        path.write_text(json.dumps(state))
    problems: list[str] = []
    host_check(str(path), NOW, problems)
    return problems


def test_a_healthy_host_report_is_no_problem(tmp_path: Path) -> None:
    assert check(tmp_path, report()) == []


def test_status_flags_an_unmounted_unwritable_full_or_silent_drive(tmp_path: Path) -> None:
    assert "NOT MOUNTED" in check(tmp_path, report(mounted=False, writable=False))[0]
    assert "not writable" in check(tmp_path, report(writable=False, error="cannot write"))[0]
    assert "only 9.0% free" in check(tmp_path, report(free_pct=9.0))[0]
    stale = report(checked_at=int((NOW - timedelta(minutes=30)).timestamp()))
    assert "last ran 30m ago" in check(tmp_path, stale)[0]


def test_a_missing_or_garbled_report_is_a_problem_and_no_setting_means_no_check(
    tmp_path: Path,
) -> None:
    assert "never reported" in check(tmp_path, None)[0]
    (tmp_path / "host.json").write_text("{not json")
    problems: list[str] = []
    host_check(str(tmp_path / "host.json"), NOW, problems)
    assert "never reported" in problems[0]
    problems = []
    assert host_check(None, NOW, problems) is None and problems == []


# ---------------------------------------------------------------- the discovery loop


class Stop(Exception):
    pass


async def test_the_discovery_loop_keeps_going_after_a_failed_cycle() -> None:
    calls: list[str] = []

    class FakeReport:
        def lines(self) -> list[str]:
            return ["mode: incremental", "took 1.0s"]

    async def cycle(rest: object, engine: object) -> FakeReport:
        calls.append("cycle")
        if len(calls) == 1:
            raise ConnectionError("kalshi unreachable")
        return FakeReport()

    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        if len(sleeps) == 3:
            raise Stop

    with pytest.raises(Stop):
        await discovery_loop(None, None, 900, run=cycle, sleep=sleep)  # type: ignore[arg-type]
    assert calls == ["cycle"] * 3 and sleeps == [900, 900, 900]  # the failure did not end it
