"""deploy/host/install.sh: the host-side stand-up (PLAN.md section 10.2, slice 2.11).

The script drives macOS launchd, so these tests only run it in its read-only `check` mode and in
`--dry-run`, which prints the plists and the commands and changes nothing. On macOS the script is
run with /bin/bash (3.2, what the Mac Studio has), so the macOS CI jobs prove that compatibility.
"""

import plistlib
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

DEPLOY = Path(__file__).resolve().parents[3] / "deploy"
# macOS: its own bash 3.2. Elsewhere: an absolute path, because some tests run the script with a
# stripped PATH, where a bare "bash" cannot be found.
BASH = "/bin/bash" if sys.platform == "darwin" else (shutil.which("bash") or "bash")
pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="needs bash and POSIX permissions")

PEM = "-----BEGIN PRIVATE KEY-----\nTOPSECRETKEYBYTES\n-----END PRIVATE KEY-----\n"


@pytest.fixture
def deploy(tmp_path: Path) -> Path:
    """A copy of deploy/host in a deploy directory where `init` has already run."""
    root = tmp_path / "deploy"
    shutil.copytree(DEPLOY / "host", root / "host")  # keeps the executable bits
    (root / "state").mkdir()
    (root / "secrets").mkdir()
    (root / "secrets" / "tls_cert.pem").write_text("certificate")
    (root / ".env").write_text("KTERM_STORAGE_BUDGET_GB=1\nKTERM_API_PORT=8701\n")
    return root


@pytest.fixture
def drive(tmp_path: Path) -> Path:
    path = tmp_path / "drive"
    path.mkdir()
    return path


@pytest.fixture
def key(tmp_path: Path) -> Path:
    path = tmp_path / "server_key.pem"
    path.write_text(PEM)
    path.chmod(0o600)
    return path


def fake_docker(tmp_path: Path, *, running: bool) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    script = bin_dir / "docker"
    script.write_text(f"#!/bin/sh\nexit {0 if running else 1}\n")
    script.chmod(0o755)
    return bin_dir


def install(
    deploy: Path,
    tmp_path: Path,
    *args: object,
    stdin: str = "",
    docker: bool = True,
    volumes: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    import os

    env = dict(os.environ)
    env["PATH"] = f"{fake_docker(tmp_path, running=docker)}{os.pathsep}{env['PATH']}"
    if volumes is not None:
        env["KTERM_VOLUMES_DIR"] = str(volumes)
    return subprocess.run(
        [BASH, str(deploy / "host" / "install.sh"), *map(str, args)],
        input=stdin,
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
    )


def plists(output: str) -> dict[str, dict[str, object]]:
    found = re.findall(
        r"----- BEGIN (\S+)\.plist .*? -----\n(.*?)----- END \1\.plist -----", output, re.S
    )
    return {label: plistlib.loads(body.encode()) for label, body in found}


# ---------------------------------------------------------------- check (before init)


def test_check_passes_on_a_good_host_and_prints_the_init_command(
    deploy: Path, tmp_path: Path, key: Path, drive: Path
) -> None:
    result = install(
        deploy, tmp_path, "check", "--key-file", key, "--drive", drive, "--allow-plain-directory"
    )
    assert result.returncode == 0, result.stderr
    assert "All good" in result.stdout and f"KEY_FILE={key}" in result.stdout
    assert "TOPSECRETKEYBYTES" not in result.stdout + result.stderr  # never shows key contents


def test_check_explains_a_key_path_that_is_a_directory(deploy: Path, tmp_path: Path) -> None:
    stray = tmp_path / "server_key.pem"
    stray.mkdir()  # what Docker leaves behind for a mount source that did not exist
    result = install(deploy, tmp_path, "check", "--key-file", stray)
    assert result.returncode == 1
    assert "DIRECTORY" in result.stderr and "rmdir" in result.stderr


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        (None, "does not exist"),
        ("", "is empty"),
        ("just some text\n", "does not look like a PEM"),
        ("-----BEGIN ENCRYPTED PRIVATE KEY-----\nx\n", "passphrase-protected"),
    ],
)
def test_check_names_what_is_wrong_with_the_key(
    deploy: Path, tmp_path: Path, content: str | None, expected: str
) -> None:
    path = tmp_path / "k.pem"
    if content is not None:
        path.write_text(content)
    result = install(deploy, tmp_path, "check", "--key-file", path)
    assert result.returncode == 1 and expected in result.stderr


def test_check_wants_an_absolute_key_path(deploy: Path, tmp_path: Path) -> None:
    result = install(deploy, tmp_path, "check", "--key-file", "~/server_key.pem")
    assert result.returncode == 1 and "must be absolute" in result.stderr


def test_check_warns_about_a_loose_key_mode_but_passes(
    deploy: Path, tmp_path: Path, key: Path
) -> None:
    key.chmod(0o644)
    result = install(deploy, tmp_path, "check", "--key-file", key)
    assert result.returncode == 0 and "chmod 600" in result.stderr


def test_check_fails_when_docker_is_not_running(deploy: Path, tmp_path: Path) -> None:
    result = install(deploy, tmp_path, "check", docker=False)
    assert result.returncode == 1 and "not running" in result.stderr


def test_check_wants_a_real_drive_not_a_folder_on_the_boot_disk(
    deploy: Path, tmp_path: Path, drive: Path
) -> None:
    result = install(deploy, tmp_path, "check", "--drive", drive)
    assert result.returncode == 1 and "not a mount point" in result.stderr


@pytest.mark.parametrize(
    ("name", "expected"),
    [("missing", "does not exist"), ("a&b", "not allowed"), ("it's", "not allowed")],
)
def test_check_rejects_bad_drive_paths(
    deploy: Path, tmp_path: Path, name: str, expected: str
) -> None:
    path = tmp_path / name
    if name != "missing":
        path.mkdir()
    result = install(deploy, tmp_path, "check", "--drive", path, "--allow-plain-directory")
    assert result.returncode == 1 and expected in result.stderr


def test_check_rejects_a_relative_drive_path(deploy: Path, tmp_path: Path) -> None:
    result = install(deploy, tmp_path, "check", "--drive", "Volumes/Data")
    assert result.returncode == 1 and "must be absolute" in result.stderr


# ---------------------------------------------------------------- docker off the PATH

TOOLS = (
    "dirname",
    "df",
    "awk",
    "sed",
    "head",
    "grep",
    "tr",
    "stat",
    "date",
    "cat",
    "rm",
    "uname",
    "id",
)


def check_with_bare_path(
    deploy: Path, tmp_path: Path, docker_dirs: Path, *, daemon_up: bool | None
) -> subprocess.CompletedProcess[str]:
    """Run `check` the way `ssh host 'cmd'` does: a PATH with the basic tools and no Docker."""
    farm = tmp_path / "farm"
    farm.mkdir(exist_ok=True)
    for tool in TOOLS:
        found = shutil.which(tool)
        if found and not (farm / tool).exists():
            (farm / tool).symlink_to(found)
    if daemon_up is not None:
        docker_dirs.mkdir(exist_ok=True)
        script = docker_dirs / "docker"
        script.write_text(f"#!/bin/sh\nexit {0 if daemon_up else 1}\n")
        script.chmod(0o755)
    env = {"PATH": str(farm), "HOME": str(tmp_path), "KTERM_DOCKER_DIRS": str(docker_dirs)}
    return subprocess.run(
        [BASH, str(deploy / "host" / "install.sh"), "check"],
        input="", capture_output=True, text=True, env=env, timeout=60,
    )  # fmt: skip


def test_docker_is_found_in_its_usual_place_when_it_is_not_on_the_path(
    deploy: Path, tmp_path: Path
) -> None:
    result = check_with_bare_path(deploy, tmp_path, tmp_path / "dockerbin", daemon_up=True)
    assert result.returncode == 0, result.stderr
    assert "docker is running" in result.stdout and "not on this shell's PATH" in result.stdout


def test_a_docker_that_is_found_but_not_running_is_said_so(deploy: Path, tmp_path: Path) -> None:
    result = check_with_bare_path(deploy, tmp_path, tmp_path / "dockerbin", daemon_up=False)
    assert result.returncode == 1 and "not running" in result.stderr


def test_a_missing_docker_names_where_it_looked_and_how_to_fix_the_path(
    deploy: Path, tmp_path: Path
) -> None:
    result = check_with_bare_path(deploy, tmp_path, tmp_path / "nowhere", daemon_up=None)
    assert result.returncode == 1
    assert "docker not found" in result.stderr and str(tmp_path / "nowhere") in result.stderr
    assert ".zshenv" in result.stderr


# ---------------------------------------------------------------- install --dry-run


def test_a_dry_run_renders_both_valid_plists_with_the_right_paths(
    deploy: Path, tmp_path: Path, drive: Path
) -> None:
    result = install(
        deploy, tmp_path, "install", "--drive", drive, "--allow-plain-directory",
        "--advertise", "--dry-run",
    )  # fmt: skip
    assert result.returncode == 0, result.stderr
    assert "Dry run: nothing was changed" in result.stdout
    assert (
        "<!--" not in result.stdout
    )  # the instruction comment is stripped from the installed file
    rendered = plists(result.stdout)
    assert set(rendered) == {"com.kalshiterm.hostcheck", "com.kalshiterm.advertise"}
    # started as /bin/bash, not through the script's `env` shebang: macOS privacy (TCC) then sees
    # bash as the responsible program, which is the narrow thing to grant access to
    assert rendered["com.kalshiterm.hostcheck"]["ProgramArguments"] == [
        "/bin/bash",
        str(deploy / "host" / "check-data-drive.sh"),
        str(drive),
        str(deploy / "state" / "host.json"),
    ]
    assert rendered["com.kalshiterm.advertise"]["ProgramArguments"] == [
        str(deploy / "host" / "advertise.sh"),
        str(deploy / "secrets" / "tls_cert.pem"),
        "8701",  # the port comes from .env
    ]
    assert "would run: sudo launchctl bootstrap system" in result.stdout


def test_a_dry_run_without_the_advertiser_installs_only_the_drive_check(
    deploy: Path, tmp_path: Path, drive: Path
) -> None:
    result = install(
        deploy, tmp_path, "install", "--drive", drive, "--allow-plain-directory",
        "--no-advertise", "--port", "9000", "--dry-run",
    )  # fmt: skip
    assert result.returncode == 0, result.stderr
    assert set(plists(result.stdout)) == {"com.kalshiterm.hostcheck"}
    assert "advertise" not in result.stdout


def test_a_drive_path_with_a_space_survives_into_the_plist(deploy: Path, tmp_path: Path) -> None:
    spaced = tmp_path / "Samsung T7"
    spaced.mkdir()
    result = install(
        deploy, tmp_path, "install", "--drive", spaced, "--allow-plain-directory",
        "--no-advertise", "--dry-run",
    )  # fmt: skip
    assert result.returncode == 0, result.stderr
    assert plists(result.stdout)["com.kalshiterm.hostcheck"]["ProgramArguments"][2] == str(spaced)  # type: ignore[index]


def test_install_needs_init_to_have_run(deploy: Path, tmp_path: Path, drive: Path) -> None:
    (deploy / ".env").unlink()
    result = install(deploy, tmp_path, "install", "--drive", drive, "--dry-run")
    assert result.returncode == 1 and "run init first" in result.stderr


def test_install_refuses_the_advertiser_without_a_certificate(
    deploy: Path, tmp_path: Path, drive: Path
) -> None:
    (deploy / "secrets" / "tls_cert.pem").unlink()
    result = install(
        deploy, tmp_path, "install", "--drive", drive, "--allow-plain-directory",
        "--advertise", "--dry-run",
    )  # fmt: skip
    assert result.returncode == 1 and "tls_cert.pem" in result.stderr


def test_install_refuses_a_folder_that_is_not_a_mount_point(
    deploy: Path, tmp_path: Path, drive: Path
) -> None:
    result = install(deploy, tmp_path, "install", "--drive", drive, "--no-advertise", "--dry-run")
    assert result.returncode == 1 and "not a mount point" in result.stderr


def test_yes_never_guesses_which_volume_holds_the_data(deploy: Path, tmp_path: Path) -> None:
    result = install(deploy, tmp_path, "install", "--yes", "--dry-run")
    assert result.returncode == 1 and "--yes needs --drive" in result.stderr


# ---------------------------------------------------------------- the volume prompt


@pytest.fixture
def volumes(tmp_path: Path) -> Path:
    root = tmp_path / "Volumes"
    (root / "Alpha").mkdir(parents=True)
    (root / "Beta").mkdir()
    return root


def test_the_volume_prompt_lists_volumes_and_takes_a_number(
    deploy: Path, tmp_path: Path, volumes: Path
) -> None:
    result = install(
        deploy, tmp_path, "install", "--allow-plain-directory", "--no-advertise", "--dry-run",
        stdin="2\n", volumes=volumes,
    )  # fmt: skip
    assert result.returncode == 0, result.stderr
    assert "1) " in result.stderr and "Alpha" in result.stderr and "Beta" in result.stderr
    args = plists(result.stdout)["com.kalshiterm.hostcheck"]["ProgramArguments"]
    assert args[2] == str(volumes / "Beta")  # type: ignore[index]


def test_the_volume_prompt_also_takes_a_typed_path(
    deploy: Path, tmp_path: Path, volumes: Path, drive: Path
) -> None:
    result = install(
        deploy, tmp_path, "install", "--allow-plain-directory", "--no-advertise", "--dry-run",
        stdin=f"{drive}\n", volumes=volumes,
    )  # fmt: skip
    assert result.returncode == 0, result.stderr
    assert plists(result.stdout)["com.kalshiterm.hostcheck"]["ProgramArguments"][2] == str(drive)  # type: ignore[index]


def test_the_volume_prompt_rejects_a_number_out_of_range_and_no_answer(
    deploy: Path, tmp_path: Path, volumes: Path
) -> None:
    args = ("install", "--allow-plain-directory", "--no-advertise", "--dry-run")
    out_of_range = install(deploy, tmp_path, *args, stdin="9\n", volumes=volumes)
    assert out_of_range.returncode == 1 and "choose a number" in out_of_range.stderr
    no_answer = install(deploy, tmp_path, *args, stdin="", volumes=volumes)
    assert no_answer.returncode == 1 and "no answer" in no_answer.stderr


def test_with_no_mounted_volume_the_prompt_says_so(deploy: Path, tmp_path: Path) -> None:
    empty = tmp_path / "EmptyVolumes"
    empty.mkdir()
    result = install(deploy, tmp_path, "install", "--no-advertise", "--dry-run", volumes=empty)
    assert result.returncode == 1 and "no mounted volume" in result.stderr


# ---------------------------------------------------------------- uninstall, help, misuse


def test_uninstall_dry_run_removes_both_daemons(deploy: Path, tmp_path: Path) -> None:
    result = install(deploy, tmp_path, "uninstall", "--dry-run")
    assert result.returncode == 0, result.stderr
    for label in ("hostcheck", "advertise"):
        assert f"sudo launchctl bootout system/com.kalshiterm.{label}" in result.stdout
        assert f"sudo rm -f /Library/LaunchDaemons/com.kalshiterm.{label}.plist" in result.stdout


def test_help_and_misuse(deploy: Path, tmp_path: Path) -> None:
    helped = install(deploy, tmp_path, "help")
    assert helped.returncode == 0 and "check -> init -> up -> install" in helped.stdout
    assert install(deploy, tmp_path, "frobnicate").returncode == 2
    assert install(deploy, tmp_path, "check", "--bogus").returncode == 2


@pytest.mark.skipif(sys.platform == "darwin", reason="would reach sudo on a Mac")
def test_a_real_install_refuses_to_run_outside_macos(
    deploy: Path, tmp_path: Path, drive: Path
) -> None:
    result = install(
        deploy, tmp_path, "install", "--drive", drive, "--allow-plain-directory", "--no-advertise"
    )
    assert result.returncode == 1 and "macOS launchd" in result.stderr


# ---------------------------------------------------------------- verify (after a permission fix)


def write_report(
    deploy: Path, *, mounted: bool, writable: bool, checked_at: int | None = None
) -> None:
    import json
    import time

    report = {
        "checked_at": checked_at if checked_at is not None else int(time.time()) + 1000,
        "path": "/Volumes/X",
        "mounted": mounted,
        "writable": writable,
        "free_pct": 99.0,
        "error": "" if writable else "cannot write to /Volumes/X",
    }
    (deploy / "state" / "host.json").write_text(json.dumps(report))


def verify(deploy: Path, tmp_path: Path) -> subprocess.CompletedProcess[str]:
    import os

    env = dict(os.environ, KTERM_VERIFY_TIMEOUT="3")
    return subprocess.run(
        [BASH, str(deploy / "host" / "install.sh"), "verify"],
        input="", capture_output=True, text=True, env=env, timeout=60,
    )  # fmt: skip


def test_verify_passes_on_a_fresh_good_report(deploy: Path, tmp_path: Path) -> None:
    write_report(deploy, mounted=True, writable=True)
    result = verify(deploy, tmp_path)
    assert result.returncode == 0, result.stderr
    assert "The drive check works" in result.stdout


def test_verify_explains_the_macos_privacy_block_when_the_drive_is_not_writable(
    deploy: Path, tmp_path: Path
) -> None:
    write_report(deploy, mounted=True, writable=False)
    result = verify(deploy, tmp_path)
    assert result.returncode == 1
    assert "Full Disk Access" in result.stderr and "/bin/bash" in result.stderr
    assert "host/install.sh verify" in result.stderr and "tccd" in result.stderr


def test_verify_does_not_blame_privacy_when_the_drive_is_not_even_mounted(
    deploy: Path, tmp_path: Path
) -> None:
    write_report(deploy, mounted=False, writable=False)
    result = verify(deploy, tmp_path)
    assert result.returncode == 1 and "Full Disk Access" not in result.stderr


def test_verify_ignores_a_stale_report_and_says_the_job_may_not_be_running(
    deploy: Path, tmp_path: Path
) -> None:
    write_report(deploy, mounted=True, writable=True, checked_at=1_000_000)  # long ago
    result = verify(deploy, tmp_path)
    assert (
        result.returncode == 1
        and "no fresh" in result.stderr
        and "launchctl print" in result.stderr
    )
