"""deploy/host/keep-mounted.sh: keeps the NAS share mounted without a person.

The macOS commands it relies on (mount, security, osascript, diskutil) are replaced by small fakes
that record what they were asked and keep a "is it mounted" flag in a file, so every decision the
script makes can be checked on any machine: do nothing when the share answers, mount it when it is
gone, recover a stale mount, refuse to try without a saved password, and never hang.
"""

import os
import plistlib
import re
import subprocess
import sys
import time
from pathlib import Path

import pytest

DEPLOY = Path(__file__).resolve().parents[3] / "deploy"
SCRIPT = DEPLOY / "host" / "keep-mounted.sh"
BASH = "/bin/bash" if sys.platform == "darwin" else "bash"
pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="needs bash and POSIX tools")

URL = "smb://stebe@navi/kdb"


class World:
    """A fake machine: a mount point, a flag file for 'mounted', and a log of the calls made."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.mp = root / "Volumes" / "kdb"
        self.flag = root / "mounted"
        self.calls = root / "calls.log"
        self.log = root / "keep.log"
        self.bin = root / "bin"
        self.bin.mkdir()
        self.calls.write_text("")
        mount_line = f"//stebe@navi/kdb on {self.mp} (smbfs, nodev, nosuid, mounted by stebe)"
        self._script("mount", f'[ -e "{self.flag}" ] && echo "{mount_line}"; exit 0')
        self._script("security", 'echo "security $*" >>"$CALLS"; [ "$FAKE_HAS_CRED" = 1 ]')
        self._script("diskutil", f'echo "diskutil $*" >>"$CALLS"; rm -f "{self.flag}"')
        self._script("umount", f'echo "umount $*" >>"$CALLS"; rm -f "{self.flag}"')
        self._script(
            "osascript",
            'echo "osascript $*" >>"$CALLS"; '
            'case "$FAKE_MOUNT" in '
            f'ok) mkdir -p "{self.mp}"; touch "{self.flag}" ;; '
            "hang) sleep 30 ;; "
            "*) exit 1 ;; esac",
        )

    def _script(self, name: str, body: str) -> None:
        path = self.bin / name
        path.write_text(f"#!/bin/sh\n{body}\n")
        path.chmod(0o755)

    def mounted_and_answering(self) -> None:
        self.mp.mkdir(parents=True, exist_ok=True)
        self.flag.touch()

    def mounted_but_dead(self) -> None:
        self.flag.touch()  # `mount` lists it, but the directory is not there to list

    def run(
        self, *, cred: bool = True, mount: str = "ok"
    ) -> tuple[subprocess.CompletedProcess[str], list[str]]:
        env = dict(
            os.environ,
            PATH=f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            CALLS=str(self.calls),
            FAKE_HAS_CRED="1" if cred else "0",
            FAKE_MOUNT=mount,
            KTERM_KEEPMOUNTED_LOG=str(self.log),
            KTERM_LS_TIMEOUT="2",
            KTERM_MOUNT_TIMEOUT="2",
        )
        result = subprocess.run(
            [BASH, str(SCRIPT), URL, str(self.mp)],
            capture_output=True, text=True, env=env, timeout=30,
        )  # fmt: skip
        return result, self.calls.read_text().splitlines()

    def log_text(self) -> str:
        return self.log.read_text() if self.log.exists() else ""


@pytest.fixture
def world(tmp_path: Path) -> World:
    return World(tmp_path)


def test_a_share_that_answers_is_left_alone_and_logs_nothing(world: World) -> None:
    world.mounted_and_answering()
    result, calls = world.run()
    assert result.returncode == 0 and calls == [] and world.log_text() == ""


def test_a_missing_share_is_mounted_with_the_saved_password(world: World) -> None:
    result, calls = world.run()
    assert result.returncode == 0, result.stderr
    assert [c.split()[0] for c in calls] == ["security", "osascript"]
    assert (
        f'mount volume "{URL}"' in calls[1] and "-s navi" in calls[0]
    )  # the host comes from the URL
    assert f"mounted at {world.mp}" in world.log_text()


def test_without_a_saved_password_it_does_not_try_because_that_would_only_show_a_dialog(
    world: World,
) -> None:
    result, calls = world.run(cred=False)
    assert result.returncode == 2
    assert all(not c.startswith("osascript") for c in calls)
    assert "no saved password for navi" in world.log_text()


def test_a_stale_share_is_forced_off_and_then_remounted(world: World) -> None:
    world.mounted_but_dead()
    result, calls = world.run()
    assert result.returncode == 0, result.stderr
    assert [c.split()[0] for c in calls] == ["diskutil", "security", "osascript"]
    assert "not answering" in world.log_text() and f"mounted at {world.mp}" in world.log_text()


def test_a_stale_share_that_will_not_unmount_is_reported_and_nothing_else_is_tried(
    world: World,
) -> None:
    world.mounted_but_dead()
    world._script("diskutil", 'echo "diskutil $*" >>"$CALLS"; exit 1')  # refuses; the flag stays
    world._script("umount", 'echo "umount $*" >>"$CALLS"; exit 1')
    result, calls = world.run()
    assert result.returncode == 1
    assert all(not c.startswith("osascript") for c in calls)
    assert "could not unmount" in world.log_text()


def test_a_failed_mount_is_logged_and_fails(world: World) -> None:
    result, _ = world.run(mount="fail")
    assert result.returncode == 1 and "FAILED to mount" in world.log_text()


def test_a_mount_that_hangs_is_cut_off_instead_of_piling_up(world: World) -> None:
    started = time.monotonic()
    result, _ = world.run(mount="hang")
    assert result.returncode == 1 and "FAILED to mount" in world.log_text()
    assert time.monotonic() - started < 15  # the fake sleeps 30 s; the guard here is 2 s


def test_the_launch_agent_template_renders_to_a_valid_plist_that_runs_bash_explicitly() -> None:
    text = (DEPLOY / "host" / "com.kalshiterm.keepmounted.plist.template").read_text()
    comment = re.search(r"<!--(.*?)-->", text, re.S)
    assert comment is not None and "--" not in comment.group(1)  # invalid inside an XML comment
    rendered = (
        text.replace("__DEPLOY_DIR__", "/Users/x/KalshiTerm2/deploy")
        .replace("__SHARE_URL__", URL)
        .replace("__MOUNT_POINT__", "/Volumes/kdb")
    )
    plist = plistlib.loads(rendered.encode())
    assert plist["Label"] == "com.kalshiterm.keepmounted"
    assert plist["ProgramArguments"] == [
        "/bin/bash",
        "/Users/x/KalshiTerm2/deploy/host/keep-mounted.sh",
        URL,
        "/Volumes/kdb",
    ]
    assert plist["RunAtLoad"] is True and plist["StartInterval"] == 60
