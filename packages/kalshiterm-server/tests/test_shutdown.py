import subprocess
import sys
import textwrap
import time

import pytest

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")

CHILD = textwrap.dedent(
    """
    import asyncio
    from kalshiterm_server.shutdown import cancel_on_sigterm

    async def main():
        cancel_on_sigterm()
        try:
            print("ready", flush=True)
            await asyncio.sleep(60)
        finally:
            await asyncio.sleep(0.2)  # the final flush
            print("drained", flush=True)

    try:
        asyncio.run(main())
    except asyncio.CancelledError:
        pass
    """
)


def test_sigterm_lets_cleanup_finish_instead_of_being_ignored() -> None:
    process = subprocess.Popen(
        [sys.executable, "-I", "-c", CHILD], stdout=subprocess.PIPE, text=True
    )
    assert process.stdout is not None
    assert process.stdout.readline().strip() == "ready"
    started = time.monotonic()
    process.terminate()  # SIGTERM, as `docker stop` sends
    output, _ = process.communicate(timeout=10)
    assert "drained" in output  # the finally block ran
    assert time.monotonic() - started < 5  # and promptly, not after a grace period


def test_without_the_handler_a_python_process_is_left_to_be_killed() -> None:
    """Documents why the handler exists: default SIGTERM skips ``finally`` blocks entirely."""
    script = CHILD.replace("cancel_on_sigterm()\n", "pass\n")
    process = subprocess.Popen(
        [sys.executable, "-I", "-c", script], stdout=subprocess.PIPE, text=True
    )
    assert process.stdout is not None
    process.stdout.readline()
    process.terminate()
    output, _ = process.communicate(timeout=10)
    assert "drained" not in output
