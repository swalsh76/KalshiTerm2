"""Graceful shutdown for a process that is PID 1 in a container."""

import asyncio
import contextlib
import signal


def cancel_on_sigterm() -> None:
    """Make SIGTERM (what ``docker stop`` sends) cancel the current task, like Ctrl-C does.

    A Python process running as a container's main process ignores SIGTERM unless it installs
    a handler, so Docker waits out the whole grace period and then kills it: the final flush
    of buffered rows never happens. Cancelling the task lets ``finally`` blocks drain first.
    Where signal handlers are unavailable (Windows) this does nothing.
    """
    task = asyncio.current_task()
    if task is None:
        return
    with contextlib.suppress(NotImplementedError, RuntimeError):
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
