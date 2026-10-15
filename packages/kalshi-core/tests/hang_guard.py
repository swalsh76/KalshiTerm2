"""One number for every "this test must not hang" guard in the kalshi-core tests.

These guards wrap local websocket exchanges that finish in milliseconds; they exist only to turn
a hang into a failure instead of a stuck CI job. So they are generous: a degraded runner must not
trip them (on 2026-10-15 a Windows runner ran 5 to 10 times slower than its siblings and a
5-second guard failed four healthy tests), and a passing test never waits for them.

Timeouts that ARE the behaviour under test (``snapshot_timeout=0.1``, "nothing arrives within
0.2 s") are not guards and stay short.
"""

HANG_GUARD = 30  # seconds
