"""Live read-only soak: everything the Phase 2 ingestor would subscribe to, for N minutes.

Run:  uv run python packages/kalshi-core/bench/soak.py [--minutes 10] [--markets 50]

Needs the read-only production key in .env. Subscribes to all trades, all tickers, both
lifecycle channels, and orderbooks for the busiest ordinary markets; consumes with a fast
counting loop so it measures the real incoming traffic, not our own backlog.
"""

import argparse
import asyncio
import collections
import json
import os
import subprocess
import time

from kalshi_core.auth import KalshiSigner
from kalshi_core.config import KalshiSettings
from kalshi_core.orderbook import (
    DELTA,
    GAP,
    MESSAGE,
    RESET,
    RESUBSCRIBE_FAILED,
    RESYNC_FAILED,
    SNAPSHOT,
    OrderBookFeed,
)
from kalshi_core.rest import KalshiRestClient
from kalshi_core.ws import KalshiWebSocket

CHANNELS = ("trade", "ticker", "multivariate_market_lifecycle", "market_lifecycle_v2")


def rss_mb() -> float:
    cmd = ["ps", "-o", "rss=", "-p", str(os.getpid())]
    return int(subprocess.run(cmd, capture_output=True, text=True).stdout.strip()) / 1024


class Stats:
    def __init__(self) -> None:
        self.totals: collections.Counter[str] = collections.Counter()
        self.bytes: collections.Counter[str] = collections.Counter()
        self.window: collections.Counter[str] = collections.Counter()
        self.per_second: dict[str, list[int]] = collections.defaultdict(list)
        self.last_seq: dict[int, int] = {}
        self.seq_gaps = 0
        self.parse_failures = 0
        self.resets = 0
        self.problems: list[str] = []
        self.rss: list[float] = []
        self.ws_queue_max = 0
        self.out_queue_max = 0
        self.lag_max = 0.0
        self.lag_total = 0.0
        self.lag_samples = 0

    def tick(self) -> None:
        for key in set(self.totals) | set(self.per_second):
            self.per_second[key].append(self.window.get(key, 0))
        self.window.clear()


async def sampler(stats: Stats, ws: KalshiWebSocket, feed: OrderBookFeed, started: float) -> None:
    loop = asyncio.get_running_loop()
    n = 0
    while True:
        t = loop.time()
        await asyncio.sleep(1.0)
        lag = loop.time() - t - 1.0
        stats.lag_max, stats.lag_total, stats.lag_samples = (
            max(stats.lag_max, lag),
            stats.lag_total + lag,
            stats.lag_samples + 1,
        )
        stats.tick()
        stats.ws_queue_max = max(stats.ws_queue_max, ws._queue.qsize())
        stats.out_queue_max = max(stats.out_queue_max, feed._out.qsize())
        n += 1
        if n % 5 == 0:
            stats.rss.append(rss_mb())
        if n % 60 == 0:
            rate = sum(sum(v[-60:]) for v in stats.per_second.values()) / 60
            print(
                f"  [{(time.monotonic() - started) / 60:4.1f} min] "
                f"{rate:7.0f} msgs/s over last 60s, "
                f"rss {stats.rss[-1]:.0f} MB, queues ws={ws._queue.qsize()} "
                f"feed={feed._out.qsize()}, gaps={stats.seq_gaps + stats.totals[GAP]}, "
                f"resets={stats.resets}",
                flush=True,
            )


def pct(values: list[int], q: float) -> float:
    ordered = sorted(values)
    return float(ordered[min(len(ordered) - 1, int(len(ordered) * q))]) if ordered else 0.0


async def main(minutes: float, markets: int) -> None:
    settings = KalshiSettings()
    signer = KalshiSigner.from_settings(settings)
    stats = Stats()
    async with (
        KalshiRestClient(settings, signer=signer) as rest,
        KalshiWebSocket(settings, signer) as ws,
    ):
        page = await rest.markets_page(limit=1000, status="open", mve_filter="exclude")
        ranked = sorted(page.markets, key=lambda m: m.volume_24h_fp or 0, reverse=True)
        tickers = [m.ticker for m in ranked[:markets]]
        print(f"soak: {minutes} min, orderbooks for {len(tickers)} markets (top: {tickers[:3]})")
        for channel in CHANNELS:
            await ws.subscribe(channel)
        stats.rss.append(rss_mb())
        started = time.monotonic()
        async with OrderBookFeed(ws, rest, tickers) as feed:
            task = asyncio.create_task(sampler(stats, ws, feed, started))
            try:
                async with asyncio.timeout(minutes * 60):
                    async for event in feed.events():
                        key = event.kind
                        if event.kind == MESSAGE:
                            m = event.message
                            assert m is not None
                            key = m.type
                            try:
                                m.payload()
                            except Exception:
                                stats.parse_failures += 1
                            stats.bytes[key] += len(json.dumps(m.msg, separators=(",", ":")))
                            if m.sid is not None and m.seq is not None:
                                last = stats.last_seq.get(m.sid)
                                if last is not None and m.seq != last + 1:
                                    stats.seq_gaps += 1
                                stats.last_seq[m.sid] = m.seq
                        elif event.kind == RESET:
                            stats.resets += 1
                            stats.last_seq.clear()
                        elif event.kind in (RESUBSCRIBE_FAILED, RESYNC_FAILED):
                            stats.problems.append(f"{event.kind}: {event.detail}")
                        stats.totals[key] += 1
                        stats.window[key] += 1
            except TimeoutError:
                pass
            finally:
                task.cancel()
            stale = set(feed.tracker.stale)
    report(stats, time.monotonic() - started, stale)


def report(stats: Stats, seconds: float, stale: set[str]) -> None:
    print(f"\n=== soak result: {seconds / 60:.1f} min ===")
    print(f"{'channel/event':34} {'total':>9} {'mean/s':>8} {'p95/s':>7} {'max/s':>7} {'avg B':>7}")
    grand = [0] * len(next(iter(stats.per_second.values()), []))
    for key in sorted(stats.totals, key=lambda k: -stats.totals[k]):
        series = stats.per_second[key]
        for i, v in enumerate(series):
            grand[i] += v
        avg = f"{stats.bytes[key] / stats.totals[key]:.0f}" if stats.bytes[key] else "-"
        print(
            f"{key:34} {stats.totals[key]:9,} {stats.totals[key] / seconds:8.1f} "
            f"{pct(series, 0.95):7.0f} {max(series, default=0):7} {avg:>7}"
        )
    print(
        f"{'ALL':34} {sum(stats.totals.values()):9,} {sum(stats.totals.values()) / seconds:8.1f} "
        f"{pct(grand, 0.95):7.0f} {max(grand, default=0):7}"
    )
    print(
        f"\nmemory: start {stats.rss[0]:.0f} MB, max {max(stats.rss):.0f} MB, "
        f"end {stats.rss[-1]:.0f} MB"
    )
    print(f"queue depth max: ws={stats.ws_queue_max}, feed={stats.out_queue_max}")
    print(
        f"event-loop lag: mean {stats.lag_total / max(stats.lag_samples, 1) * 1000:.1f} ms, "
        f"max {stats.lag_max * 1000:.1f} ms"
    )
    print(
        f"seq gaps (non-book): {stats.seq_gaps}; book gaps: {stats.totals[GAP]}; "
        f"reconnects: {stats.resets}; parse failures: {stats.parse_failures}"
    )
    print(
        f"orderbook: {stats.totals[SNAPSHOT]} snapshots, {stats.totals[DELTA]} deltas, "
        f"stale at end: {sorted(stale)}"
    )
    print(f"problems: {stats.problems or 'none'}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=float, default=10)
    parser.add_argument("--markets", type=int, default=50)
    args = parser.parse_args()
    asyncio.run(main(args.minutes, args.markets))
