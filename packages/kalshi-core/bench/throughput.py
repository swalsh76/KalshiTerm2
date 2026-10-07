"""Offline throughput benchmark for the WebSocket client and orderbook feed.

Run:  uv run python packages/kalshi-core/bench/throughput.py [--count 200000]

A fake server in a *separate process* blasts pre-built messages as fast as it can, so the
client's CPU and memory are measured in isolation. No network, no credentials.
"""

import argparse
import asyncio
import json
import os
import random
import subprocess
import sys
import time
from collections.abc import Awaitable, Callable
from typing import Any

SEQ = "987654321"  # placeholder replaced by %d in templates


def _templates(kind: str) -> list[str]:
    rng = random.Random(1)

    def tmpl(type_: str, msg: dict[str, Any], sid: int = 1) -> str:
        body = {"type": type_, "sid": sid, "seq": int(SEQ), "msg": msg}
        return json.dumps(body).replace(SEQ, "%d")

    def trade() -> str:
        return tmpl(
            "trade",
            {
                "trade_id": f"{rng.getrandbits(64):016x}-0000-0000-0000-000000000000",
                "market_ticker": f"KXBTC15M-26OCT062130-{rng.randint(0, 60)}",
                "yes_price_dollars": f"{rng.random():.4f}",
                "no_price_dollars": f"{rng.random():.4f}",
                "count_fp": f"{rng.random() * 100:.2f}",
                "taker_side": "yes",
                "is_block_trade": False,
                "ts_ms": 1791335640231,
            },
        )

    def ticker() -> str:
        return tmpl(
            "ticker",
            {
                "market_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
                "market_ticker": f"KXNBASPREAD-26OCT06BKNCHA-BKN{rng.randint(0, 40)}",
                "price_dollars": "0.2900",
                "yes_bid_dollars": "0.2800",
                "yes_ask_dollars": "0.3000",
                "yes_bid_size_fp": "120.00",
                "yes_ask_size_fp": "75.00",
                "volume_fp": "18234.00",
                "open_interest_fp": "9120.00",
                "last_trade_size_fp": "3.00",
                "ts_ms": 1791335640231,
            },
        )

    def lifecycle() -> str:
        event = rng.choice(["created", "determined", "settled", "close_date_updated"])
        msg: dict[str, Any] = {
            "market_ticker": (
                f"KXMVECROSSCATEGORY-S{rng.getrandbits(48):012x}-{rng.getrandbits(40):010x}"
            ),
            "event_type": event,
        }
        if event == "created":
            msg |= {
                "exchange_index": 1,
                "open_ts": 1791333932,
                "close_ts": 1791923100,
                "additional_metadata": {
                    "name": "",
                    "title": "yes Ted Hurst III: 15+,yes Rashid Shaheed: 25+",
                    "yes_sub_title": "",
                    "no_sub_title": "",
                    "rules_primary": "",
                    "rules_secondary": "",
                    "can_close_early": True,
                },
            }
        elif event == "determined":
            msg |= {"determination_ts": 1791333932, "result": "no", "settlement_value": "0.0000"}
        elif event == "settled":
            msg |= {"settled_ts": 1791333932}
        else:
            msg |= {"close_ts": 1791333932}
        return tmpl("multivariate_market_lifecycle", msg)

    if kind == "mixed":  # roughly the live mix: lifecycle-heavy, then trades, deltas, tickers
        makers = [lifecycle] * 4 + [trade] * 4 + [ticker] * 2
        return [rng.choice(makers)() for _ in range(5000)]
    raise ValueError(kind)


def _book_messages(count: int, markets: int, levels: int = 60) -> list[str]:
    """Snapshots for every market, then positive deltas (never negative, never a gap)."""
    rng = random.Random(2)
    out: list[str] = []
    prices = [f"{0.30 + i * 0.01:.4f}" for i in range(levels)]
    for m in range(markets):
        snap = {
            "market_ticker": f"MKT-{m}",
            "market_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
            "yes_dollars_fp": [[p, "10.00"] for p in prices],
            "no_dollars_fp": [[p, "10.00"] for p in prices],
        }
        out.append(json.dumps({"type": "orderbook_snapshot", "sid": 1, "seq": m + 1, "msg": snap}))
    for i in range(count):
        delta = {
            "market_ticker": f"MKT-{rng.randrange(markets)}",
            "market_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
            "price_dollars": rng.choice(prices),
            "delta_fp": f"{rng.random() * 5 + 0.01:.2f}",
            "side": rng.choice(["yes", "no"]),
            "ts_ms": 1791335640231,
        }
        out.append(
            json.dumps({"type": "orderbook_delta", "sid": 1, "seq": markets + i + 1, "msg": delta})
        )
    return out


# ---------------------------------------------------------------- server (separate process)
async def serve(kind: str, count: int, markets: int, compress: bool) -> None:
    from websockets.asyncio.server import ServerConnection
    from websockets.asyncio.server import serve as ws_serve

    if kind == "book":
        prebuilt = _book_messages(count, markets)
        messages = prebuilt
    else:
        templates = _templates(kind)
        messages = [templates[i % len(templates)] % (i + 1) for i in range(count)]

    async def handler(ws: ServerConnection) -> None:
        cmd = json.loads(await ws.recv())
        channel = cmd["params"]["channels"][0]
        await ws.send(
            json.dumps(
                {"id": cmd["id"], "type": "subscribed", "msg": {"channel": channel, "sid": 1}}
            )
        )
        for message in messages:
            await ws.send(message)
        await ws.close()

    compression = "deflate" if compress else None
    async with ws_serve(handler, "127.0.0.1", 0, max_size=None, compression=compression) as server:
        print(f"PORT {server.sockets[0].getsockname()[1]}", flush=True)
        await asyncio.Future()


# ---------------------------------------------------------------- client measurements
def rss_mb() -> float:
    out = subprocess.run(
        ["ps", "-o", "rss=", "-p", str(os.getpid())], capture_output=True, text=True
    )
    return int(out.stdout.strip()) / 1024


class LoopLag:
    """Max and mean event-loop delay, sampled every 10 ms."""

    def __init__(self) -> None:
        self.max = 0.0
        self.total = 0.0
        self.samples = 0
        self._task: asyncio.Task[None] | None = None

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            start = loop.time()
            await asyncio.sleep(0.01)
            lag = loop.time() - start - 0.01
            self.max = max(self.max, lag)
            self.total += lag
            self.samples += 1

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        assert self._task is not None
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)

    @property
    def mean(self) -> float:
        return self.total / max(self.samples, 1)


def start_server(kind: str, count: int, markets: int = 50) -> tuple[subprocess.Popen[str], int]:
    proc = subprocess.Popen(
        [
            sys.executable,
            __file__,
            "--serve",
            kind,
            "--count",
            str(count),
            "--markets",
            str(markets),
            *(["--compress"] if os.environ.get("BENCH_COMPRESS") else []),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    line = proc.stdout.readline()
    assert line.startswith("PORT"), line
    return proc, int(line.split()[1])


def make_client(port: int) -> Any:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from kalshi_core.auth import KalshiSigner
    from kalshi_core.config import KalshiSettings
    from kalshi_core.ws import KalshiWebSocket

    os.environ["KALSHI_ENV"] = "demo"
    signer = KalshiSigner("bench", Ed25519PrivateKey.generate())
    return KalshiWebSocket(
        KalshiSettings(), signer, url=f"ws://127.0.0.1:{port}/trade-api/ws/v2", auto_reconnect=False
    )


Result = dict[str, Any]


async def measure(
    kind: str,
    count: int,
    consume: Callable[[Any], Awaitable[int]],
    *,
    channel: str = "trade",
    markets: int = 50,
) -> Result:
    from kalshi_core.ws import KalshiWSError  # noqa: F401  (imported for side-effect-free typing)

    proc, port = start_server(kind, count, markets)
    try:
        ws = make_client(port)
        await ws.connect()
        rss0, lag = rss_mb(), LoopLag()
        lag.start()
        await ws.subscribe(channel)
        cpu0, t0 = time.process_time(), time.perf_counter()
        received = await consume(ws)
        wall, cpu = time.perf_counter() - t0, time.process_time() - cpu0
        await lag.stop()
        result: Result = {
            "compressed": bool(ws._conn.protocol.extensions),
            "msgs": received,
            "wall_s": wall,
            "msgs_per_s": received / wall,
            "us_per_msg": wall / received * 1e6,
            "cpu_pct": cpu / wall * 100,
            "rss_mb": rss_mb(),
            "rss_growth_mb": rss_mb() - rss0,
            "lag_mean_ms": lag.mean * 1000,
            "lag_max_ms": lag.max * 1000,
        }
        await ws.close()
        return result
    finally:
        proc.kill()


async def drain(ws: Any, typed: bool) -> int:
    from kalshi_core.ws import KalshiWSError

    n = 0
    try:
        async for message in ws.messages():
            if typed:
                message.payload()
            n += 1
    except KalshiWSError:
        pass
    return n


async def scenario_book(count: int) -> Result:
    from kalshi_core.config import KalshiSettings
    from kalshi_core.orderbook import DELTA, GAP, OrderBookFeed
    from kalshi_core.rest import KalshiRestClient

    markets = 50
    stats = {"deltas": 0, "gaps": 0}

    async def consume(ws: Any) -> int:
        from kalshi_core.ws import KalshiWSError

        async with KalshiRestClient(KalshiSettings()) as rest:
            feed = OrderBookFeed(ws, rest, [f"MKT-{m}" for m in range(markets)])
            # The server already subscribed us on "orderbook_delta"; attach the tracker to it.
            feed._sid = 1
            feed.tracker.begin_subscription(1, feed._tickers)
            feed._pump = asyncio.create_task(feed._run_pump())
            try:
                async for event in feed.events():
                    if event.kind == DELTA:
                        stats["deltas"] += 1
                    elif event.kind == GAP:
                        stats["gaps"] += 1
            except KalshiWSError:
                pass
            await feed.close()
        return stats["deltas"]

    result = await measure("book", count, consume, channel="orderbook_delta", markets=markets)
    result["gaps"] = stats["gaps"]
    return result


async def scenario_backlog(count: int, consumed: int = 2000) -> Result:
    """A consumer that handles ~1000 msgs/s while the server sends at full speed."""
    holder: dict[str, Any] = {}

    async def consume(ws: Any) -> int:
        n = 0
        async for _ in ws.messages():
            n += 1
            await asyncio.sleep(0.001)
            if n >= consumed:
                break
        await asyncio.sleep(1.0)  # let the reader finish ingesting everything
        holder["queued"] = ws._queue.qsize()
        return n

    result = await measure("mixed", count, consume)
    result["queued"] = holder["queued"]
    result["bytes_per_queued_msg"] = (
        result["rss_growth_mb"] * 1024 * 1024 / max(holder["queued"], 1)
    )
    return result


def show(name: str, r: Result) -> None:
    print(f"\n== {name} ==")
    for key, value in r.items():
        print(f"  {key:22} {value:,.1f}" if isinstance(value, float) else f"  {key:22} {value:,}")


async def main(count: int) -> None:
    show(
        "1. read + dispatch only (mixed messages)",
        await measure("mixed", count, lambda ws: drain(ws, False)),
    )
    show("2. read + typed payload parse", await measure("mixed", count, lambda ws: drain(ws, True)))
    show("3. orderbook feed, 50 markets, applying deltas", await scenario_book(count))
    show("4. slow consumer (~1000 msgs/s) vs full-speed server", await scenario_backlog(count))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", choices=["mixed", "book"])
    parser.add_argument("--count", type=int, default=200_000)
    parser.add_argument("--markets", type=int, default=50)
    parser.add_argument("--compress", action="store_true", help="server uses permessage-deflate")
    args = parser.parse_args()
    if args.serve:
        asyncio.run(serve(args.serve, args.count, args.markets, args.compress))
    else:
        asyncio.run(main(args.count))
