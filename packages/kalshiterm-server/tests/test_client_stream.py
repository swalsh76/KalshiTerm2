"""StreamClient against the real server, over real TLS, through a proxy that breaks the link.

The point of these tests: while rows keep being committed, the connection is cut again and again,
and every committed event still reaches the caller exactly once, in order (BUILD-LOG §10.6)."""

import asyncio
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from kalshiterm_client.book import BookTracker
from kalshiterm_client.client import ServerClient
from kalshiterm_client.errors import CertificateChanged, Unauthorized
from kalshiterm_client.pinning import fingerprint_of_pem
from kalshiterm_client.profiles import Profile, ProfileStore
from kalshiterm_client.stream import StreamClient
from kalshiterm_client.stream_events import (
    AlertEvent,
    BookDeltaEvent,
    BookSnapshotEvent,
    Connected,
    Disconnected,
    Resync,
    StreamEvent,
    TickerEvent,
    TradeEvent,
)
from kalshiterm_server import tls
from kalshiterm_server.api.app import create_app
from kalshiterm_server.config import ServerSettings
from live_server import LiveServer, Proxy
from test_api_stream import A, B, Writer, prepare
from test_api_stream_alerts import Alerts

pytestmark = pytest.mark.db

FAST = {"backoff": (0.05, 0.2), "idle_timeout": 10.0, "ping_interval": 30.0}


@dataclass
class Rig:
    db_url: str
    server: LiveServer
    proxy: Proxy
    profile: Profile  # points at the proxy and pins the server's certificate
    tokens: dict[str, str]
    writer: Writer
    alerts: Alerts

    def stream(self, who: str = "alice", **kw: Any) -> StreamClient:
        return StreamClient.for_profile(self.profile, token=self.tokens[who], **{**FAST, **kw})

    def direct(self, who: str = "alice") -> ServerClient:
        """A REST client straight to the server (not through the proxy)."""
        return ServerClient.for_profile(ProfileStore().get("direct"), token=self.tokens[who])


@pytest.fixture
def rig_factory(
    migrated_db_url: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Callable[..., Any]:
    monkeypatch.setenv("KTERM_CONFIG_DIR", str(tmp_path / "config"))
    prepared = asyncio.run(prepare(migrated_db_url))

    @contextmanager
    def make(**settings: Any) -> Iterator[Rig]:
        app = create_app(ServerSettings(db_url=migrated_db_url, **settings))
        with LiveServer(app, tmp_path / "certs") as server:
            proxy = Proxy(server.port).start()
            try:
                store = ProfileStore()
                store.add("rig", f"https://127.0.0.1:{proxy.port}", replace=True)
                store.save_pin("rig", server.pem, fingerprint_of_pem(server.pem))
                store.add("direct", server.url, replace=True)
                store.save_pin("direct", server.pem, fingerprint_of_pem(server.pem))
                yield Rig(
                    migrated_db_url, server, proxy, store.get("rig"), prepared["tokens"],
                    Writer(migrated_db_url, prepared["ids"]), Alerts(migrated_db_url),
                )  # fmt: skip
            finally:
                proxy.stop()

    return make


class Consumer:
    """Reads a stream in the background and lets the test wait for what it expects."""

    def __init__(self, stream: StreamClient) -> None:
        self.stream = stream
        self.events: list[StreamEvent] = []
        self.task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        async for event in self.stream:
            self.events.append(event)

    async def until(self, ready: Callable[[list[StreamEvent]], bool], within: float = 25) -> None:
        async with asyncio.timeout(within):
            while not ready(self.events):
                if self.task.done() and self.task.exception():
                    raise self.task.exception()  # type: ignore[misc]
                await asyncio.sleep(0.02)

    def of(self, kind: type[Any]) -> list[Any]:
        return [e for e in self.events if isinstance(e, kind)]

    async def stop(self) -> None:
        await self.stream.aclose()
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)


async def connected(consumer: Consumer, n: int = 1) -> None:
    await consumer.until(lambda e: sum(isinstance(x, Connected) for x in e) >= n)


# ------------------------------------------------------------------ exactly-once under cuts


async def test_market_events_survive_repeated_disconnects_exactly_once_and_in_order(
    rig_factory: Callable[..., Any],
) -> None:
    with rig_factory() as rig:
        consumer = Consumer(rig.stream(channels=["trades", "ticker"], markets=[A]))
        await connected(consumer)
        expected: list[int] = []
        for i in range(36):
            seqs = await asyncio.to_thread(rig.writer.batch, trades=(A, A, B), tickers=(A, B))
            expected += seqs["trades"][:2] + seqs["tickers"][:1]  # A's rows only
            if i in (6, 14, 22, 30):
                rig.proxy.sever()
            await asyncio.sleep(0.03)
        await consumer.until(
            lambda e: (
                len([x for x in e if isinstance(x, TradeEvent | TickerEvent)]) >= len(expected)
            )
        )
        await consumer.stop()
        got = [e.cursor for e in consumer.events if isinstance(e, TradeEvent | TickerEvent)]
        assert got == sorted(expected) and len(set(got)) == len(got)  # every row, once, in order
        assert consumer.stream.stats.duplicates_suppressed == 0  # the server never repeats itself
        assert consumer.stream.stats.reconnects >= 3
        later = consumer.of(Connected)[1:]
        assert later and all(c.resumed for c in later) and not consumer.of(Resync)
        assert all(e.type != "disconnected" for e in consumer.events[:2])


async def test_alerts_survive_disconnects_exactly_once_and_only_the_owners_arrive(
    rig_factory: Callable[..., Any],
) -> None:
    with rig_factory() as rig:
        consumer = Consumer(rig.stream(channels=["alerts"]))
        await connected(consumer)
        mine: list[int] = []
        for i in range(30):
            ids = await asyncio.to_thread(rig.alerts.add, "alice", "bob")
            mine.append(ids[0])
            if i in (5, 12, 19, 25):
                rig.proxy.sever()
            await asyncio.sleep(0.03)
        await consumer.until(
            lambda e: len([x for x in e if isinstance(x, AlertEvent)]) >= len(mine)
        )
        await consumer.stop()
        got = [e.id for e in consumer.of(AlertEvent)]
        assert got == mine and consumer.stream.stats.duplicates_suppressed == 0
        assert consumer.stream.stats.reconnects >= 3 and not consumer.of(Resync)
        assert [e.alert_cursor for e in consumer.of(AlertEvent)] == mine


async def test_market_data_and_alerts_together_keep_their_cursors_apart(
    rig_factory: Callable[..., Any],
) -> None:
    with rig_factory() as rig:
        consumer = Consumer(rig.stream(channels=["trades", "alerts"], markets=[A]))
        await connected(consumer)
        trade_cursors: list[int] = []
        alert_ids: list[int] = []
        for i in range(24):
            seqs = await asyncio.to_thread(rig.writer.batch, trades=(A,))
            trade_cursors += seqs["trades"]
            alert_ids.append((await asyncio.to_thread(rig.alerts.add, "alice"))[0])
            if i in (4, 11, 18):
                rig.proxy.sever()
            await asyncio.sleep(0.03)
        await consumer.until(
            lambda e: (
                sum(isinstance(x, TradeEvent) for x in e) >= len(trade_cursors)
                and sum(isinstance(x, AlertEvent) for x in e) >= len(alert_ids)
            )
        )
        await consumer.stop()
        assert [e.cursor for e in consumer.of(TradeEvent)] == trade_cursors
        assert [e.id for e in consumer.of(AlertEvent)] == alert_ids


async def test_an_orderbook_rebuilt_from_the_stream_matches_the_servers_book_after_cuts(
    rig_factory: Callable[..., Any],
) -> None:
    with rig_factory() as rig:
        await asyncio.to_thread(
            rig.writer.batch, snapshots=((A, 1, 1, [(400_000, 1000), (390_000, 500)]),)
        )
        consumer = Consumer(rig.stream(channels=["orderbook"], markets=[A]))
        await connected(consumer)
        seq = 1
        for i in range(30):
            seq += 1
            price = 380_000 + 10_000 * (i % 5)
            await asyncio.to_thread(
                rig.writer.batch,
                deltas=((A, 1, seq, price, 100 + 10 * (i % 3)),),
            )
            if i in (5, 13, 21):
                rig.proxy.sever()
            await asyncio.sleep(0.03)
        last = seq
        await consumer.until(
            lambda e: [x.seq for x in e if isinstance(x, BookDeltaEvent)][-1:] == [last]
        )
        await consumer.stop()
        tracker = BookTracker()
        for event in consumer.events:
            if isinstance(event, Resync):
                tracker.reset()
            elif isinstance(event, BookSnapshotEvent | BookDeltaEvent):
                tracker.apply(event)
        book = tracker.book(A)
        assert book is not None and book.inconsistent is False
        async with rig.direct() as api:
            rest = await api.orderbook(A)
        assert book.yes == {level.price: level.size for level in rest.yes}
        assert rest.complete and consumer.stream.stats.reconnects >= 2


# ------------------------------------------------------------------ resync


async def test_when_the_server_cannot_replay_the_client_resyncs_and_the_book_is_fresh(
    rig_factory: Callable[..., Any],
) -> None:
    with rig_factory(stream_catchup_horizon=3) as rig:
        await asyncio.to_thread(rig.writer.batch, snapshots=((A, 1, 1, [(400_000, 1000)]),))
        consumer = Consumer(rig.stream(channels=["trades", "orderbook"], markets=[A]))
        await connected(consumer)
        rig.proxy.down = True  # the network is gone...
        rig.proxy.sever()
        missed: list[int] = []
        for i in range(8):  # ...while far more rows than the replay horizon are committed
            seqs = await asyncio.to_thread(
                rig.writer.batch, trades=(A, A), deltas=((A, 1, 2 + i, 410_000, 100),)
            )
            missed += seqs["trades"]
        await asyncio.to_thread(rig.writer.batch, snapshots=((A, 2, 1, [(450_000, 777)]),))
        rig.proxy.down = False  # ...and then it comes back
        await consumer.until(
            lambda e: any(isinstance(x, Resync) for x in e) and len(consumer.of(Connected)) >= 3
        )
        [resync] = consumer.of(Resync)
        assert resync.reason == "cursor_too_old" and resync.missed_after is not None
        connects = consumer.of(Connected)
        assert (
            connects[0].resumed is False
            and connects[1].resumed is True
            and connects[2].resumed is False
        )
        live = (await asyncio.to_thread(rig.writer.batch, trades=(A,)))["trades"][0]
        await consumer.until(
            lambda e: any(isinstance(x, TradeEvent) and x.cursor == live for x in e)
        )
        await consumer.stop()
        delivered = {e.cursor for e in consumer.of(TradeEvent)}
        assert live in delivered and not delivered & set(missed)  # the missed ones need REST
        tracker = BookTracker()
        for event in consumer.events:
            if isinstance(event, Resync):
                tracker.reset()
            elif isinstance(event, BookSnapshotEvent | BookDeltaEvent):
                tracker.apply(event)
        book = tracker.book(A)
        assert book is not None and book.yes == {Decimal("0.450000"): Decimal("7.77")}
        assert consumer.stream.stats.resyncs == 1


# ------------------------------------------------------------------ fatal conditions


async def test_a_wrong_token_raises_unauthorized_and_does_not_retry(
    rig_factory: Callable[..., Any],
) -> None:
    with rig_factory() as rig:
        stream = rig.stream(channels=["trades"], markets=[A])
        stream._token = "kt_1_" + "x" * 43  # a token the server has never issued
        with pytest.raises(Unauthorized, match="rejected the token"):
            async with asyncio.timeout(15):
                async for _ in stream:
                    pass
        assert stream.stats.reconnects == 0


async def test_a_changed_server_certificate_raises_and_is_not_retried(
    rig_factory: Callable[..., Any], tmp_path: Path
) -> None:
    with rig_factory() as rig:
        other = tmp_path / "other"
        other.mkdir()
        tls.write_certificate(other, ["localhost"], ["127.0.0.1"])  # a different certificate
        old = (other / tls.CERT_FILE).read_bytes()
        store = ProfileStore()
        store.save_pin("rig", old, fingerprint_of_pem(old))  # we pinned the OLD one
        stream = StreamClient.for_profile(
            store.get("rig"), token=rig.tokens["alice"], channels=["trades"], markets=[A], **FAST
        )
        with pytest.raises(CertificateChanged, match="HAS CHANGED"):
            async with asyncio.timeout(15):
                async for _ in stream:
                    pass
        assert stream.stats.reconnects == 0


# ------------------------------------------------------------------ silence


async def test_heartbeats_keep_a_quiet_connection_alive_and_silence_is_noticed(
    rig_factory: Callable[..., Any],
) -> None:
    with rig_factory(stream_heartbeat_seconds=0.2) as rig:
        consumer = Consumer(rig.stream(channels=["trades"], markets=[A], idle_timeout=3.0))
        await connected(consumer)
        await asyncio.sleep(4.5)  # no data at all, longer than the idle timeout
        assert consumer.stream.stats.reconnects == 0 and not consumer.of(Disconnected)
        rig.proxy.blackhole(True)  # the network dies without closing anything
        await consumer.until(lambda e: any(isinstance(x, Disconnected) for x in e), within=10)
        [drop] = consumer.of(Disconnected)
        assert "nothing from the server for 3 seconds" in drop.reason
        rig.proxy.blackhole(False)
        await connected(consumer, 2)
        await consumer.stop()
        assert consumer.of(Connected)[1].resumed is True
