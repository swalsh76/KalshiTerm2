"""The real API over TLS on a loopback port, and a TCP proxy that can cut it off on command."""

import asyncio
import contextlib
import socket
import threading
import time
from pathlib import Path
from types import TracebackType
from typing import Any

import uvicorn
from kalshiterm_server import tls


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class LiveServer:
    """``with LiveServer(app, certs_dir) as server:`` -> ``server.url``, ``server.pem``."""

    def __init__(self, app: Any, certs: Path) -> None:
        certs.mkdir(parents=True, exist_ok=True)
        tls.write_certificate(certs, ["localhost"], ["127.0.0.1"])
        self.pem = (certs / tls.CERT_FILE).read_bytes()
        self.port = free_port()
        self.url = f"https://127.0.0.1:{self.port}"
        self._server = uvicorn.Server(
            uvicorn.Config(
                app,
                host="127.0.0.1",
                port=self.port,
                log_level="warning",
                ssl_certfile=str(certs / tls.CERT_FILE),
                ssl_keyfile=str(certs / tls.KEY_FILE),
            )  # fmt: skip
        )
        self._thread = threading.Thread(target=self._server.run, daemon=True)

    def __enter__(self) -> "LiveServer":
        self._thread.start()
        deadline = time.monotonic() + 20
        while not self._server.started and time.monotonic() < deadline:
            time.sleep(0.05)
        assert self._server.started, "the API did not start"
        return self

    def __exit__(
        self, kind: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self._server.should_exit = True
        self._thread.join(15)


class Proxy:
    """Relays TCP between a client and the server, so a test can break the link the way a network
    does: ``sever()`` drops every open connection, ``down`` refuses new ones, ``blackhole(True)``
    keeps connections open but delivers nothing (a dead network, not a closed socket)."""

    def __init__(self, target_port: int) -> None:
        self.target = target_port
        self.port = 0
        self.down = False
        self._flow: asyncio.Event | None = None  # set while bytes may pass
        self._links: set[asyncio.Task[None]] = set()
        self._writers: set[asyncio.StreamWriter] = set()
        self._main: asyncio.Task[None] | None = None
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._main = self._loop.create_task(self._serve())
        with contextlib.suppress(asyncio.CancelledError):  # stop() cancels it: the normal end
            self._loop.run_until_complete(self._main)

    async def _serve(self) -> None:
        self._flow = asyncio.Event()
        self._flow.set()
        server = await asyncio.start_server(self._accept, "127.0.0.1", 0)
        self.port = server.sockets[0].getsockname()[1]
        self._ready.set()
        async with server:
            await server.serve_forever()

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self.down:
            writer.transport.abort()
            return
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection(
                "127.0.0.1", self.target
            )
        except OSError:
            writer.transport.abort()
            return
        self._writers |= {writer, upstream_writer}
        pumps = [
            asyncio.create_task(self._pump(reader, upstream_writer)),
            asyncio.create_task(self._pump(upstream_reader, writer)),
        ]
        self._links |= set(pumps)
        await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
        for pump in pumps:
            pump.cancel()
        for w in (writer, upstream_writer):
            w.transport.abort()
            self._writers.discard(w)

    async def _pump(self, source: asyncio.StreamReader, sink: asyncio.StreamWriter) -> None:
        while chunk := await source.read(65536):
            assert self._flow is not None
            await self._flow.wait()  # while blackholed, hold the bytes: up, but silent
            sink.write(chunk)
            await sink.drain()

    def start(self) -> "Proxy":
        self._thread.start()
        assert self._ready.wait(10), "the proxy did not start"
        return self

    def blackhole(self, on: bool) -> None:
        """Keep every connection open but stop delivering bytes (on), or resume (off)."""
        assert self._flow is not None
        self._loop.call_soon_threadsafe(self._flow.clear if on else self._flow.set)

    def sever(self) -> None:
        """Drop every open connection at once."""

        def cut() -> None:
            for w in list(self._writers):
                w.transport.abort()

        self._loop.call_soon_threadsafe(cut)

    def stop(self) -> None:
        self.sever()
        if self._main is not None:
            self._loop.call_soon_threadsafe(self._main.cancel)
        self._thread.join(5)
