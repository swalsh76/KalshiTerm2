"""Authenticated WebSocket client for Kalshi's public data channels.

This slice covers connect, subscribe/unsubscribe and message streaming. A dropped
connection raises ``KalshiWSError`` (auto-reconnect and gap recovery are separate slices).
"""

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from types import TracebackType
from typing import Any, Self
from urllib.parse import urlparse

import websockets.asyncio.client
from websockets.exceptions import ConnectionClosed

from kalshi_core.auth import KalshiSigner
from kalshi_core.config import KalshiSettings
from kalshi_core.ws_models import WsMessage

log = logging.getLogger("kalshi_core")

_RESPONSE_TYPES = frozenset({"subscribed", "ok", "unsubscribed", "error"})


class KalshiWSError(Exception):
    def __init__(self, message: str, code: int | None = None) -> None:
        super().__init__(message if code is None else f"[{code}] {message}")
        self.code = code


class KalshiWebSocket:
    def __init__(
        self,
        settings: KalshiSettings,
        signer: KalshiSigner,
        *,
        url: str | None = None,
        connect: Callable[..., Awaitable[Any]] = websockets.asyncio.client.connect,
        command_timeout: float = 10.0,
    ) -> None:
        self._url = url or settings.ws_url
        self._path = urlparse(self._url).path
        self._signer = signer
        self._connect = connect
        self._command_timeout = command_timeout
        self._conn: Any = None
        self._reader: asyncio.Task[None] | None = None
        self._next_id = 1
        self._pending: dict[int, asyncio.Future[WsMessage]] = {}
        self._queue: asyncio.Queue[WsMessage | None] = asyncio.Queue()
        self._closing = False
        if settings.is_production:
            log.warning("Kalshi WebSocket using PRODUCTION environment (read-only)")

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def connect(self) -> None:
        headers = self._signer.headers("GET", self._path)
        self._conn = await self._connect(self._url, additional_headers=headers)
        self._reader = asyncio.create_task(self._read_loop())

    async def close(self) -> None:
        self._closing = True
        if self._conn is not None:
            await self._conn.close()
        if self._reader is not None:
            await self._reader

    async def _read_loop(self) -> None:
        try:
            async for raw in self._conn:
                self._dispatch(WsMessage.model_validate(json.loads(raw)))
        except ConnectionClosed:
            pass
        finally:
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(KalshiWSError("connection closed"))
            self._pending.clear()
            self._queue.put_nowait(None)

    def _dispatch(self, message: WsMessage) -> None:
        if message.type in _RESPONSE_TYPES and message.id in self._pending:
            fut = self._pending.pop(message.id)
            if message.type == "error":
                detail = message.msg if isinstance(message.msg, dict) else {}
                fut.set_exception(
                    KalshiWSError(str(detail.get("msg", "error")), detail.get("code"))
                )
            else:
                fut.set_result(message)
        elif message.type in _RESPONSE_TYPES and message.type != "error":
            log.debug("unmatched control message: %s", message)
        else:
            self._queue.put_nowait(message)

    async def _command(self, cmd: str, params: dict[str, Any]) -> WsMessage:
        if self._conn is None:
            raise KalshiWSError("not connected")
        command_id = self._next_id
        self._next_id += 1
        fut: asyncio.Future[WsMessage] = asyncio.get_running_loop().create_future()
        self._pending[command_id] = fut
        try:
            await self._conn.send(json.dumps({"id": command_id, "cmd": cmd, "params": params}))
            return await asyncio.wait_for(fut, self._command_timeout)
        except TimeoutError:
            raise KalshiWSError(f"{cmd} timed out") from None
        except ConnectionClosed:
            raise KalshiWSError("connection closed") from None
        finally:
            self._pending.pop(command_id, None)

    async def subscribe(
        self, channel: str, *, market_tickers: list[str] | None = None, **params: Any
    ) -> int:
        """Subscribe to one channel; returns its subscription id (``sid``)."""
        body: dict[str, Any] = {"channels": [channel], **params}
        if market_tickers:
            body["market_tickers"] = market_tickers
        reply = await self._command("subscribe", body)
        return int(reply.msg["sid"])

    async def unsubscribe(self, sid: int) -> None:
        await self._command("unsubscribe", {"sids": [sid]})

    async def messages(self) -> AsyncIterator[WsMessage]:
        """Stream data messages. Ends on a deliberate close; raises if the link drops."""
        while True:
            item = await self._queue.get()
            if item is None:
                self._queue.put_nowait(None)  # keep later iterations terminating
                if self._closing:
                    return
                raise KalshiWSError("connection closed")
            yield item
