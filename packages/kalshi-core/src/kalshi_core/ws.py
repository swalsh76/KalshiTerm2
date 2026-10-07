"""Authenticated WebSocket client for Kalshi's public data channels.

Handles connect, subscribe/unsubscribe, message streaming and automatic reconnect.
After a reconnect every active subscription is re-sent and a synthetic ``reconnected``
message is yielded so consumers know data may have been missed. Orderbook gap recovery
is a separate slice.
"""

import asyncio
import contextlib
import json
import logging
import random
from collections.abc import AsyncIterator, Awaitable, Callable
from types import TracebackType
from typing import Any, Self
from urllib.parse import urlparse

import websockets.asyncio.client
from websockets.exceptions import ConnectionClosed, WebSocketException

from kalshi_core.auth import KalshiSigner
from kalshi_core.config import KalshiSettings
from kalshi_core.ws_models import WsMessage

log = logging.getLogger("kalshi_core")

_RESPONSE_TYPES = frozenset({"subscribed", "ok", "unsubscribed", "error"})
RECONNECTED = "reconnected"


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
        auto_reconnect: bool = True,
        reconnect_base: float = 0.5,
        reconnect_max: float = 30.0,
        max_reconnect_attempts: int | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._url = url or settings.ws_url
        self._path = urlparse(self._url).path
        self._signer = signer
        self._connect = connect
        self._command_timeout = command_timeout
        self._auto_reconnect = auto_reconnect
        self._reconnect_base = reconnect_base
        self._reconnect_max = reconnect_max
        self._max_attempts = max_reconnect_attempts
        self._sleep = sleep
        self._conn: Any = None
        self._supervisor: asyncio.Task[None] | None = None
        self._next_id = 1
        self._pending: dict[int, asyncio.Future[WsMessage]] = {}
        self._queue: asyncio.Queue[WsMessage | None] = asyncio.Queue()
        self._subs: dict[int, tuple[str, dict[str, Any]]] = {}
        self._held: list[WsMessage] | None = None  # data held back while resubscribing
        self._connected = False
        self._reconnecting = False
        self._closing = False
        self._lost: str | None = None
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

    @property
    def subscriptions(self) -> list[tuple[int, str, dict[str, Any]]]:
        """Active subscriptions as ``(sid, channel, params)``. Sids change on reconnect."""
        return [(sid, channel, dict(body)) for sid, (channel, body) in self._subs.items()]

    async def _open(self) -> Any:
        # Fresh headers every time: the signed timestamp must be current.
        headers = self._signer.headers("GET", self._path)
        return await self._connect(self._url, additional_headers=headers)

    async def connect(self) -> None:
        self._conn = await self._open()
        self._connected = True
        self._supervisor = asyncio.create_task(self._supervise())

    async def close(self) -> None:
        self._closing = True
        if self._supervisor is not None and self._reconnecting:
            self._supervisor.cancel()
        elif self._conn is not None:
            await self._conn.close()
        if self._supervisor is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._supervisor

    async def _supervise(self) -> None:
        reader: asyncio.Task[None] | None = None
        first = True
        try:
            while True:
                if not first and not await self._reopen():
                    break
                reader = asyncio.create_task(self._read_loop(self._conn))
                if not first:
                    await self._restore_subscriptions()
                first = False
                await reader
                self._connected = False
                self._fail_pending()
                if self._closing or not self._auto_reconnect:
                    break
                log.warning("Kalshi WebSocket connection lost; reconnecting")
        finally:
            if reader is not None and not reader.done():
                reader.cancel()
            self._connected = False
            self._fail_pending()
            self._queue.put_nowait(None)

    async def _reopen(self) -> bool:
        """Reconnect with backoff. False if closing or attempts are exhausted."""
        self._reconnecting = True
        try:
            attempt = 0
            while True:
                if self._closing:
                    return False
                if self._max_attempts is not None and attempt >= self._max_attempts:
                    self._lost = "reconnect attempts exhausted"
                    return False
                await self._sleep(self._backoff(attempt))
                attempt += 1
                if self._closing:
                    return False
                try:
                    self._conn = await self._open()
                except (OSError, WebSocketException, TimeoutError) as exc:
                    log.warning("Kalshi WebSocket reconnect attempt %d failed: %s", attempt, exc)
                    continue
                self._connected = True
                return True
        finally:
            self._reconnecting = False

    def _backoff(self, attempt: int) -> float:
        delay = min(self._reconnect_max, self._reconnect_base * (2.0**attempt))
        return delay * random.uniform(0.5, 1.0)  # noqa: S311 - jitter, not security

    async def _restore_subscriptions(self) -> None:
        """Resubscribe, then release data in order: ``reconnected`` event first."""
        self._held = []
        try:
            completed = await self._resubscribe()
        finally:
            held, self._held = self._held, None
        if completed:
            for message in held:
                self._queue.put_nowait(message)

    async def _resubscribe(self) -> bool:
        old = list(self._subs.items())
        self._subs.clear()
        resubscribed: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for index, (old_sid, (channel, body)) in enumerate(old):
            try:
                sid = await self._subscribe(channel, body)
            except KalshiWSError as exc:
                if exc.code is None:  # transport trouble: keep the rest for the next round
                    self._subs.update(dict(old[index:]))
                    return False
                log.warning("resubscribe to %s failed: %s", channel, exc)
                failed.append({"channel": channel, "old_sid": old_sid, "error": str(exc)})
            else:
                resubscribed.append({"channel": channel, "sid": sid, "old_sid": old_sid})
        event = {"resubscribed": resubscribed, "failed": failed}
        self._queue.put_nowait(WsMessage(type=RECONNECTED, msg=event))
        return True

    async def _read_loop(self, conn: Any) -> None:
        try:
            async for raw in conn:
                try:
                    message = WsMessage.model_validate(json.loads(raw))
                except ValueError:
                    log.warning("dropping unparseable WebSocket message: %.200s", raw)
                    continue
                self._dispatch(message)
        except ConnectionClosed:
            pass

    def _fail_pending(self) -> None:
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(KalshiWSError("connection closed"))
        self._pending.clear()

    def _dispatch(self, message: WsMessage) -> None:
        if message.type in _RESPONSE_TYPES and message.id in self._pending:
            fut = self._pending.pop(message.id)
            if message.type == "error":
                detail = message.msg if isinstance(message.msg, dict) else {}
                text, code = str(detail.get("msg", "error")), detail.get("code")
                fut.set_exception(KalshiWSError(text, code))
            else:
                fut.set_result(message)
        elif message.type in _RESPONSE_TYPES and message.type != "error":
            log.debug("unmatched control message: %s", message)
        elif self._held is not None:
            self._held.append(message)
        else:
            self._queue.put_nowait(message)

    async def _command(self, cmd: str, params: dict[str, Any]) -> WsMessage:
        if self._conn is None or not self._connected:
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

    async def _subscribe(self, channel: str, body: dict[str, Any]) -> int:
        reply = await self._command("subscribe", {"channels": [channel], **body})
        sid = int(reply.msg["sid"])
        self._subs[sid] = (channel, body)
        return sid

    async def subscribe(
        self, channel: str, *, market_tickers: list[str] | None = None, **params: Any
    ) -> int:
        """Subscribe to one channel; returns its subscription id (``sid``)."""
        body: dict[str, Any] = dict(params)
        if market_tickers:
            body["market_tickers"] = market_tickers
        return await self._subscribe(channel, body)

    async def unsubscribe(self, sid: int) -> None:
        await self._command("unsubscribe", {"sids": [sid]})
        self._subs.pop(sid, None)

    async def messages(self) -> AsyncIterator[WsMessage]:
        """Stream data messages (plus ``reconnected`` events).

        Ends on a deliberate close; raises if the link is lost for good.
        """
        while True:
            item = await self._queue.get()
            if item is None:
                self._queue.put_nowait(None)  # keep later iterations terminating
                if self._closing:
                    return
                raise KalshiWSError(self._lost or "connection closed")
            yield item
