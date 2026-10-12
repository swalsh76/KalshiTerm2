"""The live stream (``/v1/stream``) as an async iterator that survives dropped connections.

``async for event in StreamClient(...)`` yields each market event and each alert **exactly once,
in order**, across reconnects:

* Two cursors, never mixed: ``cursor`` (market data) and ``alert_cursor`` (alerts). They advance
  when an event is handed to the caller, and also on the server's ``heartbeat`` and ``caught_up``
  messages, which it sends only when everything up to that point was delivered or filtered.
* A reconnect resubscribes with ``since`` / ``alerts_since`` set to those cursors, so the server
  replays exactly what was missed. As a safety net the client also drops any event whose cursor is
  not strictly greater than the last one delivered (``stats.duplicates_suppressed``).
* If the server cannot replay (``cursor_too_old``, or a cursor from the future after a restore) the
  client yields ``Resync``: state built from the stream (orderbooks) is stale, and the client
  resubscribes from now so fresh books follow. The alerts cursor is kept, so no alert is lost.
* The socket is read only when the caller asks for the next event, so a slow caller is simply a slow
  reader; one that stalls long enough is dropped by the server (1013) and resumes by cursor.
* Dropped connections, silence for ``idle_timeout`` seconds and "too many connections" reconnect
  with jittered exponential backoff (``Disconnected`` says when). Being unauthorized, a refused
  subscription and a changed certificate are fatal: they raise, they do not loop.
"""

import asyncio
import contextlib
import json
import logging
import random
import ssl
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import websockets.asyncio.client
from websockets.exceptions import ConnectionClosed, WebSocketException

from kalshiterm_client import secrets
from kalshiterm_client.client import explain_tls_failure, ssl_cause
from kalshiterm_client.errors import ConnectionProblem, InvalidRequest, Unauthorized
from kalshiterm_client.pinning import trusting
from kalshiterm_client.profiles import Profile
from kalshiterm_client.stream_events import (
    AlertEvent,
    CaughtUp,
    Connected,
    Disconnected,
    MalformedMessage,
    Resync,
    StreamEvent,
    UnknownEvent,
    WatchlistChanged,
    parse_data,
)

log = logging.getLogger(__name__)

CHANNELS = ("trades", "ticker", "orderbook", "alerts")
MARKET_CHANNELS = ("trades", "ticker", "orderbook")
PATH = "/v1/stream"


class _Dropped(Exception):
    """The connection is gone or unusable; reconnect."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass
class StreamStats:
    events: int = 0  # data events handed to the caller
    reconnects: int = 0
    resyncs: int = 0
    duplicates_suppressed: int = 0
    connected: bool = False
    cursor: int | None = None  # market data delivered up to here
    alerts_cursor: int | None = None


class StreamClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        channels: Sequence[str],
        markets: Sequence[str] = (),
        watchlist: bool = False,
        verify: ssl.SSLContext | bool = True,
        profile: Profile | None = None,
        idle_timeout: float = 45.0,
        backoff: tuple[float, float] = (0.5, 30.0),
        ping_interval: float = 20.0,
        close_timeout: float = 2.0,
        connect: Callable[..., Awaitable[Any]] = websockets.asyncio.client.connect,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        jitter: Callable[[], float] = random.random,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        unknown = [c for c in channels if c not in CHANNELS]
        if not channels or unknown:
            raise ValueError(f"channels must be some of {CHANNELS}; got {list(channels)}")
        self._market_channels = [c for c in channels if c in MARKET_CHANNELS]
        if self._market_channels and not markets and not watchlist:
            raise ValueError("market channels need markets=[...] or watchlist=True")
        parts = urlsplit(base_url)
        self._url = f"{'wss' if parts.scheme == 'https' else 'ws'}://{parts.netloc}{PATH}"
        self._token = token
        self._channels = list(dict.fromkeys(channels))
        self._markets = list(dict.fromkeys(markets))
        self._watchlist = watchlist
        self._verify = verify
        self._profile = profile
        self._idle = idle_timeout
        self._backoff = backoff
        self._ping_interval = ping_interval
        self._close_timeout = close_timeout
        self._connect = connect
        self._sleep = sleep
        self._jitter = jitter
        self._clock = clock
        self.stats = StreamStats()
        self._ws: Any = None
        self._closed = False
        self._running = False
        self._cursor: int | None = None  # None: the next subscribe starts fresh
        self._alert_cursor: int | None = None
        self._sent_since = False
        self._sent_alerts_since = False
        self._established = False
        self._cursor_errors: set[str] = set()

    @classmethod
    def for_profile(
        cls, profile: Profile, *, token: str | None = None, **kw: Any
    ) -> "StreamClient":
        """A stream for a saved server: token from the keyring (or ``KTERM_TOKEN``) and, for
        https, TLS that trusts exactly the pinned certificate."""
        token = token or secrets.get_token(profile.name)
        if not token:
            raise ConnectionProblem(
                f"no token for {profile.name}: run `kterm config token {profile.name}`"
            )
        secure = urlsplit(profile.url).scheme == "https"
        verify: ssl.SSLContext | bool = trusting(profile) if secure else False
        return cls(profile.url, token, verify=verify, profile=profile, **kw)

    # ------------------------------------------------------------------ the public loop

    def __aiter__(self) -> AsyncIterator[StreamEvent]:
        return self.events()

    async def aclose(self) -> None:
        self._closed = True
        ws, self._ws = self._ws, None
        if ws is not None:
            with contextlib.suppress(WebSocketException, OSError):
                await ws.close()

    def _delay(self, attempt: int) -> float:
        base, cap = self._backoff
        return float(min(cap, base * 2**attempt) * (0.5 + 0.5 * self._jitter()))

    async def events(self) -> AsyncIterator[StreamEvent]:
        if self._running:
            raise RuntimeError("a StreamClient has one consumer at a time")
        self._running = True
        attempt = 0
        try:
            while not self._closed:
                try:
                    async for event in self._session():
                        yield event
                    reason = "the server closed the connection"
                except _Dropped as dropped:
                    reason = dropped.reason
                self.stats.connected = False
                if self._closed:
                    return
                if self._established:
                    attempt = 0  # it worked for a while: start the backoff over
                delay = self._delay(attempt)
                attempt += 1
                self.stats.reconnects += 1
                yield Disconnected(reason=reason, retry_in=round(delay, 3))
                await self._sleep(delay)
        finally:
            self._running = False
            await self.aclose()

    # ------------------------------------------------------------------ one connection

    async def _session(self) -> AsyncIterator[StreamEvent]:
        self._established = False
        self._cursor_errors = set()
        try:
            ws = await self._connect(
                self._url,
                ssl=self._verify if self._url.startswith("wss") else None,
                additional_headers={"Authorization": f"Bearer {self._token}"},
                ping_interval=self._ping_interval,
                ping_timeout=self._ping_interval,
                open_timeout=10,
                close_timeout=self._close_timeout,  # a dead network never answers a close
                max_size=2**22,
            )
        except (OSError, TimeoutError, WebSocketException) as exc:
            cause = ssl_cause(exc)
            if cause is not None:  # a certificate problem will not go away: say so, do not loop
                raise await explain_tls_failure(self._profile, cause) from exc
            raise _Dropped(f"cannot connect to {self._url}: {exc.__class__.__name__}") from exc
        self._ws = ws
        try:
            await self._subscribe(ws)
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), self._idle)
                except TimeoutError:
                    raise _Dropped(f"nothing from the server for {self._idle:g} seconds") from None
                except ConnectionClosed as exc:
                    raise self._closed_by_server(exc) from exc
                for event in await self._handle(ws, raw):
                    yield event
        finally:
            self._ws = None
            with contextlib.suppress(WebSocketException, OSError):
                await ws.close()

    def _closed_by_server(self, exc: ConnectionClosed) -> Exception:
        code = exc.rcvd.code if exc.rcvd else None
        if code == 4401:
            return Unauthorized(
                401, "unauthorized", "the server rejected the token (revoked, expired or wrong)"
            )  # noqa: E501
        if code in (4400, 1009):
            return ConnectionProblem(
                f"the server refused this client's messages (close code {code})"
            )
        if code == 4429:
            return _Dropped("too many connections open for this user")
        if code == 1013:
            return _Dropped("the server dropped this connection for reading too slowly")
        return _Dropped(f"connection closed ({code if code is not None else 'no close code'})")

    async def _subscribe(self, ws: Any) -> None:
        """Subscribe, resuming from the cursors held. A missing cursor means "from now"."""
        message: dict[str, Any] = {
            "op": "subscribe",
            "channels": self._channels,
            "markets": self._markets,
            "watchlist": self._watchlist,
        }
        self._sent_since = bool(self._market_channels) and self._cursor is not None
        self._sent_alerts_since = "alerts" in self._channels and self._alert_cursor is not None
        if self._sent_since:
            message["since"] = str(self._cursor)
        if self._sent_alerts_since:
            message["alerts_since"] = str(self._alert_cursor)
        await ws.send(json.dumps(message))

    # ------------------------------------------------------------------ messages

    def _advance(self, market: int | None = None, alerts: int | None = None) -> None:
        """Move the resume cursors forward (never back)."""
        if market is not None and self._cursor is not None and market > self._cursor:
            self._cursor = market
        if alerts is not None and self._alert_cursor is not None and alerts > self._alert_cursor:
            self._alert_cursor = alerts
        self.stats.cursor, self.stats.alerts_cursor = self._cursor, self._alert_cursor

    async def _handle(self, ws: Any, raw: str | bytes) -> list[StreamEvent]:
        try:
            message = json.loads(raw)
        except ValueError:
            raise _Dropped("the server sent something that is not JSON") from None
        if not isinstance(message, dict):
            return []
        kind = str(message.get("type"))
        try:
            data = parse_data(message)
        except MalformedMessage as exc:
            raise ConnectionProblem(str(exc)) from exc
        if data is not None:
            return self._deliver(data)
        if kind == "subscribed":
            return [self._on_subscribed(message)]
        if kind == "heartbeat":
            self._on_heartbeat(message)
            return []
        if kind in ("caught_up", "alerts_caught_up"):
            if kind == "alerts_caught_up":
                cursor = int(message.get("alerts_cursor", 0))
                self._advance(alerts=cursor)
                return [CaughtUp(stream="alerts", cursor=cursor)]
            cursor = int(message.get("cursor", 0))
            self._advance(market=cursor)
            return [CaughtUp(stream="market", cursor=cursor)]
        if kind == "gap":
            return await self._on_gap(ws, message)
        if kind == "watchlist_changed":
            return [
                WatchlistChanged(added=message.get("added", []), removed=message.get("removed", []))
            ]
        if kind == "error":
            return await self._on_error(ws, message)
        if kind in ("pong", "unsubscribed"):
            return []
        return [UnknownEvent(original_type=kind, data=message)]

    def _deliver(self, event: Any) -> list[StreamEvent]:
        """A data event, unless it is one we already delivered."""
        if isinstance(event, AlertEvent):
            if self._alert_cursor is not None and event.alert_cursor <= self._alert_cursor:
                self.stats.duplicates_suppressed += 1
                return []
            self._alert_cursor = event.alert_cursor
        elif event.cursor is not None:
            if self._cursor is not None and event.cursor <= self._cursor:
                self.stats.duplicates_suppressed += 1
                return []
            self._cursor = event.cursor
        self.stats.events += 1
        self.stats.cursor, self.stats.alerts_cursor = self._cursor, self._alert_cursor
        return [event]

    def _on_subscribed(self, message: dict[str, Any]) -> Connected:
        self._established = True
        self.stats.connected = True
        head = int(message["cursor"])
        if self._market_channels and not self._sent_since:
            self._cursor = head  # a fresh start: live delivery begins after the head
        alerts_head = message.get("alerts_cursor")
        if "alerts" in self._channels and not self._sent_alerts_since and alerts_head is not None:
            self._alert_cursor = int(alerts_head)
        self.stats.cursor, self.stats.alerts_cursor = self._cursor, self._alert_cursor
        resumed = (self._sent_since or not self._market_channels) and (
            self._sent_alerts_since or "alerts" not in self._channels
        )
        return Connected(
            resumed=resumed,
            markets=message.get("markets", []),
            unknown=message.get("unknown", []),
            no_orderbook=message.get("no_orderbook", []),
            cursor=head,
            alerts_cursor=None if alerts_head is None else int(alerts_head),
        )

    def _on_heartbeat(self, message: dict[str, Any]) -> None:
        market = int(message["cursor"]) if self._market_channels and "cursor" in message else None
        alerts = int(message["alerts_cursor"]) if "alerts_cursor" in message else None
        self._advance(market=market, alerts=alerts)

    async def _on_gap(self, ws: Any, message: dict[str, Any]) -> list[StreamEvent]:
        """The server cannot replay: our books are stale. Start the market side over."""
        missed_after = self._cursor
        head = int(message.get("cursor", 0))
        self._cursor = None  # the next subscribe has no `since`, so fresh books are sent
        self.stats.resyncs += 1
        await self._subscribe(ws)
        return [
            Resync(reason=str(message.get("reason", "gap")), missed_after=missed_after, cursor=head)
        ]

    async def _on_error(self, ws: Any, message: dict[str, Any]) -> list[StreamEvent]:
        code = str(message.get("error", ""))
        if code == "unauthorized":
            raise Unauthorized(
                401, code, "the server rejected the token (revoked, expired or wrong)"
            )
        if code == "too_many_connections":
            raise _Dropped(
                f"too many connections open for this user (limit {message.get('limit')})"
            )
        fields = message.get("fields") or []
        if code == "invalid_parameter" and message.get("reason") == "in_the_future":
            # a cursor from the future: the server was restored from an older backup
            which = "alerts_since" if "alerts_since" in fields else "since"
            if which in self._cursor_errors:
                raise ConnectionProblem(f"the server keeps refusing {which}; cannot resume")
            self._cursor_errors.add(which)
            missed = self._alert_cursor if which == "alerts_since" else self._cursor
            if which == "alerts_since":
                self._alert_cursor = None
            else:
                self._cursor = None
            self.stats.resyncs += 1
            await self._subscribe(ws)
            return [Resync(reason=f"{which}_in_the_future", missed_after=missed, cursor=None)]
        extra = {k: v for k, v in message.items() if k not in ("type", "error")}
        detail = f" ({', '.join(map(str, fields))})" if fields else ""
        raise InvalidRequest(
            422, code, f"the server refused the subscription: {code}{detail}", **extra
        )
