"""What stands between the web page (and any other local program) and the account.

The browser UI will one day be able to place orders, so the local backend is treated as a target.
Every request, HTTP or WebSocket, passes these checks before any route code runs:

* **Host allow-list.** Only ``127.0.0.1:PORT`` and ``localhost:PORT`` (this exact port). A web page
  on ``attacker.example`` whose DNS name is flipped to 127.0.0.1 ("DNS rebinding") still sends
  ``Host: attacker.example:PORT`` and is refused, even though its packets reach this socket.
* **Origin allow-list** on every request that can change something and on every WebSocket upgrade.
  The same two origins only; a missing Origin is refused too (browsers always send one there).
* **A per-session token** on every ``/api`` request, sent as ``Authorization: Bearer ...``. It lives
  in the URL fragment the user's browser is opened with (a fragment is never sent to any server),
  never in a query string or a cookie, so it cannot leak into a log, a Referer header or another
  site's request. There are **no cookies and no CORS headers at all**: another tab has nothing to
  ride on and cannot read a response. Unauthenticated ``/api`` requests get 401 whether or not the
  path exists. The WebSocket sends its token as the first message instead (see ``app``).
* **A request body is capped** (1 MiB, counted as it arrives, so a missing or lying Content-Length
  helps no one): the largest legitimate body is a saved layout, a fraction of that.
* **Response headers**: no framing (a hidden frame cannot click the order button), no sniffing, no
  Referer, a Content-Security-Policy that allows only this origin, and no caching of ``/api``.

Nothing here logs a token or an ``Authorization`` header.
"""

import contextlib
import hmac
import json
import secrets
from collections.abc import Awaitable, Callable, MutableMapping
from typing import Any

Scope = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[MutableMapping[str, Any]]]
Send = Callable[[MutableMapping[str, Any]], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]

SAFE_METHODS = frozenset({"GET", "HEAD"})
MAX_REQUEST_BYTES = 1_048_576
WS_PATH = "/api/ws"

# Wasm is allowed because the grid candidate in the 7.5 spike (Perspective) needs it; this is
# tightened in 7.5 once the grid is chosen.
CONTENT_SECURITY_POLICY = (
    "default-src 'self'; connect-src 'self'; img-src 'self' data:; "
    "style-src 'self' 'unsafe-inline'; script-src 'self' 'wasm-unsafe-eval'; "
    "frame-ancestors 'none'; base-uri 'none'; form-action 'none'"
)
SECURITY_HEADERS = {
    "x-frame-options": "DENY",
    "x-content-type-options": "nosniff",
    "referrer-policy": "no-referrer",
    "cross-origin-opener-policy": "same-origin",
    "cross-origin-resource-policy": "same-origin",
    "content-security-policy": CONTENT_SECURITY_POLICY,
}


def new_token() -> str:
    """256 random bits, URL-safe."""
    return secrets.token_urlsafe(32)


def allowed_hosts(port: int) -> frozenset[str]:
    return frozenset({f"127.0.0.1:{port}", f"localhost:{port}"})


def allowed_origins(port: int) -> frozenset[str]:
    return frozenset({f"http://127.0.0.1:{port}", f"http://localhost:{port}"})


def token_matches(presented: str | None, expected: str) -> bool:
    """Constant-time comparison; anything not a string of the right kind is a mismatch."""
    if not presented:
        return False
    return hmac.compare_digest(presented.encode(), expected.encode())


def bearer_token(headers: dict[str, str]) -> str | None:
    value = headers.get("authorization", "")
    scheme, _, token = value.partition(" ")
    if scheme.lower() != "bearer" or not token or " " in token:
        return None
    return token


def _headers(scope: Scope) -> dict[str, str]:
    return {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope["headers"]}


class BodyTooLarge(Exception):
    pass


class SecurityMiddleware:
    def __init__(self, app: ASGIApp, *, port: int, token: str) -> None:
        self.app = app
        self.hosts = allowed_hosts(port)
        self.origins = allowed_origins(port)
        self.token = token

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        headers = _headers(scope)
        refusal = self._refusal(scope, headers)
        if refusal is not None:
            await self._refuse(scope, receive, send, *refusal)
            return
        path: str = scope["path"]

        async def with_headers(message: MutableMapping[str, Any]) -> None:
            if message["type"] == "http.response.start":
                extra = dict(SECURITY_HEADERS)
                if path.startswith("/api"):
                    extra["cache-control"] = "no-store"
                existing = {k.lower() for k, _ in message["headers"]}
                message["headers"] = [
                    *message["headers"],
                    *(
                        (k.encode(), v.encode())
                        for k, v in extra.items()
                        if k.encode() not in existing
                    ),
                ]
            await send(message)

        started = False
        overflowed = False

        async def tracked_send(message: MutableMapping[str, Any]) -> None:
            nonlocal started
            if overflowed:  # we already answered 413; whatever the app says now is moot
                return
            started = started or message["type"] == "http.response.start"
            await with_headers(message)

        received = 0

        async def counted_receive() -> MutableMapping[str, Any]:
            nonlocal received, overflowed
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > MAX_REQUEST_BYTES:
                    # Answer now: the framework would turn an exception raised here into a 400.
                    if not started and not overflowed:
                        overflowed = True
                        await self._refuse(scope, receive, send, 413, "request body too large")
                    raise BodyTooLarge
            return message

        with contextlib.suppress(BodyTooLarge):  # we already answered 413
            await self.app(
                scope, counted_receive if scope["type"] == "http" else receive, tracked_send
            )

    def _refusal(self, scope: Scope, headers: dict[str, str]) -> tuple[int, str] | None:
        if headers.get("host") not in self.hosts:
            return 400, "unexpected Host header"
        is_ws = scope["type"] == "websocket"
        changes_state = is_ws or scope["method"] not in SAFE_METHODS
        if changes_state and headers.get("origin") not in self.origins:
            return 403, "unexpected Origin"
        path: str = scope["path"]
        declared = headers.get("content-length", "")
        if scope["type"] == "http" and declared.isdigit() and int(declared) > MAX_REQUEST_BYTES:
            return 413, "request body too large"
        under_api = path == "/api" or path.startswith("/api/")
        if (
            scope["type"] == "http"
            and under_api
            and not token_matches(bearer_token(headers), self.token)
        ):
            return 401, "missing or wrong session token"
        return None

    @staticmethod
    async def _refuse(scope: Scope, receive: Receive, send: Send, status: int, text: str) -> None:
        if scope["type"] == "websocket":
            # Closing before accepting makes the server answer the upgrade with HTTP 403.
            await receive()
            await send({"type": "websocket.close", "code": 1008})
            return
        body = json.dumps({"error": text}).encode()
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                    (b"cache-control", b"no-store"),
                    *((k.encode(), v.encode()) for k, v in SECURITY_HEADERS.items()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})
