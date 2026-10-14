"""The FastAPI application behind ``kterm ui``.

Slice 7.1 is the skeleton: the security layer (``security``), a health / session pair, a keep-alive
POST (the later idle timeout needs a mutating call to test the Origin rule against), a WebSocket
that authenticates in its first message, and the page (the built front end if there is one,
otherwise a short placeholder). The session is **read-only**: there are no order endpoints at all
in this slice.
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib import metadata
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import TypeAdapter, ValidationError
from starlette.websockets import WebSocketDisconnect

from kalshiterm_client.ui import messages as m
from kalshiterm_client.ui.data_routes import DataAccess, register_error_handlers, router
from kalshiterm_client.ui.security import WS_PATH, SecurityMiddleware, token_matches
from kalshiterm_client.ui.stream_hub import Connection, StreamFactory, StreamHub

ASSETS_DIR = Path(__file__).resolve().parent.parent / "ui_assets"
AUTH_TIMEOUT_SECONDS = 5.0
WS_AUTH_FAILED = 4401
MAX_WS_MESSAGE_BYTES = 65_536

PLACEHOLDER = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>KalshiTerm</title>
<style>body{font:16px system-ui;max-width:42em;margin:4em auto;padding:0 1em;line-height:1.5}
code{background:#8882;padding:.1em .3em;border-radius:3px}</style></head>
<body><h1>KalshiTerm backend is running</h1>
<p>The browser front end has not been built yet, so there is nothing to show here.
The backend answers on <code>/api</code> with the session token this page was opened with.</p>
<p>Mode: <strong>read-only</strong>.</p></body></html>
"""


def version() -> str:
    try:
        return metadata.version("kalshiterm-client")
    except metadata.PackageNotFoundError:
        return "dev"


def create_app(
    *,
    port: int,
    token: str,
    environment: str,
    assets_dir: Path | None = None,
    data: DataAccess | None = None,
    stream_factory: StreamFactory | None = None,
) -> FastAPI:
    """Build the app. ``port`` and ``token`` feed the security layer; ``environment`` is what
    ``KALSHI_ENV`` says (``demo`` or ``production``)."""
    # No interactive docs or schema endpoint: a smaller surface. The schema for the generated
    # TypeScript types comes from ``create_app(...).openapi()`` in a build script.
    hub = StreamHub(stream_factory)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        await hub.close()
        await app.state.data.close()

    app = FastAPI(
        title="KalshiTerm local backend",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    assets = assets_dir if assets_dir is not None else ASSETS_DIR
    app.state.data = data or DataAccess(None)
    app.state.hub = hub
    register_error_handlers(app)
    app.include_router(router)

    @app.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "version": version()}

    @app.get("/api/session")
    async def session() -> dict[str, Any]:
        return {"mode": "read-only", "environment": environment, "version": version()}

    @app.post("/api/session/touch")
    async def touch() -> dict[str, bool]:
        return {"ok": True}

    @app.websocket(WS_PATH)
    async def stream(websocket: WebSocket) -> None:
        await websocket.accept()
        try:
            first = await asyncio.wait_for(websocket.receive_json(), AUTH_TIMEOUT_SECONDS)
        except (TimeoutError, ValueError, WebSocketDisconnect, KeyError, RuntimeError):
            await _close(websocket, WS_AUTH_FAILED)
            return
        presented = first.get("token") if isinstance(first, dict) else None
        if not (
            isinstance(first, dict)
            and first.get("type") == "auth"
            and isinstance(presented, str)
            and token_matches(presented, token)
        ):
            await _close(websocket, WS_AUTH_FAILED)
            return
        await websocket.send_json(
            m.Hello(mode="read-only", environment=environment).model_dump(mode="json")
        )
        await serve_connection(websocket, hub)

    if (assets / "index.html").is_file():
        app.mount("/", StaticFiles(directory=assets, html=True), name="ui")
    else:

        @app.get("/", response_class=HTMLResponse, include_in_schema=False)
        async def placeholder() -> str:
            return PLACEHOLDER

    app.add_middleware(SecurityMiddleware, port=port, token=token)
    return app


_CLIENT_MESSAGE: TypeAdapter[Any] = TypeAdapter(m.ClientMessage)
WS_TRY_AGAIN_LATER = 1013


def _handle(hub: StreamHub, conn: Connection, raw: Any) -> None:
    """One message from the browser. A bad one gets an error message; the connection stays."""
    try:
        message = _CLIENT_MESSAGE.validate_python(raw)
    except ValidationError:
        conn.send(m.ErrorMessage(error="bad message"))
        return
    try:
        if isinstance(message, m.Ping):
            conn.send(m.Pong())
        elif isinstance(message, m.Subscribe):
            hub.subscribe(conn, message.channels, message.tickers)
        elif isinstance(message, m.Unsubscribe):
            hub.unsubscribe(conn, message.channels, message.tickers)
        elif isinstance(message, m.SetAlerts):
            hub.set_alerts(conn, message.enabled)
        else:
            conn.send(m.ErrorMessage(error="already authenticated"))
    except ValueError as exc:
        conn.send(m.ErrorMessage(error=str(exc)))


async def _write(websocket: WebSocket, conn: Connection) -> None:
    """Send queued messages to the page."""
    try:
        while True:
            await websocket.send_json(await conn.queue.get())
    except (WebSocketDisconnect, RuntimeError):
        return


async def _read(websocket: WebSocket, hub: StreamHub, conn: Connection) -> None:
    """Handle what the page sends until it goes away."""
    try:
        while True:
            try:
                raw = await websocket.receive_json()
            except ValueError:
                conn.send(m.ErrorMessage(error="not JSON"))
                continue
            _handle(hub, conn, raw)
    except (WebSocketDisconnect, KeyError, RuntimeError):
        return


async def serve_connection(websocket: WebSocket, hub: StreamHub) -> None:
    """An authenticated page: read its commands, write its events, and drop it if it cannot keep
    up. A page that stops reading blocks our send, so the drop cannot wait for the writer: it is
    triggered by the queue limit and tears the other two tasks down."""
    conn = Connection()
    hub.attach(conn)
    reader = asyncio.create_task(_read(websocket, hub, conn))
    writer = asyncio.create_task(_write(websocket, conn))
    dropped = asyncio.create_task(conn.dropped.wait())
    try:
        done, _ = await asyncio.wait({reader, writer, dropped}, return_when=asyncio.FIRST_COMPLETED)
        was_dropped = dropped in done
    finally:
        for task in (reader, writer, dropped):
            task.cancel()
        for task in (reader, writer, dropped):
            with contextlib.suppress(asyncio.CancelledError):
                await task
        hub.detach(conn)
    if was_dropped:
        await _close(websocket, WS_TRY_AGAIN_LATER)


async def _close(websocket: WebSocket, code: int) -> None:
    with contextlib.suppress(RuntimeError):  # already closed by the peer
        await websocket.close(code=code)
