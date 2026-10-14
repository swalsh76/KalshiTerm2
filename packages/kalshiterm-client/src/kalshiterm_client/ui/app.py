"""The FastAPI application behind ``kterm ui``.

Slice 7.1 is the skeleton: the security layer (``security``), a health / session pair, a keep-alive
POST (the later idle timeout needs a mutating call to test the Origin rule against), a WebSocket
that authenticates in its first message, and the page (the built front end if there is one,
otherwise a short placeholder). The session is **read-only**: there are no order endpoints at all
in this slice.
"""

import asyncio
import contextlib
from importlib import metadata
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette.websockets import WebSocketDisconnect

from kalshiterm_client.ui.security import WS_PATH, SecurityMiddleware, token_matches

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
    *, port: int, token: str, environment: str, assets_dir: Path | None = None
) -> FastAPI:
    """Build the app. ``port`` and ``token`` feed the security layer; ``environment`` is what
    ``KALSHI_ENV`` says (``demo`` or ``production``)."""
    # No interactive docs or schema endpoint: a smaller surface. The schema for the generated
    # TypeScript types comes from ``create_app(...).openapi()`` in a build script.
    app = FastAPI(title="KalshiTerm local backend", docs_url=None, redoc_url=None, openapi_url=None)
    assets = assets_dir if assets_dir is not None else ASSETS_DIR

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
            {"type": "hello", "mode": "read-only", "environment": environment}
        )
        try:
            while True:
                message = await websocket.receive_json()
                if isinstance(message, dict) and message.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})
                else:
                    await websocket.send_json({"type": "error", "error": "unknown message"})
        except (WebSocketDisconnect, ValueError, KeyError, RuntimeError):
            return

    if (assets / "index.html").is_file():
        app.mount("/", StaticFiles(directory=assets, html=True), name="ui")
    else:

        @app.get("/", response_class=HTMLResponse)
        async def placeholder() -> str:
            return PLACEHOLDER

    app.add_middleware(SecurityMiddleware, port=port, token=token)
    return app


async def _close(websocket: WebSocket, code: int) -> None:
    with contextlib.suppress(RuntimeError):  # already closed by the peer
        await websocket.close(code=code)
