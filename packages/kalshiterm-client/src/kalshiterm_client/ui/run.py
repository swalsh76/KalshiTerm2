"""Starting the backend: a loopback socket, a fresh token, uvicorn, and the browser."""

import socket
import webbrowser
from collections.abc import Callable
from pathlib import Path

import uvicorn

from kalshiterm_client.ui.app import MAX_WS_MESSAGE_BYTES, create_app
from kalshiterm_client.ui.security import new_token

LOOPBACK = "127.0.0.1"


def bind_loopback(port: int = 0) -> socket.socket:
    """A listening socket on 127.0.0.1 only (port 0 picks a free one). There is deliberately no
    way to ask for another address."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((LOOPBACK, port))
    sock.listen(64)
    return sock


def build_url(port: int, token: str) -> str:
    """The address to open. The token is in the fragment: browsers never send it to the server,
    so it appears in no log and no Referer header."""
    return f"http://{LOOPBACK}:{port}/#token={token}"


def make_server(
    sock: socket.socket, token: str, environment: str, assets_dir: Path | None = None
) -> uvicorn.Server:
    app = create_app(
        port=sock.getsockname()[1], token=token, environment=environment, assets_dir=assets_dir
    )
    config = uvicorn.Config(
        app,
        log_level="warning",
        access_log=False,  # request lines can carry paths; nothing here should be logged at all
        ws_max_size=MAX_WS_MESSAGE_BYTES,
        server_header=False,
        date_header=False,
    )
    return uvicorn.Server(config)


def serve(
    *,
    port: int = 0,
    open_browser: bool = True,
    environment: str = "demo",
    echo: Callable[[str], None] = print,
) -> None:
    """Run until interrupted."""
    sock = bind_loopback(port)
    token = new_token()
    url = build_url(sock.getsockname()[1], token)
    server = make_server(sock, token, environment)
    echo(
        f"KalshiTerm UI ({environment}, read-only) listening on {LOOPBACK}:{sock.getsockname()[1]}"
    )
    echo(f"open: {url}")
    if open_browser:
        webbrowser.open(url)
    try:
        server.run(sockets=[sock])
    finally:
        sock.close()
