"""Starting the backend: a loopback socket, a fresh token, uvicorn, and the browser."""

import socket
import webbrowser
from collections.abc import Callable, Sequence
from pathlib import Path

import uvicorn
from kalshi_core.config import KalshiSettings

from kalshiterm_client.client import ServerClient
from kalshiterm_client.errors import ConnectionProblem
from kalshiterm_client.profiles import ProfileError, ProfileStore
from kalshiterm_client.risk_config import load_policy
from kalshiterm_client.stream import StreamClient
from kalshiterm_client.ui.app import MAX_WS_MESSAGE_BYTES, create_app
from kalshiterm_client.ui.data_routes import DataAccess
from kalshiterm_client.ui.security import new_token
from kalshiterm_client.ui.stream_hub import StreamFactory, StreamLike
from kalshiterm_client.ui.trading import TradingSession

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


def connect_server(profile_name: str | None = None) -> tuple[DataAccess, StreamFactory | None]:
    """The KalshiTerm server this session reads from, if one is set up.

    A missing profile or token is not an error: the UI still starts (trading does not need the
    server) and the data panels say why they are empty."""
    try:
        profile = ProfileStore().resolve(profile_name)
    except ProfileError as exc:
        return DataAccess(None, reason=str(exc)), None
    try:
        client = ServerClient.for_profile(profile)
    except ConnectionProblem as exc:  # no token in the keyring
        return DataAccess(None, profile=profile.name, url=profile.url, reason=str(exc)), None

    def factory(channels: Sequence[str], tickers: Sequence[str]) -> StreamLike:
        return StreamClient.for_profile(profile, channels=list(channels), markets=list(tickers))

    access = DataAccess(client, profile=profile.name, url=profile.url, owns_client=True)
    return access, factory


def make_server(
    sock: socket.socket,
    token: str,
    environment: str,
    assets_dir: Path | None = None,
    *,
    data: DataAccess | None = None,
    stream_factory: StreamFactory | None = None,
    trading: TradingSession | None = None,
) -> uvicorn.Server:
    app = create_app(
        port=sock.getsockname()[1],
        token=token,
        environment=environment,
        assets_dir=assets_dir,
        data=data,
        stream_factory=stream_factory,
        trading=trading,
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
    profile: str | None = None,
    echo: Callable[[str], None] = print,
) -> None:
    """Run until interrupted."""
    sock = bind_loopback(port)
    token = new_token()
    url = build_url(sock.getsockname()[1], token)
    data, stream_factory = connect_server(profile)
    trading = TradingSession(settings=KalshiSettings(), policy_loader=load_policy)
    server = make_server(
        sock, token, environment, data=data, stream_factory=stream_factory, trading=trading
    )
    echo(
        f"KalshiTerm UI ({environment}) listening on {LOOPBACK}:{sock.getsockname()[1]}; "
        "trading is OFF until you switch it on in the page"
    )
    echo(f"server: {data.profile or 'none'} ({'configured' if data.client else data.reason})")
    echo(f"open: {url}")
    if open_browser:
        webbrowser.open(url)
    try:
        server.run(sockets=[sock])
    finally:
        sock.close()
