"""Run the API with uvicorn, refusing to expose plaintext HTTP beyond this machine."""

import ipaddress
from pathlib import Path

import uvicorn

from kalshiterm_server.api.app import create_app
from kalshiterm_server.config import ServerSettings


class UnsafeBind(Exception):
    """The requested address would serve unencrypted HTTP to the network."""


def is_loopback(host: str) -> bool:
    """True for addresses that cannot be reached from another machine."""
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False  # a hostname, 0.0.0.0, ::, ... could be reached from the LAN


def check_bind(host: str, cert: Path | None, key: Path | None) -> None:
    if (cert is None) != (key is None):
        raise UnsafeBind("give both --tls-cert and --tls-key, or neither")
    if cert is None and not is_loopback(host):
        raise UnsafeBind(
            f"refusing to serve plaintext HTTP on {host}: bind to a loopback address, "
            "or provide --tls-cert and --tls-key"
        )
    for path in (cert, key):
        if path is not None and not path.is_file():
            raise UnsafeBind(f"{path} does not exist")


def serve(
    settings: ServerSettings, host: str, port: int, cert: Path | None, key: Path | None
) -> None:
    check_bind(host, cert, key)
    config = uvicorn.Config(
        create_app(settings),
        host=host,
        port=port,
        ssl_certfile=str(cert) if cert else None,
        ssl_keyfile=str(key) if key else None,
        log_level="info",
        server_header=False,  # do not advertise the server software
    )
    uvicorn.Server(config).run()  # installs its own SIGTERM/SIGINT handling
