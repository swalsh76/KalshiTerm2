"""A tiny HTTPS (or plain) server that records every request, plus certificates to serve."""

import ipaddress
import socket
import ssl
import tempfile
import threading
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID


def make_certificate(
    ips: tuple[str, ...] = ("127.0.0.1",),
    dns: tuple[str, ...] = ("localhost",),
    *,
    expired: bool = False,
) -> tuple[bytes, bytes]:
    """A self-signed certificate and its key, as PEM."""
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "kterm test")])
    now = datetime.now(UTC)
    start, end = (
        (now - timedelta(days=30), now - timedelta(days=1))
        if expired
        else (now - timedelta(minutes=5), now + timedelta(days=30))
    )
    names: list[x509.GeneralName] = [x509.DNSName(d) for d in dns]
    names += [x509.IPAddress(ipaddress.ip_address(ip)) for ip in ips]
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(start)
        .not_valid_after(end)
        .add_extension(x509.SubjectAlternativeName(names), critical=False)
        .sign(key, hashes.SHA256())
    )
    return (
        cert.public_bytes(serialization.Encoding.PEM),
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ),
    )


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class _Quiet(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def handle_error(self, request: Any, client_address: Any) -> None:
        pass  # clients that reject our certificate abort the handshake; that is expected


class FakeServer:
    def __init__(self, port: int | None = None, cert: tuple[bytes, bytes] | None = None) -> None:
        self.port = port or free_port()
        self.cert = cert
        self.requests: list[tuple[str, str | None]] = []  # (path, Authorization header)
        self.status = 200
        self.body: Any = {"time": "2026-10-09T12:00:00+00:00", "problems": []}
        self._server: _Quiet | None = None
        self._dir = tempfile.TemporaryDirectory()

    @property
    def url(self) -> str:
        return f"{'https' if self.cert else 'http'}://127.0.0.1:{self.port}"

    def start(self) -> "FakeServer":
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                outer.requests.append((self.path, self.headers.get("Authorization")))
                import json

                payload = json.dumps(outer.body).encode()
                self.send_response(outer.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                if outer.status == 429:
                    self.send_header("Retry-After", "42")
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args: Any) -> None:
                pass

        server = _Quiet(("127.0.0.1", self.port), Handler)
        if self.cert:
            directory = Path(self._dir.name)
            (directory / "cert.pem").write_bytes(self.cert[0])
            (directory / "key.pem").write_bytes(self.cert[1])
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(directory / "cert.pem", directory / "key.pem")
            server.socket = context.wrap_socket(server.socket, server_side=True)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self._server = server
        return self

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
