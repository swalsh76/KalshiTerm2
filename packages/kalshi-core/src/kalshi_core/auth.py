"""Request signing for the Kalshi API (Ed25519 and RSA-PSS/SHA-256)."""

import base64
import time
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from kalshi_core.config import KalshiSettings

PrivateKey = Ed25519PrivateKey | RSAPrivateKey

HEADER_KEY = "KALSHI-ACCESS-KEY"
HEADER_TIMESTAMP = "KALSHI-ACCESS-TIMESTAMP"
HEADER_SIGNATURE = "KALSHI-ACCESS-SIGNATURE"


class AuthError(Exception):
    """Credentials are missing or unusable."""


def load_private_key(path: Path) -> PrivateKey:
    """Load an unencrypted PEM private key (Ed25519 or RSA)."""
    try:
        data = path.expanduser().read_bytes()
    except OSError as exc:
        raise AuthError(f"cannot read private key file {path}: {exc.strerror}") from exc
    try:
        key = serialization.load_pem_private_key(data, password=None)
    except (ValueError, TypeError) as exc:
        raise AuthError(f"{path} is not an unencrypted PEM private key") from exc
    if not isinstance(key, Ed25519PrivateKey | RSAPrivateKey):
        raise AuthError(f"unsupported key type {type(key).__name__}; use Ed25519 or RSA")
    return key


class KalshiSigner:
    """Builds the signed headers Kalshi requires on REST requests and the WS handshake."""

    def __init__(self, key_id: str, private_key: PrivateKey) -> None:
        self._key_id = key_id
        self._key = private_key

    @classmethod
    def from_settings(cls, settings: KalshiSettings) -> "KalshiSigner":
        if not settings.key_id or settings.private_key_path is None:
            raise AuthError("KALSHI_KEY_ID and KALSHI_PRIVATE_KEY_PATH must both be set")
        return cls(settings.key_id, load_private_key(settings.private_key_path))

    def sign(self, timestamp_ms: int, method: str, path: str) -> str:
        """Base64 signature over ``timestamp + METHOD + path`` (query string excluded)."""
        message = f"{timestamp_ms}{method.upper()}{path.split('?', 1)[0]}".encode()
        if isinstance(self._key, Ed25519PrivateKey):
            raw = self._key.sign(message)
        else:
            raw = self._key.sign(
                message,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH,
                ),
                hashes.SHA256(),
            )
        return base64.b64encode(raw).decode()

    def headers(self, method: str, path: str, *, timestamp_ms: int | None = None) -> dict[str, str]:
        ts = int(time.time() * 1000) if timestamp_ms is None else timestamp_ms
        return {
            HEADER_KEY: self._key_id,
            HEADER_TIMESTAMP: str(ts),
            HEADER_SIGNATURE: self.sign(ts, method, path),
        }
