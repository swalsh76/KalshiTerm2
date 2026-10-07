import base64
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from kalshi_core.auth import (
    HEADER_KEY,
    HEADER_SIGNATURE,
    HEADER_TIMESTAMP,
    AuthError,
    KalshiSigner,
    load_private_key,
)
from kalshi_core.config import KalshiSettings

PATH = "/trade-api/v2/portfolio/balance"
TS = 1_700_000_000_000


def pem(key: Ed25519PrivateKey | rsa.RSAPrivateKey | ec.EllipticCurvePrivateKey) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def test_ed25519_signature_verifies() -> None:
    key = Ed25519PrivateKey.generate()
    sig = KalshiSigner("kid", key).sign(TS, "GET", PATH)
    key.public_key().verify(base64.b64decode(sig), f"{TS}GET{PATH}".encode())


def test_query_string_excluded_and_method_uppercased() -> None:
    key = Ed25519PrivateKey.generate()
    signer = KalshiSigner("kid", key)
    with_query = signer.sign(TS, "get", PATH + "?limit=5&cursor=x")
    key.public_key().verify(base64.b64decode(with_query), f"{TS}GET{PATH}".encode())


def test_headers() -> None:
    signer = KalshiSigner("kid-123", Ed25519PrivateKey.generate())
    h = signer.headers("GET", PATH, timestamp_ms=TS)
    assert h[HEADER_KEY] == "kid-123"
    assert h[HEADER_TIMESTAMP] == str(TS)
    assert h[HEADER_SIGNATURE] == signer.sign(TS, "GET", PATH)


def test_headers_default_timestamp_is_ms() -> None:
    h = KalshiSigner("k", Ed25519PrivateKey.generate()).headers("GET", PATH)
    assert int(h[HEADER_TIMESTAMP]) > 1_600_000_000_000


def test_load_ed25519_private_key(tmp_path: Path) -> None:
    key = Ed25519PrivateKey.generate()
    f = tmp_path / "k.pem"
    f.write_bytes(pem(key))
    assert isinstance(load_private_key(f), Ed25519PrivateKey)


def test_rsa_keys_are_rejected_with_a_clear_message(tmp_path: Path) -> None:
    f = tmp_path / "rsa.pem"
    f.write_bytes(pem(rsa.generate_private_key(public_exponent=65537, key_size=2048)))
    with pytest.raises(AuthError, match="only Ed25519 keys are supported"):
        load_private_key(f)


def test_load_rejects_unsupported_and_garbage(tmp_path: Path) -> None:
    ec_file = tmp_path / "ec.pem"
    ec_file.write_bytes(pem(ec.generate_private_key(ec.SECP256R1())))
    with pytest.raises(AuthError, match="unsupported key type"):
        load_private_key(ec_file)
    bad = tmp_path / "bad.pem"
    bad.write_text("not a key")
    with pytest.raises(AuthError, match="PEM private key"):
        load_private_key(bad)
    with pytest.raises(AuthError, match="cannot read"):
        load_private_key(tmp_path / "missing.pem")


def test_from_settings_requires_credentials(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    for name in ("KALSHI_KEY_ID", "KALSHI_PRIVATE_KEY_PATH"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(AuthError, match="must both be set"):
        KalshiSigner.from_settings(KalshiSettings())


def test_from_settings_loads_key(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    f = tmp_path / "k.pem"
    f.write_bytes(pem(Ed25519PrivateKey.generate()))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("KALSHI_KEY_ID", "kid")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PATH", str(f))
    signer = KalshiSigner.from_settings(KalshiSettings())
    assert signer.headers("GET", PATH)[HEADER_KEY] == "kid"
