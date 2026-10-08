"""The server's self-signed TLS certificate (PLAN §8.4).

Clients do not trust a certificate authority: they pin this certificate's SHA-256
fingerprint on first use and treat any change as an event to confirm. So the certificate is
simple and strict: ECDSA P-256, valid for a year, not a CA, good for server authentication
only, and valid exactly for the names clients will use (plus loopback, so local checks can
verify it too).
"""

import hashlib
import ipaddress
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

CERT_FILE = "tls_cert.pem"
KEY_FILE = "tls_key.pem"
VALID_DAYS = 365
LOOPBACK_NAMES = ("localhost",)
LOOPBACK_IPS = ("127.0.0.1", "::1")

_LABEL = r"[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?"
HOSTNAME = re.compile(rf"^{_LABEL}(\.{_LABEL})*$")


class TlsError(Exception):
    """A name or file that cannot be used."""


@dataclass(frozen=True, slots=True)
class CertInfo:
    fingerprint: str  # SHA-256 of the DER certificate, "AB:CD:..." (what clients pin)
    dns_names: tuple[str, ...]
    ip_addresses: tuple[str, ...]
    not_before: datetime
    not_after: datetime
    key_type: str


def fingerprint_of(der: bytes) -> str:
    digest = hashlib.sha256(der).hexdigest().upper()
    return ":".join(digest[i : i + 2] for i in range(0, len(digest), 2))


def normalise_names(hosts: list[str], ips: list[str]) -> tuple[list[str], list[str]]:
    """Validated, de-duplicated names: loopback always, ``<host>.local`` for short hostnames."""
    dns: list[str] = list(LOOPBACK_NAMES)
    for raw in hosts:
        name = raw.strip().lower().rstrip(".")
        if len(name) > 253 or not HOSTNAME.fullmatch(name):
            raise TlsError(
                f"{raw!r} is not a valid host name (no wildcards, spaces or underscores)"
            )
        short = "." not in name and name not in LOOPBACK_NAMES
        for candidate in (name, f"{name}.local" if short else None):
            if candidate and candidate not in dns:
                dns.append(candidate)
    addresses: list[str] = list(LOOPBACK_IPS)
    for raw in ips:
        try:
            ip = ipaddress.ip_address(raw.strip())
        except ValueError as exc:
            raise TlsError(f"{raw!r} is not an IP address") from exc
        if ip.is_unspecified or ip.is_multicast:
            raise TlsError(f"{raw!r} cannot be a server address")
        if str(ip) not in addresses:
            addresses.append(str(ip))
    return dns, addresses


def generate(hosts: list[str], ips: list[str], now: datetime | None = None) -> tuple[bytes, bytes]:
    """A new certificate and private key, both PEM."""
    dns, addresses = normalise_names(hosts, ips)
    issued = (now or datetime.now(UTC)).replace(microsecond=0)
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "KalshiTerm server")])
    sans = [x509.DNSName(d) for d in dns] + [
        x509.IPAddress(ipaddress.ip_address(a)) for a in addresses
    ]
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)  # self-signed
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(issued - timedelta(minutes=5))  # tolerate a slightly slow client clock
        .not_valid_after(issued + timedelta(days=VALID_DAYS))
        .add_extension(x509.SubjectAlternativeName(sans), critical=False)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .sign(key, hashes.SHA256())
    )
    return (
        certificate.public_bytes(serialization.Encoding.PEM),
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ),
    )


def describe(cert_pem: bytes) -> CertInfo:
    try:
        cert = x509.load_pem_x509_certificate(cert_pem)
        san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    except (ValueError, x509.ExtensionNotFound) as exc:
        raise TlsError("not a usable PEM certificate") from exc
    key = cert.public_key()
    key_type = f"ECDSA {key.curve.name}" if isinstance(key, ec.EllipticCurvePublicKey) else "other"
    return CertInfo(
        fingerprint_of(cert.public_bytes(serialization.Encoding.DER)),
        tuple(san.get_values_for_type(x509.DNSName)),
        tuple(str(ip) for ip in san.get_values_for_type(x509.IPAddress)),
        cert.not_valid_before_utc,
        cert.not_valid_after_utc,
        key_type,
    )


def _write(path: Path, content: bytes, mode: int) -> None:
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    try:
        os.write(fd, content)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.chmod(tmp, mode)
    os.replace(tmp, path)


def write_certificate(
    secrets_dir: Path, hosts: list[str], ips: list[str], now: datetime | None = None
) -> CertInfo:
    """Create (or replace) the certificate and key in ``secrets_dir``."""
    cert_pem, key_pem = generate(hosts, ips, now)
    _write(secrets_dir / KEY_FILE, key_pem, 0o600)  # the key first: never a cert without its key
    _write(secrets_dir / CERT_FILE, cert_pem, 0o644)  # the certificate is public
    return describe(cert_pem)


def user_names(info: CertInfo) -> tuple[list[str], list[str]]:
    """The explicitly requested names of an existing certificate (without the automatic ones)."""
    hosts = [
        d
        for d in info.dns_names
        if d not in LOOPBACK_NAMES and not (d.endswith(".local") and d[:-6] in info.dns_names)
    ]
    ips = [a for a in info.ip_addresses if a not in LOOPBACK_IPS]
    return hosts, ips
