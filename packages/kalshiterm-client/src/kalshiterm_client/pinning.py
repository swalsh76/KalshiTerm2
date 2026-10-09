"""Trust on first use: fetch a server's certificate, show it, pin it, and from then on trust
exactly that certificate (and nothing else) for the profile."""

import hashlib
import re
import ssl
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlsplit

from cryptography import x509
from cryptography.hazmat.primitives import serialization

from kalshiterm_client.profiles import Profile, ProfileError, pin_path


class PinError(ProfileError):
    """The pinned certificate cannot be used as it stands."""


@dataclass(frozen=True)
class CertDetails:
    fingerprint: str  # SHA-256 of the DER certificate, "AB:CD:..."
    dns_names: list[str]
    ip_addresses: list[str]
    not_before: datetime
    not_after: datetime


def fingerprint_of_der(der: bytes) -> str:
    digest = hashlib.sha256(der).hexdigest().upper()
    return ":".join(digest[i : i + 2] for i in range(0, len(digest), 2))


def normalise_fingerprint(text: str) -> str:
    """Accept a fingerprint with or without colons, in any case; return ``AB:CD:...``."""
    plain = re.sub(r"[\s:]", "", text).upper()
    if not re.fullmatch(r"[0-9A-F]{64}", plain):
        raise ProfileError("a fingerprint is 64 hex digits (SHA-256), e.g. AB:CD:... as printed")
    return ":".join(plain[i : i + 2] for i in range(0, 64, 2))


def parse(pem: bytes) -> x509.Certificate:
    try:
        return x509.load_pem_x509_certificate(pem)
    except ValueError as exc:
        raise PinError("that is not a valid certificate") from exc


def fingerprint_of_pem(pem: bytes) -> str:
    return fingerprint_of_der(parse(pem).public_bytes(serialization.Encoding.DER))


def describe(pem: bytes) -> CertDetails:
    cert = parse(pem)
    try:
        san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        dns = list(san.get_values_for_type(x509.DNSName))
        ips = [str(ip) for ip in san.get_values_for_type(x509.IPAddress)]
    except x509.ExtensionNotFound:
        dns, ips = [], []
    return CertDetails(
        fingerprint_of_pem(pem),
        dns,
        ips,
        cert.not_valid_before_utc,
        cert.not_valid_after_utc,
    )


def host_port(url: str) -> tuple[str, int]:
    parts = urlsplit(url)
    assert parts.hostname  # profile URLs are validated when stored
    return parts.hostname, parts.port or 443


def fetch_certificate(url: str, timeout: float = 5.0) -> bytes:
    """The certificate the server presents, WITHOUT trusting it (for showing and pinning)."""
    host, port = host_port(url)
    try:
        return ssl.get_server_certificate((host, port), timeout=timeout).encode()
    except (OSError, ssl.SSLError) as exc:
        raise ProfileError(f"cannot reach {host}:{port} to read its certificate: {exc}") from exc


def pinned_pem(profile: Profile) -> bytes:
    """The pinned certificate, checked against the fingerprint recorded in the profile."""
    if not profile.fingerprint:
        raise PinError(
            f"{profile.name} has no trusted certificate yet: "
            f"run `kterm --profile {profile.name} server trust`"
        )
    path = pin_path(profile.name)
    try:
        pem = path.read_bytes()
    except OSError as exc:
        raise PinError(
            f"the pinned certificate file {path} is missing: re-run `kterm server trust`"
        ) from exc
    if fingerprint_of_pem(pem) != profile.fingerprint:
        raise PinError(
            f"the pinned certificate file {path} does not match the fingerprint recorded for "
            f"{profile.name}: it was altered. Re-run `kterm server trust`"
        )
    return pem


def trusting(profile: Profile) -> ssl.SSLContext:
    """A TLS context that accepts exactly the pinned certificate (and checks name and expiry)."""
    pem = pinned_pem(profile)
    return ssl.create_default_context(cadata=pem.decode())


def local_time(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%d")
