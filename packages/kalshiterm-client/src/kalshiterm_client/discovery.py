"""Finding KalshiTerm servers on the LAN (mDNS ``_kterm._tcp``).

What a server advertises (``deploy/host/advertise.sh``): its port and ``fp=`` (the SHA-256 of its
TLS certificate, 64 hex digits). Anyone on the LAN can publish such a record, so it is a
cross-check on what you were told, never a reason to trust a certificate by itself.
"""

import time
from dataclasses import dataclass
from urllib.parse import urlsplit

from zeroconf import ServiceBrowser, ServiceInfo, ServiceStateChange, Zeroconf

from kalshiterm_client.pinning import normalise_fingerprint
from kalshiterm_client.profiles import ProfileError

SERVICE_TYPE = "_kterm._tcp.local."


@dataclass(frozen=True)
class Found:
    name: str  # the advertised service name, e.g. "KalshiTerm mac-studio"
    host: str  # the server's host name, e.g. "mac-studio.local"
    addresses: tuple[str, ...]
    port: int
    fingerprint: str | None  # "AB:CD:..." or None when absent or malformed

    @property
    def url(self) -> str:
        return f"https://{self.host}:{self.port}"


def from_info(info: ServiceInfo) -> Found:
    raw = (info.properties or {}).get(b"fp")
    fingerprint = None
    if raw:
        try:
            fingerprint = normalise_fingerprint(raw.decode("ascii", "replace"))
        except ProfileError:
            fingerprint = None  # a malformed record is ignored, not trusted
    suffix = f".{SERVICE_TYPE}"
    name = info.name[: -len(suffix)] if info.name.endswith(suffix) else info.name
    return Found(
        name=name,
        host=(info.server or "").rstrip("."),
        addresses=tuple(sorted(info.parsed_addresses())),
        port=info.port or 0,
        fingerprint=fingerprint,
    )


def browse(timeout: float = 3.0, interfaces: list[str] | None = None) -> list[Found]:
    """Listen for ``timeout`` seconds and return every KalshiTerm server that answered."""
    zc = Zeroconf(interfaces=interfaces) if interfaces else Zeroconf()
    names: set[str] = set()

    def on_change(
        zeroconf: Zeroconf, service_type: str, name: str, state_change: ServiceStateChange
    ) -> None:
        if state_change is not ServiceStateChange.Removed:
            names.add(name)

    try:
        browser = ServiceBrowser(zc, SERVICE_TYPE, handlers=[on_change])
        time.sleep(timeout)
        found = []
        for name in sorted(names):
            info = zc.get_service_info(SERVICE_TYPE, name, timeout=1000)
            if info is not None:
                found.append(from_info(info))
        browser.cancel()
    finally:
        zc.close()
    return found


def matching(url: str, servers: list[Found]) -> Found | None:
    """The advertised server that a profile's address refers to (same port, same host or IP)."""
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    port = parts.port or 443
    for server in servers:
        names = {server.host.lower(), server.host.lower().removesuffix(".local"), *server.addresses}
        if server.port == port and host in names:
            return server
    return None
