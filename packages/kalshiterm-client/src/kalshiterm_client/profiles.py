"""Connection profiles: which server to talk to. Tokens are not here (see ``secrets``)."""

import ipaddress
import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from platformdirs import user_config_path

CONFIG_DIR_ENV = "KTERM_CONFIG_DIR"
PROFILE_ENV = "KTERM_PROFILE"
FILE_NAME = "profiles.json"
NAME = re.compile(r"[a-z0-9][a-z0-9_-]{0,31}")
TOKEN = re.compile(r"kt_[0-9]+_[A-Za-z0-9_-]{20,}")
LOOPBACK_NAMES = {"localhost"}


class ProfileError(Exception):
    """A mistake the user can fix; the message says how."""


@dataclass(frozen=True)
class Profile:
    name: str
    url: str
    fingerprint: str | None = None  # SHA-256 of the pinned certificate, "AB:CD:..."


def config_dir() -> Path:
    override = os.environ.get(CONFIG_DIR_ENV)
    return Path(override) if override else user_config_path("kalshiterm", appauthor=False)


def profiles_path() -> Path:
    return config_dir() / FILE_NAME


def pin_path(name: str) -> Path:
    """Where a profile's pinned certificate (public, not secret) is kept."""
    return config_dir() / "certs" / f"{check_name(name)}.pem"


def check_name(name: str) -> str:
    if not NAME.fullmatch(name):
        raise ProfileError(
            f"bad profile name {name!r}: use 1-32 lowercase letters, digits, '-' or '_'"
        )
    return name


def check_token(token: str) -> str:
    if not TOKEN.fullmatch(token):
        raise ProfileError("that does not look like a KalshiTerm token (kt_<id>_<secret>)")
    return token


def _is_loopback(host: str) -> bool:
    if host in LOOPBACK_NAMES:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def normalise_url(url: str) -> str:
    """``https://host[:port]``; plain http is allowed only for a loopback address, so a token
    can never cross the LAN in the clear."""
    try:
        parts = urlsplit(url.strip())
        host, port = parts.hostname, parts.port
    except ValueError as exc:
        raise ProfileError(f"not a valid URL: {url!r}") from exc
    if parts.scheme not in ("http", "https") or not host:
        raise ProfileError(f"not a valid URL: {url!r} (expected https://host:port)")
    if parts.path not in ("", "/") or parts.query or parts.fragment or parts.username:
        raise ProfileError("give only the server's address, e.g. https://mac-studio:8700")
    if parts.scheme == "http" and not _is_loopback(host):
        raise ProfileError("plain http is only allowed for this machine; use https://")
    shown = f"[{host}]" if ":" in host else host
    return f"{parts.scheme}://{shown}" + (f":{port}" if port else "")


class ProfileStore:
    """``profiles.json`` in the user's config directory: ``{"default": ..., "profiles": {...}}``."""

    def __init__(self, path: Path | None = None) -> None:
        self.path = path or profiles_path()

    def _read(self) -> dict[str, object]:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {"default": None, "profiles": {}}
        except (OSError, ValueError) as exc:
            raise ProfileError(f"cannot read {self.path}: {exc}") from exc
        if not isinstance(data, dict) or not isinstance(data.get("profiles"), dict):
            raise ProfileError(f"{self.path} is not a profile file")
        return data

    def _write(self, data: dict[str, object]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.path.parent, prefix=".profiles-")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, sort_keys=True)
                f.write("\n")
            os.chmod(tmp, 0o600)
            os.replace(tmp, self.path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def names(self) -> list[str]:
        return sorted(self._read()["profiles"])  # type: ignore[call-overload,no-any-return]

    def default_name(self) -> str | None:
        value = self._read().get("default")
        return value if isinstance(value, str) else None

    def get(self, name: str) -> Profile:
        profiles: dict[str, dict[str, str]] = self._read()["profiles"]  # type: ignore[assignment]
        if name not in profiles:
            known = ", ".join(sorted(profiles)) or "none yet"
            raise ProfileError(f"no profile named {name!r} (have: {known}); see `kterm config add`")
        entry = profiles[name]
        return Profile(name, entry["url"], entry.get("fingerprint"))

    def add(self, name: str, url: str, *, replace: bool = False) -> Profile:
        check_name(name)
        data = self._read()
        profiles: dict[str, dict[str, str]] = data["profiles"]  # type: ignore[assignment]
        if name in profiles and not replace:
            raise ProfileError(f"profile {name!r} already exists (use --replace to change it)")
        address = normalise_url(url)
        previous = profiles.get(name)
        keep_pin = previous is not None and previous["url"] == address and "fingerprint" in previous
        profiles[name] = {"url": address}
        if keep_pin and previous is not None:
            profiles[name]["fingerprint"] = previous["fingerprint"]
        if data.get("default") is None:
            data["default"] = name  # the first profile is the default
        self._write(data)
        if previous is not None and not keep_pin:
            pin_path(name).unlink(missing_ok=True)  # a different server needs a fresh decision
        return self.get(name)

    def remove(self, name: str) -> None:
        data = self._read()
        profiles: dict[str, object] = data["profiles"]  # type: ignore[assignment]
        if name not in profiles:
            raise ProfileError(f"no profile named {name!r}")
        del profiles[name]
        pin_path(name).unlink(missing_ok=True)
        if data.get("default") == name:
            data["default"] = sorted(profiles)[0] if profiles else None
        self._write(data)

    def save_pin(self, name: str, pem: bytes, fingerprint: str) -> None:
        """Pin ``pem`` for a profile: the file first, then the fingerprint that vouches for it."""
        data = self._read()
        profiles: dict[str, dict[str, str]] = data["profiles"]  # type: ignore[assignment]
        if name not in profiles:
            raise ProfileError(f"no profile named {name!r}")
        path = pin_path(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_bytes(pem)
        os.replace(tmp, path)
        profiles[name]["fingerprint"] = fingerprint
        self._write(data)

    def use(self, name: str) -> None:
        self.get(name)
        data = self._read()
        data["default"] = name
        self._write(data)

    def resolve(self, requested: str | None = None) -> Profile:
        """``--profile``, then ``KTERM_PROFILE``, then the default profile."""
        name = requested or os.environ.get(PROFILE_ENV) or self.default_name()
        if not name:
            raise ProfileError(
                "no server configured: run `kterm config add NAME --url https://...`"
            )
        return self.get(name)
