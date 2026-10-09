from collections.abc import Iterator
from pathlib import Path

import keyring
import pytest
from keyring.backend import KeyringBackend
from keyring.errors import NoKeyringError, PasswordDeleteError


class MemoryKeyring(KeyringBackend):
    priority = 1

    def __init__(self) -> None:
        super().__init__()  # type: ignore[no-untyped-call]
        self.items: dict[tuple[str, str], str] = {}

    def get_password(self, service: str, username: str) -> str | None:
        return self.items.get((service, username))

    def set_password(self, service: str, username: str, password: str) -> None:
        self.items[(service, username)] = password

    def delete_password(self, service: str, username: str) -> None:
        if (service, username) not in self.items:
            raise PasswordDeleteError("not found")
        del self.items[(service, username)]


class BrokenKeyring(KeyringBackend):
    priority = 1

    def __init__(self) -> None:
        super().__init__()  # type: ignore[no-untyped-call]

    def get_password(self, service: str, username: str) -> str | None:
        raise NoKeyringError("no backend")

    def set_password(self, service: str, username: str, password: str) -> None:
        raise NoKeyringError("no backend")

    def delete_password(self, service: str, username: str) -> None:
        raise NoKeyringError("no backend")


@pytest.fixture(autouse=True)
def isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[MemoryKeyring]:
    """Every test gets its own config directory and keyring; nothing touches the real ones."""
    monkeypatch.setenv("KTERM_CONFIG_DIR", str(tmp_path / "config"))
    for var in ("KTERM_TOKEN", "KTERM_PROFILE"):
        monkeypatch.delenv(var, raising=False)
    backend = MemoryKeyring()
    previous = keyring.get_keyring()
    keyring.set_keyring(backend)
    yield backend
    keyring.set_keyring(previous)
