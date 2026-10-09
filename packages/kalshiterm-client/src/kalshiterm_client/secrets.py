"""Tokens live in the operating system's keyring and nowhere else.

macOS Keychain, Windows Credential Locker, or Secret Service on Linux. There is no plaintext
fallback file: on a machine without a keyring (a headless server, a CI job) the token comes
from the ``KTERM_TOKEN`` environment variable instead.
"""

import os

import keyring
from keyring.errors import KeyringError, PasswordDeleteError

SERVICE = "kalshiterm"
ENV_VAR = "KTERM_TOKEN"


class SecretError(Exception):
    """The keyring could not be used; the message says what to do instead."""


def _key(profile: str) -> str:
    return f"profile:{profile}"


def get_token(profile: str) -> str | None:
    """``KTERM_TOKEN`` wins, then the keyring; None when neither has one."""
    if token := os.environ.get(ENV_VAR):
        return token
    try:
        return keyring.get_password(SERVICE, _key(profile))
    except KeyringError as exc:
        raise SecretError(_unavailable(exc)) from exc


def has_stored_token(profile: str) -> bool:
    try:
        return keyring.get_password(SERVICE, _key(profile)) is not None
    except KeyringError:
        return False


def set_token(profile: str, token: str) -> None:
    try:
        keyring.set_password(SERVICE, _key(profile), token)
    except KeyringError as exc:
        raise SecretError(_unavailable(exc)) from exc


def delete_token(profile: str) -> None:
    try:
        keyring.delete_password(SERVICE, _key(profile))
    except PasswordDeleteError:
        pass  # there was none
    except KeyringError as exc:
        raise SecretError(_unavailable(exc)) from exc


def _unavailable(exc: Exception) -> str:
    return (
        f"the system keyring is not usable ({exc.__class__.__name__}). "
        f"Set {ENV_VAR} in the environment instead; tokens are never written to a file."
    )
