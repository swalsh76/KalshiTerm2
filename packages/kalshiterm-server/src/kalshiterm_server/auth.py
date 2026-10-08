"""API users, tokens and the failed-login throttle.

A token looks like ``kt_<id>_<secret>``. The secret is 256 bits from ``secrets``; only its
SHA-256 is stored. Verification fetches the one row named by ``<id>``, compares hashes in
constant time, and gives the same answer (``None``) for every kind of failure so a guesser
learns nothing about which part was wrong.
"""

import hashlib
import hmac
import re
import secrets
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

ROLES = ("read", "admin")
USER_NAME = re.compile(r"^[a-z][a-z0-9_.-]{0,62}$")
TOKEN_FORMAT = re.compile(r"^kt_([0-9]{1,9})_([A-Za-z0-9_-]{43})$")  # ASCII digits only
MAX_HEADER = 256  # a real token is ~55 characters; anything longer is not one
LAST_USED_EVERY = timedelta(minutes=1)  # do not write to the database on every request
_DUMMY_HASH = hashlib.sha256(b"no such token").digest()


class AuthError(Exception):
    """An administrative mistake (bad name, unknown user...), reported to the operator."""


@dataclass(frozen=True, slots=True)
class Principal:
    """Who is calling. Deliberately carries no part of the token."""

    user_id: int
    user: str
    role: str
    token_id: int


def hash_secret(secret: str) -> bytes:
    return hashlib.sha256(secret.encode()).digest()


def new_secret() -> str:
    return secrets.token_urlsafe(32)  # 43 URL-safe characters


def format_token(token_id: int, secret: str) -> str:
    return f"kt_{token_id}_{secret}"


def parse_token(value: str) -> tuple[int, str] | None:
    match = TOKEN_FORMAT.fullmatch(value)
    return (int(match.group(1)), match.group(2)) if match else None


def bearer_token(header: str | None) -> str | None:
    """The token from ``Authorization: Bearer <token>``, or None for anything else."""
    if not header or len(header) > MAX_HEADER:
        return None
    scheme, _, value = header.partition(" ")
    return (value.strip() or None) if scheme.lower() == "bearer" else None


# ---------------------------------------------------------------- administration


async def add_user(engine: AsyncEngine, name: str) -> int:
    if not USER_NAME.fullmatch(name):
        raise AuthError(
            "user names start with a lowercase letter and use only a-z 0-9 _ . - "
            "(at most 63 characters)"
        )
    try:
        async with engine.begin() as conn:
            user_id: int = (
                await conn.execute(
                    text("INSERT INTO users (name) VALUES (:n) RETURNING id"), {"n": name}
                )
            ).scalar_one()
    except IntegrityError as exc:
        raise AuthError(f"user {name!r} already exists") from exc
    return user_id


async def remove_user(engine: AsyncEngine, name: str) -> int:
    """Delete a user and (by cascade) their tokens; returns how many tokens went with them."""
    async with engine.begin() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT u.id, (SELECT count(*) FROM api_tokens t WHERE t.user_id = u.id) "
                    "FROM users u WHERE u.name = :n"
                ),
                {"n": name},
            )
        ).first()
        if row is None:
            raise AuthError(f"no such user {name!r}")
        await conn.execute(text("DELETE FROM users WHERE id = :i"), {"i": row[0]})
    return int(row[1])


async def list_users(engine: AsyncEngine) -> list[dict[str, Any]]:
    async with engine.connect() as conn:
        found = await conn.execute(
            text(
                "SELECT u.name, u.created_at, "
                "count(t.id) FILTER (WHERE t.revoked_at IS NULL) AS active_tokens "
                "FROM users u LEFT JOIN api_tokens t ON t.user_id = u.id "
                "GROUP BY u.id ORDER BY u.name"
            )
        )
        return [dict(r._mapping) for r in found]


async def create_token(
    engine: AsyncEngine,
    user: str,
    role: str,
    *,
    label: str = "",
    expires_in: timedelta | None = None,
    now: datetime | None = None,
) -> tuple[int, str]:
    """Create a token and return ``(id, the full token)``. The token cannot be shown again."""
    if role not in ROLES:
        raise AuthError(f"role must be one of {', '.join(ROLES)}")
    when = now or datetime.now(UTC)
    secret = new_secret()
    async with engine.begin() as conn:
        user_id = (
            await conn.execute(text("SELECT id FROM users WHERE name = :n"), {"n": user})
        ).scalar_one_or_none()
        if user_id is None:
            raise AuthError(f"no such user {user!r}: create it first with `user add`")
        token_id: int = (
            await conn.execute(
                text(
                    "INSERT INTO api_tokens (user_id, label, role, token_hash, created_at, "
                    "expires_at) VALUES (:u, :l, :r, :h, :c, :e) RETURNING id"
                ),
                {
                    "u": user_id,
                    "l": label,
                    "r": role,
                    "h": hash_secret(secret),
                    "c": when,
                    "e": when + expires_in if expires_in else None,
                },
            )
        ).scalar_one()
    return token_id, format_token(token_id, secret)


async def list_tokens(engine: AsyncEngine, user: str | None = None) -> list[dict[str, Any]]:
    """Token metadata only: neither the secret nor its hash is ever returned."""
    async with engine.connect() as conn:
        found = await conn.execute(
            text(
                "SELECT t.id, u.name AS user, t.label, t.role, t.created_at, t.expires_at, "
                "t.revoked_at, t.last_used_at FROM api_tokens t JOIN users u ON u.id = t.user_id "
                "WHERE CAST(:u AS text) IS NULL OR u.name = CAST(:u AS text) ORDER BY t.id"
            ),
            {"u": user},
        )
        return [dict(r._mapping) for r in found]


async def revoke_token(engine: AsyncEngine, token_id: int, now: datetime | None = None) -> bool:
    async with engine.begin() as conn:
        result = await conn.execute(
            text("UPDATE api_tokens SET revoked_at = :n WHERE id = :i AND revoked_at IS NULL"),
            {"n": now or datetime.now(UTC), "i": token_id},
        )
    return bool(result.rowcount)


# ---------------------------------------------------------------- verification


async def authenticate(
    engine: AsyncEngine, token: str | None, now: datetime | None = None
) -> Principal | None:
    """The caller behind ``token``, or None. Every failure looks the same from outside."""
    parsed = parse_token(token) if token else None
    if parsed is None:
        return None
    token_id, secret = parsed
    when = now or datetime.now(UTC)
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT t.user_id, u.name, t.role, t.token_hash, t.expires_at, t.revoked_at, "
                    "t.last_used_at FROM api_tokens t JOIN users u ON u.id = t.user_id "
                    "WHERE t.id = :i"
                ),
                {"i": token_id},
            )
        ).first()
    stored = bytes(row[3]) if row is not None else _DUMMY_HASH  # same work for an unknown id
    matches = hmac.compare_digest(hash_secret(secret), stored)
    if row is None or not matches:
        return None
    user_id, user, role, _, expires_at, revoked_at, last_used = row
    if revoked_at is not None or (expires_at is not None and expires_at <= when):
        return None
    if last_used is None or when - last_used >= LAST_USED_EVERY:
        async with engine.begin() as conn:
            await conn.execute(
                text("UPDATE api_tokens SET last_used_at = :n WHERE id = :i"),
                {"n": when, "i": token_id},
            )
    return Principal(user_id, user, role, token_id)


# ---------------------------------------------------------------- throttling


class FailureThrottle:
    """Lock an address out for a while after too many failed logins.

    In memory (one API process); bounded so an attacker cannot grow it without limit.
    """

    def __init__(
        self,
        max_failures: int = 10,
        window: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
        max_addresses: int = 10_000,
    ) -> None:
        self._max = max_failures
        self._window = window
        self._clock = clock
        self._cap = max_addresses
        self._failures: dict[str, deque[float]] = {}

    def _recent(self, address: str) -> deque[float]:
        events = self._failures.get(address)
        if events is None:
            return deque()
        cutoff = self._clock() - self._window
        while events and events[0] <= cutoff:
            events.popleft()
        if not events:
            del self._failures[address]
        return events

    def retry_after(self, address: str) -> int | None:
        """Seconds until ``address`` may try again, or None if it is not locked out."""
        events = self._recent(address)
        if len(events) < self._max:
            return None
        return max(1, int(events[0] + self._window - self._clock()) + 1)

    def record_failure(self, address: str) -> None:
        if address not in self._failures and len(self._failures) >= self._cap:
            self._failures.pop(next(iter(self._failures)))  # drop the oldest address
        self._failures.setdefault(address, deque()).append(self._clock())
