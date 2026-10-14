"""Per-user named UI layouts.

The server stores a layout without interpreting it: a JSON object the front end defines and
versions. What it does enforce, because a layout comes from a browser and is stored in ``jsonb``:

* the **size** (``layout_max_bytes`` of compact JSON) and **how many** a user may keep, the count
  checked in one transaction under a per-user lock so two simultaneous saves cannot both slip under
  the cap;
* the **shape**: an object, nested at most ``MAX_DEPTH`` deep, with no NUL character anywhere (jsonb
  cannot hold one and would answer 500) and no NaN / Infinity (not JSON).

Every function is about one user's layouts; there is no way to name another user's.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

MAX_DEPTH = 64
LOCK_CLASS = 7_002  # advisory lock class; the user id is the second key


class LayoutError(Exception):
    def __init__(self, status: int, code: str, **extra: Any) -> None:
        super().__init__(code)
        self.status = status
        self.code = code
        self.extra = extra


@dataclass(frozen=True, slots=True)
class LayoutInfo:
    name: str
    size_bytes: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class Layout:
    name: str
    data: dict[str, Any]
    size_bytes: int
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class Saved:
    name: str
    created: bool
    size_bytes: int
    updated_at: datetime


def parse_json(raw: bytes) -> Any:
    """The body as JSON, refusing NaN / Infinity (Python accepts them, JSON and jsonb do not)."""

    def refuse(constant: str) -> Any:
        raise ValueError(constant)

    return json.loads(raw, parse_constant=refuse)


def check_shape(data: Any) -> None:
    """Raise ``LayoutError`` unless ``data`` is an object of acceptable depth and content."""
    if not isinstance(data, dict):
        raise LayoutError(422, "invalid_layout", reason="a layout must be a JSON object")
    stack: list[tuple[Any, int]] = [(data, 1)]
    while stack:
        node, depth = stack.pop()
        if depth > MAX_DEPTH:
            raise LayoutError(422, "invalid_layout", reason=f"nested more than {MAX_DEPTH} deep")
        if isinstance(node, dict):
            for key, value in node.items():
                if "\x00" in key:
                    raise LayoutError(422, "invalid_layout", reason="contains a NUL character")
                stack.append((value, depth + 1))
        elif isinstance(node, list):
            stack.extend((item, depth + 1) for item in node)
        elif isinstance(node, str) and "\x00" in node:
            raise LayoutError(422, "invalid_layout", reason="contains a NUL character")


def compact(data: dict[str, Any]) -> str:
    return json.dumps(data, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


async def list_layouts(engine: AsyncEngine, user_id: int) -> list[LayoutInfo]:
    async with engine.connect() as conn:
        rows = await conn.execute(
            text(
                "SELECT name, size_bytes, created_at, updated_at FROM ui_layouts "
                "WHERE user_id = :u ORDER BY name"
            ),
            {"u": user_id},
        )
        return [LayoutInfo(*r) for r in rows]


async def get_layout(engine: AsyncEngine, user_id: int, name: str) -> Layout:
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT name, data, size_bytes, updated_at FROM ui_layouts "
                    "WHERE user_id = :u AND name = :n"
                ),
                {"u": user_id, "n": name},
            )
        ).first()
    if row is None:
        raise LayoutError(404, "not_found")
    return Layout(*row)


async def save_layout(
    engine: AsyncEngine,
    user_id: int,
    name: str,
    data: dict[str, Any],
    *,
    max_bytes: int,
    max_layouts: int,
) -> Saved:
    """Create or replace ``name``. Replacing never counts against the cap."""
    check_shape(data)
    body = compact(data)
    size = len(body.encode())
    if size > max_bytes:
        raise LayoutError(413, "layout_too_large", limit_bytes=max_bytes, size_bytes=size)
    async with engine.begin() as conn:
        await conn.execute(
            text("SELECT pg_advisory_xact_lock(:c, :u)"), {"c": LOCK_CLASS, "u": user_id}
        )
        exists = (
            await conn.execute(
                text("SELECT 1 FROM ui_layouts WHERE user_id = :u AND name = :n"),
                {"u": user_id, "n": name},
            )
        ).first()
        if exists is None:
            count = (
                await conn.execute(
                    text("SELECT count(*) FROM ui_layouts WHERE user_id = :u"), {"u": user_id}
                )
            ).scalar_one()
            if count >= max_layouts:
                raise LayoutError(422, "layout_limit", limit=max_layouts)
        row = (
            await conn.execute(
                text(
                    "INSERT INTO ui_layouts (user_id, name, data, size_bytes) "
                    "VALUES (:u, :n, CAST(:d AS jsonb), :s) "
                    "ON CONFLICT (user_id, name) DO UPDATE SET data = EXCLUDED.data, "
                    "size_bytes = EXCLUDED.size_bytes, updated_at = now() "
                    "RETURNING updated_at"
                ),
                {"u": user_id, "n": name, "d": body, "s": size},
            )
        ).one()
    return Saved(name, exists is None, size, row[0])


async def delete_layout(engine: AsyncEngine, user_id: int, name: str) -> None:
    async with engine.begin() as conn:
        result = await conn.execute(
            text("DELETE FROM ui_layouts WHERE user_id = :u AND name = :n"),
            {"u": user_id, "n": name},
        )
    if result.rowcount == 0:
        raise LayoutError(404, "not_found")
