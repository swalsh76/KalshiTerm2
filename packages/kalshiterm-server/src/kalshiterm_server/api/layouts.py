"""Saved UI layouts: every route is about the caller's own."""

from typing import Annotated, Any

from fastapi import APIRouter, Path, Request, Response

from kalshiterm_server import layouts
from kalshiterm_server.api import format as fmt
from kalshiterm_server.api.errors import ApiError
from kalshiterm_server.api.markets import Reader

router = APIRouter(prefix="/v1")
# Letters, digits, space, '_', '-' and '.'; starts and ends with a non-space; 1-64 characters.
NAME = r"^[A-Za-z0-9]([A-Za-z0-9 _.-]{0,62}[A-Za-z0-9_.-])?$"
Name = Annotated[str, Path(pattern=NAME)]
ENVELOPE_ALLOWANCE = 4096  # room for {"data": ...} around a layout of the maximum size


def failure(exc: layouts.LayoutError) -> ApiError:
    return ApiError(exc.status, exc.code, **exc.extra)


async def read_limited(request: Request, limit: int, report: int | None = None) -> bytes:
    """The request body, abandoned the moment it passes ``limit`` bytes (a lying or missing
    Content-Length cannot make us buffer more). ``report`` is the limit the caller is told about
    when that happens (the layout cap, which is smaller than the read limit by the envelope)."""
    shown = limit if report is None else report
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > limit:
        raise ApiError(413, "layout_too_large", limit_bytes=shown)
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > limit:
            raise ApiError(413, "layout_too_large", limit_bytes=shown)
        chunks.append(chunk)
    return b"".join(chunks)


@router.get("/layouts")
async def my_layouts(request: Request, who: Reader) -> dict[str, Any]:
    """Your saved layouts (names and sizes; fetch one to get its content)."""
    found = await layouts.list_layouts(request.app.state.engine, who.user_id)
    config = request.app.state.settings
    return {
        "layouts": [
            {
                "name": item.name,
                "size_bytes": item.size_bytes,
                "created_at": fmt.moment(item.created_at),
                "updated_at": fmt.moment(item.updated_at),
            }
            for item in found
        ],
        "limit": config.layouts_max_per_user,
        "max_bytes": config.layout_max_bytes,
    }


@router.get("/layouts/{name}")
async def get_layout(request: Request, name: Name, who: Reader) -> dict[str, Any]:
    try:
        layout = await layouts.get_layout(request.app.state.engine, who.user_id, name)
    except layouts.LayoutError as exc:
        raise failure(exc) from exc
    return {
        "name": layout.name,
        "data": layout.data,
        "size_bytes": layout.size_bytes,
        "updated_at": fmt.moment(layout.updated_at),
    }


@router.put("/layouts/{name}")
async def save_layout(
    request: Request, name: Name, who: Reader, response: Response
) -> dict[str, Any]:
    """Create or replace a layout. The body is ``{"data": {...}}``: any JSON object, at most
    ``max_bytes`` of compact JSON, nested at most 64 deep. 201 when new, 200 when replaced."""
    config = request.app.state.settings
    raw = await read_limited(
        request, config.layout_max_bytes + ENVELOPE_ALLOWANCE, report=config.layout_max_bytes
    )
    try:
        body = layouts.parse_json(raw)
    except ValueError:
        raise ApiError(400, "invalid_json") from None
    if not isinstance(body, dict) or set(body) != {"data"}:
        raise ApiError(422, "invalid_layout", reason='the body must be {"data": {...}}')
    try:
        saved = await layouts.save_layout(
            request.app.state.engine,
            who.user_id,
            name,
            body["data"],
            max_bytes=config.layout_max_bytes,
            max_layouts=config.layouts_max_per_user,
        )
    except layouts.LayoutError as exc:
        raise failure(exc) from exc
    response.status_code = 201 if saved.created else 200
    return {
        "name": saved.name,
        "created": saved.created,
        "size_bytes": saved.size_bytes,
        "updated_at": fmt.moment(saved.updated_at),
    }


@router.delete("/layouts/{name}")
async def remove_layout(request: Request, name: Name, who: Reader) -> dict[str, Any]:
    try:
        await layouts.delete_layout(request.app.state.engine, who.user_id, name)
    except layouts.LayoutError as exc:
        raise failure(exc) from exc
    return {"name": name, "deleted": True}
