"""Routes under ``/v1`` (every one needs a token)."""

from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.encoders import jsonable_encoder

from kalshiterm_server import auth
from kalshiterm_server.api.deps import admin, principal
from kalshiterm_server.status import collect_status

router = APIRouter(prefix="/v1")


@router.get("/me")
async def me(who: auth.Principal = Depends(principal)) -> dict[str, Any]:  # noqa: B008
    """Who the server thinks you are."""
    return {"user": who.user, "role": who.role, "token_id": who.token_id}


@router.get("/status")
async def status(request: Request, _: auth.Principal = Depends(admin)) -> Any:  # noqa: B008
    """The operator's health report (admin tokens only); the same data as `kterm-server status`."""
    config = request.app.state.settings
    report = await collect_status(
        request.app.state.engine,
        config.storage_budget_gb,
        config.disk_check_path,
        host_state_file=config.host_state_file,
    )
    return jsonable_encoder(report)
