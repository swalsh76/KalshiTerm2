"""Authentication dependencies for the routes under ``/v1``."""

from fastapi import Depends, Request

from kalshiterm_server import auth
from kalshiterm_server.api.errors import ApiError

UNAUTHORIZED = {"WWW-Authenticate": "Bearer"}


async def principal(request: Request) -> auth.Principal:
    """The authenticated caller, or a 401 that says nothing about *why* it failed."""
    address = request.client.host if request.client else "unknown"
    throttle: auth.FailureThrottle = request.app.state.throttle
    wait = throttle.retry_after(address)
    if wait is not None:
        raise ApiError(429, "too_many_failed_attempts", {"Retry-After": str(wait)})
    token = auth.bearer_token(request.headers.get("authorization"))
    who = await auth.authenticate(request.app.state.engine, token, request.app.state.clock())
    if who is None:
        throttle.record_failure(address)
        raise ApiError(401, "unauthorized", UNAUTHORIZED)
    return who


async def admin(who: auth.Principal = Depends(principal)) -> auth.Principal:  # noqa: B008
    if who.role != "admin":
        raise ApiError(403, "forbidden")
    return who
