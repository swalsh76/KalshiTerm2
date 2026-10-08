"""Uniform error responses: ``{"error": "<code>"}`` plus, where useful, which field was wrong."""

from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

CODES = {404: "not_found", 405: "method_not_allowed"}


class ApiError(Exception):
    def __init__(
        self,
        status: int,
        code: str,
        headers: dict[str, str] | None = None,
        **extra: Any,
    ) -> None:
        self.status = status
        self.code = code
        self.headers = headers or {}
        self.extra = extra


def install(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def handle(_: Request, exc: ApiError) -> JSONResponse:
        body = {"error": exc.code, **exc.extra}
        return JSONResponse(body, status_code=exc.status, headers=exc.headers)

    @app.exception_handler(RequestValidationError)
    async def invalid(_: Request, exc: RequestValidationError) -> JSONResponse:
        # Name the offending parameters; never echo what the caller sent.
        fields = sorted({str(e["loc"][-1]) for e in exc.errors() if e.get("loc")})
        return JSONResponse({"error": "invalid_parameter", "fields": fields}, status_code=422)

    @app.exception_handler(StarletteHTTPException)
    async def http_error(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = CODES.get(exc.status_code, "http_error")
        return JSONResponse({"error": code}, status_code=exc.status_code)
