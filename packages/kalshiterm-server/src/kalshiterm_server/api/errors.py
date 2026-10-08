"""Uniform error responses: ``{"error": "<code>"}`` and nothing else."""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


class ApiError(Exception):
    def __init__(self, status: int, code: str, headers: dict[str, str] | None = None) -> None:
        self.status = status
        self.code = code
        self.headers = headers or {}


def install(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def handle(_: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse({"error": exc.code}, status_code=exc.status, headers=exc.headers)
