"""FastAPI application factory.

``/healthz`` and ``/readyz`` are open and deliberately say nothing about the data. Everything
under ``/v1`` needs a token (slice 3.2). Interactive docs and the OpenAPI document are off
unless ``KTERM_API_DOCS`` is set: they describe the whole surface to anyone on the LAN.
"""

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import FastAPI, Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from kalshiterm_server import db
from kalshiterm_server.api import errors, markets, rawdata, v1, watchlist
from kalshiterm_server.auth import FailureThrottle
from kalshiterm_server.config import ServerSettings


def create_app(
    settings: ServerSettings | None = None,
    engine: AsyncEngine | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> FastAPI:
    """Build the app. Tests pass their own ``engine``; otherwise one is made from settings."""
    config = settings or ServerSettings()  # type: ignore[call-arg]  # db_url from KTERM_DB_URL

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        owned = engine is None
        app.state.engine = engine or db.make_engine(config.db_url)
        try:
            yield
        finally:
            if owned:
                await app.state.engine.dispose()

    docs = "/docs" if config.api_docs else None
    app = FastAPI(
        title="KalshiTerm server",
        lifespan=lifespan,
        docs_url=docs,
        redoc_url=None,
        openapi_url="/openapi.json" if config.api_docs else None,
    )
    app.state.settings = config
    app.state.clock = clock
    app.state.throttle = FailureThrottle(
        config.auth_failure_limit, config.auth_failure_window_seconds
    )
    errors.install(app)
    app.include_router(v1.router)
    app.include_router(markets.router)
    app.include_router(rawdata.router)
    app.include_router(watchlist.router)

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        """The process is up. Says nothing about the database."""
        return {"status": "ok"}

    @app.get("/readyz", include_in_schema=False)
    async def readyz(response: Response) -> dict[str, object]:
        """Ready to serve: the database answers and its schema is the one this code expects."""
        try:
            async with app.state.engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        except Exception:
            response.status_code = 503
            return {"ready": False, "reason": "database unreachable"}
        try:
            async with app.state.engine.connect() as conn:
                revision = (
                    await conn.execute(text("SELECT version_num FROM alembic_version"))
                ).scalar_one_or_none()
        except Exception:  # connected, but there is no schema yet
            revision = None
        if revision != db.head_revision():
            response.status_code = 503
            return {"ready": False, "reason": "database not migrated"}
        return {"ready": True}

    return app
