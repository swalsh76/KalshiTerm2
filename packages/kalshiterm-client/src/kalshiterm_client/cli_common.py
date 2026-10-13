"""What every ``kterm`` command shares: which server, running work against it, printing."""

import asyncio
import json
import sys
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

import typer
from pydantic import BaseModel
from rich.console import Console

from kalshiterm_client.client import ServerClient
from kalshiterm_client.profiles import Profile, ProfileError, ProfileStore

T = TypeVar("T")


class State:
    profile: str | None = None  # --profile, set by the app callback


def fail(message: str) -> typer.Exit:
    typer.echo(f"error: {message}", err=True)
    return typer.Exit(code=1)


def current_profile() -> Profile:
    """The profile commands act on: --profile, then KTERM_PROFILE, then the default."""
    try:
        return ProfileStore().resolve(State.profile)
    except ProfileError as exc:
        raise fail(str(exc)) from exc


def run_with_api(work: Callable[[ServerClient], Awaitable[T]]) -> T:
    """Open a client for the current profile, run ``work(api)``, and turn any problem a person
    can act on (a changed certificate, a refused request, an unreachable server) into a one-line
    error and exit status 1."""
    profile = current_profile()

    async def go() -> T:
        async with ServerClient.for_profile(profile) as api:
            return await work(api)

    try:
        return asyncio.run(go())
    except ProfileError as exc:
        raise fail(str(exc)) from exc


def make_console() -> Console:
    """A console that is wide enough when piped (rich assumes 80 columns without a terminal) and
    never colours a pipe."""
    return Console(width=None if sys.stdout.isatty() else 120)


def plain(value: Any) -> Any:
    """``value`` as JSON-ready data: models become dicts with decimals as exact strings."""
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json", by_alias=True)
    if isinstance(value, list | tuple):
        return [plain(v) for v in value]
    if isinstance(value, dict):
        return {k: plain(v) for k, v in value.items()}
    return value


def emit_json(value: Any) -> None:
    typer.echo(json.dumps(plain(value), indent=2))
