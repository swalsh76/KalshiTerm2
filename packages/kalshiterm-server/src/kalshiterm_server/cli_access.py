"""``kterm-server user`` and ``kterm-server token``: who may use the API."""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Annotated

import typer

from kalshiterm_server import auth, db
from kalshiterm_server.cli_common import db_url

user_app = typer.Typer(no_args_is_help=True, help="API users.")


token_app = typer.Typer(no_args_is_help=True, help="API tokens.")


def _admin(action: Callable[..., Awaitable[object]]) -> object:
    """Run one administrative database action, reporting mistakes plainly."""

    async def run() -> object:
        engine = db.make_engine(db_url())
        try:
            return await action(engine)
        finally:
            await engine.dispose()

    try:
        return asyncio.run(run())
    except auth.AuthError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@user_app.command("add")
def user_add(name: str) -> None:
    """Create a user. Then give them a token with `token create`."""
    _admin(lambda engine: auth.add_user(engine, name))
    typer.echo(f"created user {name}")


@user_app.command("list")
def user_list() -> None:
    """List users and how many active tokens each has."""
    users = _admin(lambda engine: auth.list_users(engine))
    assert isinstance(users, list)
    if not users:
        typer.echo("(no users)")
    for user in users:
        typer.echo(
            f"{user['name']:<24}{user['active_tokens']} active token(s)  "
            f"since {user['created_at']:%Y-%m-%d}"
        )


@user_app.command("remove")
def user_remove(
    name: str, yes: Annotated[bool, typer.Option("--yes", help="Do not ask.")] = False
) -> None:
    """Delete a user and all of their tokens."""
    if not yes:
        typer.confirm(f"Delete user {name} and all of their tokens?", abort=True)
    count = _admin(lambda engine: auth.remove_user(engine, name))
    typer.echo(f"removed user {name} and {count} token(s)")


@token_app.command("create")
def token_create(
    user: Annotated[str, typer.Option(help="Who the token belongs to.")],
    role: Annotated[str, typer.Option(help="read or admin.")] = "read",
    label: Annotated[str, typer.Option(help="A note, e.g. 'laptop'.")] = "",
    expires_days: Annotated[float | None, typer.Option(help="Expire after this many days.")] = None,
) -> None:
    """Create a token. It is printed once, on its own line of standard output, and never again."""
    expires = timedelta(days=expires_days) if expires_days else None
    result = _admin(
        lambda engine: auth.create_token(engine, user, role, label=label, expires_in=expires)
    )
    assert isinstance(result, tuple)
    token_id, token = result
    typer.echo(
        f"token {token_id} for {user} ({role}); store it now, it cannot be shown again", err=True
    )
    typer.echo(token)  # only the token goes to stdout, so `T=$(... token create ...)` works


@token_app.command("list")
def token_list(user: Annotated[str | None, typer.Option(help="Only this user's.")] = None) -> None:
    """List tokens (metadata only: the secret is never stored and cannot be shown)."""
    tokens = _admin(lambda engine: auth.list_tokens(engine, user))
    assert isinstance(tokens, list)
    if not tokens:
        typer.echo("(no tokens)")
    for tok in tokens:
        state = "revoked" if tok["revoked_at"] else "active"
        used = f"{tok['last_used_at']:%Y-%m-%d %H:%M}" if tok["last_used_at"] else "never"
        expires = f"{tok['expires_at']:%Y-%m-%d}" if tok["expires_at"] else "no expiry"
        typer.echo(
            f"#{tok['id']:<5}{tok['user']:<20}{tok['role']:<7}{state:<9}"
            f"last used {used:<17}{expires:<12}{tok['label']}"
        )


@token_app.command("revoke")
def token_revoke(token_id: int) -> None:
    """Revoke a token immediately."""
    revoked = _admin(lambda engine: auth.revoke_token(engine, token_id))
    typer.echo(
        f"revoked token {token_id}" if revoked else f"token {token_id} not found or already revoked"
    )
    if not revoked:
        raise typer.Exit(code=1)


def register(app: typer.Typer) -> None:
    app.add_typer(user_app, name="user")
    app.add_typer(token_app, name="token")
