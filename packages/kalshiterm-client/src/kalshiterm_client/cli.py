"""``kterm``: the KalshiTerm client's command line (grows with later slices)."""

import json
import sys
from typing import Annotated

import typer

from kalshiterm_client import connection, pinning, secrets, status_view
from kalshiterm_client.profiles import (
    PROFILE_ENV,
    Profile,
    ProfileError,
    ProfileStore,
    check_name,
    check_token,
    normalise_url,
)

app = typer.Typer(no_args_is_help=True, help="KalshiTerm client.")
config_app = typer.Typer(no_args_is_help=True, help="Servers (profiles) and their tokens.")
app.add_typer(config_app, name="config")
server_app = typer.Typer(no_args_is_help=True, help="The server you are connected to.")
app.add_typer(server_app, name="server")


class State:
    profile: str | None = None


@app.callback()
def main(
    profile: Annotated[
        str | None,
        typer.Option("--profile", "-p", envvar=PROFILE_ENV, help="Which server profile to use."),
    ] = None,
) -> None:
    State.profile = profile


def fail(message: str) -> typer.Exit:
    typer.echo(f"error: {message}", err=True)
    return typer.Exit(code=1)


def current_profile() -> Profile:
    """The profile commands should act on; later slices (status, markets) start here."""
    try:
        return ProfileStore().resolve(State.profile)
    except ProfileError as exc:
        raise fail(str(exc)) from exc


def _read_token(from_stdin: bool) -> str:
    if from_stdin:
        return sys.stdin.readline().strip()
    return str(typer.prompt("Token (input hidden)", hide_input=True)).strip()


def _store_token(name: str, token: str) -> None:
    try:
        secrets.set_token(name, check_token(token))
    except (ProfileError, secrets.SecretError) as exc:
        raise fail(str(exc)) from exc


@config_app.command("add")
def config_add(
    name: str,
    url: Annotated[str, typer.Option(help="The server, e.g. https://mac-studio:8700")],
    token_stdin: Annotated[bool, typer.Option(help="Read the token from standard input.")] = False,
    no_token: Annotated[bool, typer.Option(help="Do not ask for a token now.")] = False,
    replace: Annotated[bool, typer.Option(help="Replace an existing profile.")] = False,
) -> None:
    """Add a server. The token goes to the system keyring, never to a file."""
    store = ProfileStore()
    try:
        address = normalise_url(url)  # check everything before writing anything
        check_name(name)
        token = None if no_token else check_token(_read_token(token_stdin))
        store.add(name, address, replace=replace)
        if token:
            secrets.set_token(name, token)
    except (ProfileError, secrets.SecretError) as exc:
        raise fail(str(exc)) from exc
    typer.echo(f"profile {name}: {address}")
    if token:
        typer.echo("token stored in the system keyring")


@config_app.command("token")
def config_token(
    name: str,
    token_stdin: Annotated[bool, typer.Option(help="Read the token from standard input.")] = False,
) -> None:
    """Set or replace a profile's token."""
    try:
        ProfileStore().get(name)
    except ProfileError as exc:
        raise fail(str(exc)) from exc
    _store_token(name, _read_token(token_stdin))
    typer.echo("token stored in the system keyring")


@config_app.command("list")
def config_list() -> None:
    """The configured servers; * marks the default."""
    store = ProfileStore()
    try:
        names, default = store.names(), store.default_name()
        if not names:
            typer.echo("no servers configured: `kterm config add NAME --url https://...`")
        for name in names:
            mark = "*" if name == default else " "
            typer.echo(f"{mark} {name:<20}{store.get(name).url}")
    except ProfileError as exc:
        raise fail(str(exc)) from exc


@config_app.command("show")
def config_show(name: Annotated[str | None, typer.Argument()] = None) -> None:
    """One profile in detail. The token itself is never shown."""
    try:
        profile = ProfileStore().resolve(name or State.profile)
    except ProfileError as exc:
        raise fail(str(exc)) from exc
    stored = secrets.has_stored_token(profile.name)
    typer.echo(f"name:  {profile.name}")
    typer.echo(f"url:   {profile.url}")
    typer.echo(f"token: {'in the system keyring' if stored else 'NOT SET'}")
    if profile.url.startswith("https://"):
        pinned = profile.fingerprint or "NOT YET (run `kterm server trust`)"
        typer.echo(f"pin:   {pinned}")


@config_app.command("use")
def config_use(name: str) -> None:
    """Make a profile the default."""
    try:
        ProfileStore().use(name)
    except ProfileError as exc:
        raise fail(str(exc)) from exc
    typer.echo(f"default server is now {name}")


@config_app.command("remove")
def config_remove(name: str) -> None:
    """Forget a server and delete its token from the keyring."""
    try:
        ProfileStore().remove(name)
        secrets.delete_token(name)
    except (ProfileError, secrets.SecretError) as exc:
        raise fail(str(exc)) from exc
    typer.echo(f"removed {name}")


# ------------------------------------------------------------------ the server itself


def _show_certificate(details: pinning.CertDetails) -> None:
    typer.echo(f"  fingerprint (SHA-256): {details.fingerprint}")
    typer.echo(f"  names:   {', '.join(details.dns_names + details.ip_addresses) or '(none)'}")
    typer.echo(
        f"  valid:   {pinning.local_time(details.not_before)} to "
        f"{pinning.local_time(details.not_after)}"
    )


@server_app.command("trust")
def server_trust(
    fingerprint: Annotated[
        str | None,
        typer.Option(
            help="Pin without asking, but only if the server's certificate has exactly this "
            "fingerprint (as printed by `kterm-server init` or `cert show`)."
        ),
    ] = None,
) -> None:
    """Read the server's certificate, show its fingerprint, and pin it after you confirm.

    Compare the fingerprint with the one the server's operator printed (`kterm-server cert
    show`). From then on this profile trusts that certificate and no other.
    """
    profile = current_profile()
    if not profile.url.startswith("https://"):
        raise fail("this profile is plain http to this machine: there is no certificate to trust")
    try:
        pem = pinning.fetch_certificate(profile.url)
        details = pinning.describe(pem)
        if fingerprint is not None:
            wanted = pinning.normalise_fingerprint(fingerprint)
            if wanted != details.fingerprint:
                raise fail(
                    f"the server presented {details.fingerprint}, not {wanted}. Nothing pinned."
                )
        elif profile.fingerprint == details.fingerprint:
            typer.echo(f"{profile.name} already trusts this certificate:")
            _show_certificate(details)
            return
        else:
            if profile.fingerprint:
                typer.echo(f"WARNING: {profile.name} trusts a DIFFERENT certificate.")
                typer.echo(f"  currently pinned: {profile.fingerprint}")
                typer.echo("The server's certificate is now:")
            else:
                typer.echo(f"The server at {profile.url} presents this certificate:")
            _show_certificate(details)
            if not typer.confirm("Trust this certificate for this profile?", default=False):
                raise fail("not trusted; nothing changed")
        ProfileStore().save_pin(profile.name, pem, details.fingerprint)
    except ProfileError as exc:
        raise fail(str(exc)) from exc
    typer.echo(f"pinned for {profile.name}: {details.fingerprint}")


@server_app.command("status")
def server_status(
    as_json: Annotated[bool, typer.Option("--json", help="The raw report.")] = False,
) -> None:
    """Storage, ingest freshness, gaps, jobs and backups (needs an admin token)."""
    profile = current_profile()
    try:
        report = connection.request(profile, "/v1/status")
    except ProfileError as exc:
        raise fail(str(exc)) from exc
    typer.echo(json.dumps(report, indent=2) if as_json else status_view.render(report))
    if report.get("problems"):
        raise typer.Exit(code=1)  # so scripts can act on it, like `kterm-server status`
