"""``kterm-server`` operations CLI: wiring, plus the core commands.

The command groups live in ``cli_ingest``, ``cli_access``, ``cli_backup`` and ``cli_analyze``;
the settings they share are in ``cli_common``.
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Annotated

import typer
from kalshi_core.auth import AuthError

from kalshiterm_server import cli_access, cli_analyze, cli_backup, cli_ingest, db, tls
from kalshiterm_server.api.serve import UnsafeBind, serve
from kalshiterm_server.cli_common import db_url, server_settings
from kalshiterm_server.init import InitError, run_init
from kalshiterm_server.status import collect_status, quick_health, render

app = typer.Typer(no_args_is_help=True, help="KalshiTerm server operations.")
db_app = typer.Typer(no_args_is_help=True, help="Database migrations.")
app.add_typer(db_app, name="db")
cli_access.register(app)  # the user and token groups
cert_app = typer.Typer(no_args_is_help=True, help="The server's TLS certificate.")
app.add_typer(cert_app, name="cert")
# Typer lists commands in registration order, so this one goes first: discover, ingest, then the
# commands below, then restore (registered last, by cli_backup).
cli_ingest.register(app)


@db_app.command("upgrade")
def db_upgrade(revision: str = "head") -> None:
    """Apply migrations up to REVISION (default: head)."""
    db.upgrade(db_url(), revision)
    typer.echo(f"database is at {revision}")


@db_app.command("downgrade")
def db_downgrade(revision: str) -> None:
    """Roll back to REVISION (e.g. -1 or a revision id)."""
    db.downgrade(db_url(), revision)
    typer.echo(f"database rolled back to {revision}")


@db_app.command("current")
def db_current() -> None:
    """Show the revision the database is at."""

    async def run() -> str | None:
        engine = db.make_engine(db_url())
        try:
            return await db.current_revision(engine)
        finally:
            await engine.dispose()

    typer.echo(asyncio.run(run()) or "(no migrations applied)")


@app.command("status")
def status_command(
    as_json: bool = typer.Option(False, "--json", help="Machine-readable output."),
) -> None:
    """Storage, ingest freshness, gaps, jobs and discovery in one report."""

    async def run() -> dict[str, object]:
        server = server_settings()
        engine = db.make_engine(server.db_url)
        try:
            return await collect_status(
                engine,
                server.storage_budget_gb,
                server.disk_check_path,
                host_state_file=server.host_state_file,
                backup_configured=bool(server.backup_target),
            )
        finally:
            await engine.dispose()

    report = asyncio.run(run())
    typer.echo(json.dumps(report, indent=2, default=str) if as_json else render(report))
    if report["problems"]:
        raise typer.Exit(code=1)  # so scripts and health checks can act on it


@app.command("health")
def health_command() -> None:
    """Fast liveness check (for the container health check): exit 0 if healthy, else 1."""

    async def run() -> list[str]:
        engine = db.make_engine(db_url())
        try:
            return await quick_health(engine)
        finally:
            await engine.dispose()

    problems = asyncio.run(run())
    if problems:
        typer.echo("unhealthy: " + "; ".join(problems))
        raise typer.Exit(code=1)
    typer.echo("ok")


@app.command("init")
def init_command(
    key_id: Annotated[str, typer.Option(help="Key ID of the server's own READ-ONLY Kalshi key.")],
    key_file: Annotated[
        Path,
        # Not exists=True: run_init checks it, so the error can explain the Docker case.
        typer.Option(help="Its Ed25519 private key (PEM file)."),
    ],
    host: Annotated[
        list[str] | None,
        typer.Option("--host", help="A name clients use to reach the server (repeatable)."),
    ] = None,
    ip: Annotated[
        list[str] | None,
        typer.Option("--ip", help="An address clients use to reach the server (repeatable)."),
    ] = None,
    out: Annotated[Path, typer.Option(help="The deploy directory to write into.")] = Path(),
    budget_gb: float = typer.Option(500, help="Storage budget in GB (PLAN §9)."),
    backup_dir: Annotated[
        Path | None,
        typer.Option(help="Absolute HOST path of the mounted NAS directory for nightly backups."),
    ] = None,
    force: bool = typer.Option(False, help="Overwrite an existing setup (new DB password!)."),
) -> None:
    """Write .env, secrets and a starter watchlist for a production deployment."""
    try:
        result = run_init(
            out,
            key_id=key_id,
            key_file=key_file,
            hosts=host,
            ips=ip,
            budget_gb=budget_gb,
            backup_dir=backup_dir,
            force=force,
        )
    except (InitError, AuthError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    for path in result.written:
        typer.echo(f"wrote {path}")
    for path in result.kept:
        typer.echo(f"kept  {path} (already there)")
    if result.certificate:
        typer.echo(f"TLS certificate fingerprint (SHA-256): {result.certificate.fingerprint}")
    typer.echo("Secrets were written with owner-only permissions and are not shown.")


@app.command("api")
def api_command(
    host: Annotated[str | None, typer.Option(help="Address to bind (default: loopback).")] = None,
    port: Annotated[int | None, typer.Option(help="Port (default 8700).")] = None,
    tls_cert: Annotated[Path | None, typer.Option(help="TLS certificate (PEM).")] = None,
    tls_key: Annotated[Path | None, typer.Option(help="TLS private key (PEM).")] = None,
) -> None:
    """Serve the HTTP API. Refuses to serve plaintext HTTP beyond this machine."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    settings = server_settings()
    try:
        serve(
            settings,
            host or settings.api_host,
            port or settings.api_port,
            tls_cert,
            tls_key,
        )
    except UnsafeBind as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


def _describe(info: tls.CertInfo) -> list[str]:
    return [
        f"fingerprint (SHA-256): {info.fingerprint}",
        f"names:     {', '.join(info.dns_names)}",
        f"addresses: {', '.join(info.ip_addresses)}",
        f"valid:     {info.not_before:%Y-%m-%d} to {info.not_after:%Y-%m-%d}   ({info.key_type})",
    ]


@cert_app.command("show")
def cert_show(
    out: Annotated[
        Path, typer.Option(envvar="KTERM_DEPLOY_DIR", help="The deploy directory.")
    ] = Path(),
) -> None:
    """Show the server certificate's fingerprint (what clients pin), names and expiry."""
    path = out / "secrets" / tls.CERT_FILE
    try:
        info = tls.describe(path.read_bytes())
    except (OSError, tls.TlsError) as exc:
        typer.echo(f"error: cannot read a certificate at {path}: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    for line in _describe(info):
        typer.echo(line)


@cert_app.command("rotate")
def cert_rotate(
    out: Annotated[
        Path, typer.Option(envvar="KTERM_DEPLOY_DIR", help="The deploy directory.")
    ] = Path(),
    host: Annotated[list[str] | None, typer.Option("--host", help="Names (default: keep).")] = None,
    ip: Annotated[list[str] | None, typer.Option("--ip", help="Addresses (default: keep).")] = None,
) -> None:
    """Issue a new certificate (same names unless given). Restart the api service after."""
    secrets_dir = out / "secrets"
    try:
        hosts, ips = host or [], ip or []
        old = None
        if (secrets_dir / tls.CERT_FILE).exists():
            old = tls.describe((secrets_dir / tls.CERT_FILE).read_bytes())
            if not hosts and not ips:
                hosts, ips = tls.user_names(old)
        if not hosts and not ips:
            raise tls.TlsError("no existing certificate to copy names from: give --host/--ip")
        new = tls.write_certificate(secrets_dir, hosts, ips)
    except (OSError, tls.TlsError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    if old:
        typer.echo(f"old fingerprint: {old.fingerprint}")
    for line in _describe(new):
        typer.echo(line)
    typer.echo(
        "\nRestart the api service to use it. Every client that pinned the old certificate "
        "will refuse to connect until its user confirms the new fingerprint."
    )


cli_backup.register(app)  # the backup group and the restore command
cli_analyze.register(app)  # the analyze group
