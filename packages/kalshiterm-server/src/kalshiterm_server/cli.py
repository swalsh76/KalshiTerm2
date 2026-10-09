"""``kterm-server`` operations CLI (grows with later slices)."""

import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import timedelta
from pathlib import Path
from typing import Annotated, Any

import typer
from kalshi_core.auth import AuthError, KalshiSigner
from kalshi_core.config import KalshiSettings
from kalshi_core.orderbook import OrderBookFeed
from kalshi_core.rest import KalshiRestClient
from kalshi_core.ws import KalshiWebSocket

from kalshiterm_server import auth, backup, db, tls
from kalshiterm_server.api.serve import UnsafeBind, serve
from kalshiterm_server.config import ServerSettings
from kalshiterm_server.governor import GB, Governor
from kalshiterm_server.ingest.backfill import GapBackfiller
from kalshiterm_server.ingest.discovery import discover, discovery_loop
from kalshiterm_server.ingest.stream import StreamIngestor
from kalshiterm_server.ingest.watchlist import WatchlistConfig, WatchlistController, load_config
from kalshiterm_server.init import InitError, run_init
from kalshiterm_server.shutdown import cancel_on_sigterm
from kalshiterm_server.status import collect_status, quick_health, render

app = typer.Typer(no_args_is_help=True, help="KalshiTerm server operations.")
db_app = typer.Typer(no_args_is_help=True, help="Database migrations.")
app.add_typer(db_app, name="db")
user_app = typer.Typer(no_args_is_help=True, help="API users.")
app.add_typer(user_app, name="user")
token_app = typer.Typer(no_args_is_help=True, help="API tokens.")
app.add_typer(token_app, name="token")
cert_app = typer.Typer(no_args_is_help=True, help="The server's TLS certificate.")
app.add_typer(cert_app, name="cert")
backup_app = typer.Typer(no_args_is_help=True, help="Database backups.")
app.add_typer(backup_app, name="backup")


def _settings() -> ServerSettings:
    return ServerSettings()  # type: ignore[call-arg]  # db_url is read from KTERM_DB_URL


def _url() -> str:
    return _settings().db_url


@db_app.command("upgrade")
def db_upgrade(revision: str = "head") -> None:
    """Apply migrations up to REVISION (default: head)."""
    db.upgrade(_url(), revision)
    typer.echo(f"database is at {revision}")


@db_app.command("downgrade")
def db_downgrade(revision: str) -> None:
    """Roll back to REVISION (e.g. -1 or a revision id)."""
    db.downgrade(_url(), revision)
    typer.echo(f"database rolled back to {revision}")


@db_app.command("current")
def db_current() -> None:
    """Show the revision the database is at."""

    async def run() -> str | None:
        engine = db.make_engine(_url())
        try:
            return await db.current_revision(engine)
        finally:
            await engine.dispose()

    typer.echo(asyncio.run(run()) or "(no migrations applied)")


@app.command("discover")
def discover_command(
    full: bool = typer.Option(False, "--full", help="Force a full refresh."),
    lookback_days: int = typer.Option(
        7, help="On the first run only: how far back to collect already-settled markets."
    ),
) -> None:
    """Run one discovery cycle: series, events and markets into the database."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per request is noise

    async def run() -> None:
        settings = KalshiSettings()
        signer = KalshiSigner.from_settings(settings) if settings.key_id else None
        engine = db.make_engine(_url())
        try:
            async with KalshiRestClient(settings, signer=signer) as rest:
                report = await discover(
                    rest, engine, first_run_lookback=timedelta(days=lookback_days), full=full
                )
        finally:
            await engine.dispose()
        for line in report.lines():
            typer.echo(line)

    asyncio.run(run())


@app.command("ingest")
def ingest_command(
    seconds: float = typer.Option(0, help="Stop after this many seconds (0 = run until stopped)."),
    stats_every: float = typer.Option(30, help="Seconds between statistics lines."),
    watch: Annotated[
        list[str] | None,
        typer.Option("--watch", help="Market ticker whose orderbook to store (repeatable)."),
    ] = None,
    snapshot_interval: float = typer.Option(
        300, help="Seconds between full orderbook snapshots of each watched market."
    ),
    watchlist: Annotated[
        Path | None,
        typer.Option(
            "--watchlist",
            exists=True,
            dir_okay=False,
            help="TOML file with [watchlist] markets / auto_top_n; re-read every cycle.",
        ),
    ] = None,
    watchlist_every: float = typer.Option(
        300, help="Seconds between watchlist updates (file re-read, top-N refresh)."
    ),
    max_backfill_hours: float = typer.Option(
        6, help="Longest outage whose missed trades are fetched from REST (older is truncated)."
    ),
    governor_every: float = typer.Option(
        600, help="Seconds between storage-governor cycles (size sample, thresholds)."
    ),
    discover_every: float = typer.Option(
        0, help="Seconds between market-discovery cycles (0 = off; production uses 900)."
    ),
) -> None:
    """Stream tickers, trades and market lifecycle events (and watched orderbooks) to the DB."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if watchlist is not None:
        try:
            load_config(watchlist)  # fail fast on a bad file; later edits are re-read live
        except ValueError as exc:
            raise typer.BadParameter(str(exc), param_hint="--watchlist") from exc

    async def run() -> None:
        cancel_on_sigterm()  # `docker stop` must drain buffered rows, not kill us mid-flush
        settings = KalshiSettings()
        signer = KalshiSigner.from_settings(settings)
        engine = db.make_engine(_url())
        try:
            async with contextlib.AsyncExitStack() as stack:
                ws = await stack.enter_async_context(KalshiWebSocket(settings, signer))
                for channel in (
                    "ticker",
                    "trade",
                    "market_lifecycle_v2",
                    "multivariate_market_lifecycle",
                ):
                    await ws.subscribe(channel)
                source: AsyncIterator[Any] = ws.messages()
                feed: OrderBookFeed | None = None
                rest = await stack.enter_async_context(KalshiRestClient(settings, signer=signer))
                if watch or watchlist:
                    # Starts empty: the controller adds the markets (an empty subscription is
                    # never sent, so it cannot mean "every market").
                    feed = await stack.enter_async_context(
                        OrderBookFeed(ws, rest, [], periodic_snapshot_interval=snapshot_interval)
                    )
                    source = feed.events()  # orderbook events plus everything else, in order
                ingestor = StreamIngestor(source, engine)
                backfiller = GapBackfiller(
                    rest, engine, ingestor, max_window=timedelta(hours=max_backfill_hours)
                )
                await backfiller.start()  # queues the gap since the last run stopped
                server = _settings()
                governor = Governor(
                    engine,
                    int(server.storage_budget_gb * GB),
                    shedder=ingestor,
                    disk_path=server.disk_check_path,
                    interval=governor_every,
                )
                tasks: list[asyncio.Task[None]] = [
                    asyncio.create_task(backfiller.run()),
                    asyncio.create_task(governor.run()),
                ]
                if discover_every > 0:  # shares the REST client, so one rate limit for all
                    tasks.append(asyncio.create_task(discovery_loop(rest, engine, discover_every)))
                if feed is not None:
                    controller = WatchlistController(
                        engine,
                        feed,
                        ingestor,
                        (lambda: load_config(watchlist)) if watchlist else WatchlistConfig,
                        extra_manual=frozenset(watch or ()),
                        interval=watchlist_every,
                    )

                    async def keep_watchlist() -> None:
                        try:
                            await controller.start()
                        except Exception:  # the periodic cycle retries
                            logging.getLogger("kalshiterm_server").exception(
                                "watchlist start failed"
                            )
                        await controller.run()

                    tasks.append(asyncio.create_task(keep_watchlist()))

                async def report() -> None:
                    while True:
                        await asyncio.sleep(stats_every)
                        typer.echo(f"stats: {ingestor.stats()} ws: {ws.stats()}")

                tasks.append(asyncio.create_task(report()))
                try:
                    if seconds > 0:
                        async with asyncio.timeout(seconds):
                            await ingestor.run()
                    else:
                        await ingestor.run()
                except TimeoutError:
                    pass
                except asyncio.CancelledError:  # SIGTERM / Ctrl-C: the ingestor has drained
                    typer.echo("stop requested: buffered rows flushed")
                finally:
                    for task in tasks:
                        task.cancel()
                typer.echo(f"final stats: {ingestor.stats()}")
        finally:
            await engine.dispose()

    asyncio.run(run())


@app.command("status")
def status_command(
    as_json: bool = typer.Option(False, "--json", help="Machine-readable output."),
) -> None:
    """Storage, ingest freshness, gaps, jobs and discovery in one report."""

    async def run() -> dict[str, object]:
        server = _settings()
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
        engine = db.make_engine(_url())
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
        typer.Option(exists=True, dir_okay=False, help="Its Ed25519 private key (PEM file)."),
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
    settings = _settings()
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


def _admin(action: Callable[..., Awaitable[object]]) -> object:
    """Run one administrative database action, reporting mistakes plainly."""

    async def run() -> object:
        engine = db.make_engine(_url())
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


# ------------------------------------------------------------------ backups

TargetOption = Annotated[
    Path | None, typer.Option("--target", help="Backup directory (default: KTERM_BACKUP_TARGET).")
]


def _target(option: Path | None) -> Path:
    chosen = option or (Path(t) if (t := _settings().backup_target) else None)
    if chosen is None:
        typer.echo("error: no backup directory: pass --target or set KTERM_BACKUP_TARGET", err=True)
        raise typer.Exit(code=1)
    return chosen


def _backup_command(action: Callable[[], object]) -> object:
    try:
        return action()
    except backup.BackupError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@backup_app.command("init-target")
def backup_init_target(target: TargetOption = None) -> None:
    """Mark an EMPTY directory as the backup target (do this once, with the NAS mounted)."""
    path = _target(target)
    _backup_command(lambda: backup.init_target(path))
    typer.echo(f"{path} is now the backup target (marker {backup.MARKER}).")


@backup_app.command("run")
def backup_run(target: TargetOption = None) -> None:
    """Take one backup now, verify it, and apply the retention rule."""
    path = _target(target)
    settings = _settings()

    async def go() -> backup.BackupResult:
        engine = db.make_engine(settings.db_url)
        try:
            return await backup.run_backup(
                engine,
                backup.PgTools(),
                settings.db_url,
                path,
                keep_daily=settings.backup_keep_daily,
                keep_weekly=settings.backup_keep_weekly,
            )
        finally:
            await engine.dispose()

    result = _backup_command(lambda: asyncio.run(go()))
    assert isinstance(result, backup.BackupResult)
    typer.echo(f"wrote {result.path} ({result.size / 2**20:.1f} MB in {result.seconds:.1f} s)")
    for name in result.deleted:
        typer.echo(f"removed old backup {name}")


@backup_app.command("list")
def backup_list(target: TargetOption = None) -> None:
    """The backups in the target directory, oldest first."""
    entries = backup.list_backups(_target(target))
    if not entries:
        typer.echo("no backups")
    for entry in entries:
        manifest = entry["manifest"] or {}
        typer.echo(
            f"{entry['file']}  {entry['size'] / 2**20:8.1f} MB  "
            f"revision {manifest.get('revision', '?')}  "
            f"{'manifest ok' if manifest else 'NO MANIFEST'}"
        )


@backup_app.command("verify")
def backup_verify(
    file: Path,
    deep: bool = typer.Option(False, help="Also restore into a scratch database and compare."),
) -> None:
    """Check a backup's size, checksum and contents; --deep proves it can be restored."""
    settings = _settings()

    def check() -> None:
        info = backup.verify_file(backup.PgTools(), settings.db_url, file)
        typer.echo(f"checksum ok ({info['sha256'][:16]}...), listing readable")
        if deep:
            problems = asyncio.run(backup.deep_verify(backup.PgTools(), settings.db_url, file))
            if problems:
                raise backup.BackupError(
                    "restore differs from the manifest: " + "; ".join(problems)
                )
            typer.echo("restored into a scratch database: counts match the manifest")

    _backup_command(check)


@backup_app.command("loop")
def backup_loop_command() -> None:
    """Back up every day at KTERM_BACKUP_AT (UTC); this is what the backup container runs."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    settings = _settings()
    path = _target(None)

    async def go() -> None:
        cancel_on_sigterm()  # a stopped container ends the loop quietly
        engine = db.make_engine(settings.db_url)
        try:
            await backup.backup_loop(
                engine,
                backup.PgTools(),
                settings.db_url,
                path,
                at=settings.backup_at,
                keep_daily=settings.backup_keep_daily,
                keep_weekly=settings.backup_keep_weekly,
                retry_minutes=settings.backup_retry_minutes,
            )
        finally:
            await engine.dispose()

    with contextlib.suppress(asyncio.CancelledError, KeyboardInterrupt):
        _backup_command(lambda: asyncio.run(go()))


@app.command("restore")
def restore_command(
    file: Path,
    database: Annotated[str, typer.Option(help="Name of the NEW database to restore into.")],
    create: bool = typer.Option(
        True, help="Create the database (else it must exist and be empty)."
    ),
) -> None:
    """Restore a backup into a new database; never touches the live one. See PLAN §10.4."""
    settings = _settings()

    def restore() -> dict[str, object]:
        return asyncio.run(
            backup.restore_backup(backup.PgTools(), settings.db_url, file, database, create=create)
        )

    manifest = _backup_command(restore)
    assert isinstance(manifest, dict)
    problems = asyncio.run(backup.compare_restored(settings.db_url, database, manifest))
    if problems:
        typer.echo("RESTORED, BUT DIFFERS FROM THE BACKUP'S MANIFEST:", err=True)
        for problem in problems:
            typer.echo(f"  ! {problem}", err=True)
        raise typer.Exit(code=1)
    typer.echo(f"restored into {database}; row counts, jobs and aggregates match the manifest")
