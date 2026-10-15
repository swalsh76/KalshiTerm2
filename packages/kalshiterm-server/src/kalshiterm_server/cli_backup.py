"""``kterm-server backup ...`` and ``kterm-server restore``."""

import asyncio
import contextlib
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Annotated

import typer

from kalshiterm_server import backup, db
from kalshiterm_server.cli_common import server_settings
from kalshiterm_server.shutdown import cancel_on_sigterm

backup_app = typer.Typer(no_args_is_help=True, help="Database backups.")


TargetOption = Annotated[
    Path | None, typer.Option("--target", help="Backup directory (default: KTERM_BACKUP_TARGET).")
]


def _target(option: Path | None) -> Path:
    chosen = option or (Path(t) if (t := server_settings().backup_target) else None)
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
    settings = server_settings()

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
    settings = server_settings()

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
    settings = server_settings()
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


def restore_command(
    file: Path,
    database: Annotated[str, typer.Option(help="Name of the NEW database to restore into.")],
    create: bool = typer.Option(
        True, help="Create the database (else it must exist and be empty)."
    ),
) -> None:
    """Restore a backup into a new database; never touches the live one. See PLAN §10.4."""
    settings = server_settings()

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


def register(app: typer.Typer) -> None:
    app.add_typer(backup_app, name="backup")
    app.command("restore")(restore_command)
