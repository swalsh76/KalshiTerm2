"""A human-readable summary of the server's ``/v1/status`` report."""

from typing import Any


def _size(n: int | None) -> str:
    if n is None:
        return "?"
    for unit, step in (("GB", 1024**3), ("MB", 1024**2), ("kB", 1024)):
        if n >= step:
            return f"{n / step:.1f} {unit}"
    return f"{n} B"


def _age(seconds: float | None) -> str:
    if seconds is None:
        return "none in the last hour"
    seconds = int(seconds)
    if seconds < 120:
        return f"{seconds}s ago"
    if seconds < 7200:
        return f"{seconds // 60}m ago"
    if seconds < 172_800:
        return f"{seconds // 3600}h ago"
    return f"{seconds // 86_400}d ago"


def render(report: dict[str, Any]) -> str:
    """Problems first; then storage, streams, backup. Missing fields are skipped, so a newer or
    older server still prints something useful."""
    lines = [f"Server status {report.get('time', '')}".rstrip()]
    problems = report.get("problems") or []
    if problems:
        lines.append(f"\nNEEDS ATTENTION ({len(problems)})")
        lines += [f"  ! {p}" for p in problems]
    else:
        lines.append("\nAll checks passed.")
    storage = report.get("storage")
    if storage:
        lines.append(
            f"\nStorage  {_size(storage.get('used_bytes'))} of "
            f"{_size(storage.get('budget_bytes'))} ({storage.get('percent')}%)  "
            f"mode: {storage.get('mode')}"
        )
    streams = report.get("streams") or {}
    if streams:
        lines.append("\nStreams")
        for label, info in streams.items():
            lines.append(
                f"  {label:<20}{_age(info.get('age_seconds')):<24}"
                f"{info.get('per_second_5min', 0)}/s"
            )
    backup = report.get("backup")
    if backup:
        ok = backup.get("last_ok")
        if ok:
            lines.append(
                f"\nBackup   last good {_age(ok.get('age_seconds'))}, {_size(ok.get('size_bytes'))}"
            )
        elif backup.get("configured"):
            lines.append("\nBackup   none has completed")
    return "\n".join(lines)
