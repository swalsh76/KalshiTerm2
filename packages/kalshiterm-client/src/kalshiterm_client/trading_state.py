"""Trading state that must survive a restart, kept in ``trading_state.json`` (config directory).

Three things the risk layer depends on cannot live only in memory, or restarting the program would
quietly reset a limit:

* the **day's realized-P&L baseline**: Kalshi reports cumulative realized P&L, so "today's loss"
  is the cumulative figure minus what it was when the day (UTC) began, as first seen by this client;
* the **day's transferred total**, which the per-day transfer cap is checked against;
* the **halt** set by the kill switch.

It also remembers ``client_order_id``s whose outcome was never learned (``WriteOutcomeUnknown``)
and transfers sent but not confirmed, so they are reconciled rather than forgotten.

A file that exists but cannot be read is an error, never a fresh start: resetting would erase the
halt and the day's counters, and the risk layer fails closed instead.
"""

import contextlib
import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from kalshiterm_client.profiles import ProfileError, config_dir

FILE_NAME = "trading_state.json"


class TradingStateError(ProfileError):
    """The state file is unreadable; the message says which file and what to do."""


def state_path() -> Path:
    return config_dir() / FILE_NAME


def utc_day(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%d")


@dataclass
class TradingState:
    path: Path = field(default_factory=state_path)
    day: str = ""
    realized_baseline_e6: int | None = None
    transferred_e6: int = 0
    halted: bool = False
    halt_reason: str = ""
    unknown_orders: list[str] = field(default_factory=list)
    pending_transfers: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def load(cls, path: Path | None = None) -> "TradingState":
        target = path or state_path()
        state = cls(path=target)
        try:
            raw = target.read_text(encoding="utf-8")
        except FileNotFoundError:
            return state
        except OSError as exc:
            raise TradingStateError(f"cannot read {target}: {exc.strerror}") from None
        try:
            data = json.loads(raw)
            baseline = data["realized_baseline_e6"]
            state.day = str(data["day"])
            state.realized_baseline_e6 = None if baseline is None else int(baseline)
            state.transferred_e6 = int(data["transferred_e6"])
            state.halted = bool(data["halted"])
            state.halt_reason = str(data.get("halt_reason", ""))
            state.unknown_orders = [str(x) for x in data.get("unknown_orders", [])]
            state.pending_transfers = [dict(x) for x in data.get("pending_transfers", [])]
        except (ValueError, KeyError, TypeError) as exc:
            raise TradingStateError(
                f"{target} is damaged ({type(exc).__name__}); trading is refused rather than "
                "guessing the day's limits and the halt. Check the file; delete it only if you "
                "are sure no halt was set and no transfers are pending."
            ) from None
        return state

    def save(self) -> None:
        data = {
            "day": self.day,
            "realized_baseline_e6": self.realized_baseline_e6,
            "transferred_e6": self.transferred_e6,
            "halted": self.halted,
            "halt_reason": self.halt_reason,
            "unknown_orders": self.unknown_orders,
            "pending_transfers": self.pending_transfers,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.path.parent, prefix=".trading_state-")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(data, handle, indent=2)
            with contextlib.suppress(OSError):  # not on Windows
                os.chmod(tmp, 0o600)
            os.replace(tmp, self.path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            raise

    def roll(self, today: str) -> bool:
        """Start a new day: counters reset; the halt and unresolved items stay. True if rolled."""
        if self.day == today:
            return False
        self.day = today
        self.realized_baseline_e6 = None
        self.transferred_e6 = 0
        self.save()
        return True

    def set_baseline(self, realized_e6: int) -> None:
        """Record today's starting cumulative realized P&L (only if not already recorded)."""
        if self.realized_baseline_e6 is None:
            self.realized_baseline_e6 = realized_e6
            self.save()

    def realized_today_e6(self, realized_now_e6: int) -> int:
        baseline = self.realized_baseline_e6
        return 0 if baseline is None else realized_now_e6 - baseline

    def begin_transfer(
        self, transfer_id: str, source: int, destination: int, amount_e6: int
    ) -> None:
        """Count a transfer against the day and remember it until it is confirmed. Saved before
        the request is sent, so a crash in between still counts it."""
        self.transferred_e6 += amount_e6
        self.pending_transfers.append(
            {
                "id": transfer_id,
                "source": source,
                "destination": destination,
                "amount_e6": amount_e6,
            }
        )
        self.save()

    def finish_transfer(self, transfer_id: str) -> None:
        self.pending_transfers = [t for t in self.pending_transfers if t.get("id") != transfer_id]
        self.save()

    def abort_transfer(self, transfer_id: str) -> None:
        """A transfer that Kalshi definitely refused: take it back out of the day's total."""
        for item in self.pending_transfers:
            if item.get("id") == transfer_id:
                self.transferred_e6 = max(0, self.transferred_e6 - int(item["amount_e6"]))
        self.finish_transfer(transfer_id)

    def halt(self, reason: str) -> None:
        self.halted = True
        self.halt_reason = reason
        self.save()

    def clear_halt(self) -> None:
        self.halted = False
        self.halt_reason = ""
        self.save()

    def note_unknown_order(self, client_order_id: str) -> None:
        if client_order_id not in self.unknown_orders:
            self.unknown_orders.append(client_order_id)
            self.save()

    def resolve_unknown_order(self, client_order_id: str) -> None:
        if client_order_id in self.unknown_orders:
            self.unknown_orders.remove(client_order_id)
            self.save()
