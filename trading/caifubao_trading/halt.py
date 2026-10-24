"""Trading-domain operational halt authority, not a research snapshot.

A missing/corrupt store FAILS CLOSED (halted). To arm PAPER mode, operations
must initialize a dedicated store and explicitly resume with attribution.
"""

from __future__ import annotations

import copy
import datetime as dt
import json
import os
from pathlib import Path
import tempfile


class TradingHaltStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        if not str(path).strip():
            raise ValueError("trading halt path is required")

    def snapshot(self) -> dict:
        try:
            with self.path.open(encoding="utf-8") as stream:
                state = json.load(stream)
        except FileNotFoundError:
            return {"halted": True, "reason": "UNINITIALIZED"}
        except (OSError, ValueError) as exc:
            raise ValueError("trading halt state unreadable: FAIL_CLOSED") from exc
        if (
            not isinstance(state, dict)
            or type(state.get("halted")) is not bool
            or not isinstance(state.get("history"), list)
        ):
            raise ValueError("trading halt state malformed: FAIL_CLOSED")
        return copy.deepcopy(state)

    def change(
        self, *, halted: bool, actor: str, reason: str, at: dt.datetime
    ) -> dict:
        if type(halted) is not bool:
            raise ValueError("halted must be a boolean")
        if not isinstance(actor, str) or not actor.strip():
            raise ValueError("actor is required")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason is required")
        if not isinstance(at, dt.datetime) or at.tzinfo is None:
            raise ValueError("at must be timezone-aware")
        prior = self.snapshot()
        if "history" not in prior:
            # First write is ALWAYS a stop, never silent activation.
            if not halted:
                raise ValueError("initialize trading halt as engaged first")
            prior = {"halted": True, "history": []}
        transition = {
            "halted": halted,
            "actor": actor.strip(),
            "reason": reason.strip(),
            "at": at.astimezone(dt.timezone.utc).isoformat(),
        }
        current = {
            **transition,
            "history": [*prior["history"], transition],
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix=".trading-halt-", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(current, stream, sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, self.path)
        finally:
            if os.path.exists(name):
                os.unlink(name)
        return copy.deepcopy(current)

    def assert_orders_allowed(self) -> None:
        state = self.snapshot()
        if state["halted"]:
            raise ValueError("TRADING_HALTED: " + state.get("reason", "halted"))
