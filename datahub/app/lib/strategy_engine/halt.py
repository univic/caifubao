# -*- coding: utf-8 -*-
"""Fail-safe halt (kill switch) for strategy order generation.

A single persisted, default-OFF flag stops the daily runner from producing any
order. The flag lives in a JSON file whose path is supplied by the operator
(``CAIFUBAO_STRATEGY_HALT_FILE`` env var or the ``--halt-file`` CLI option);
there is NO built-in default path — an unconfigured halt store must fail loudly
rather than silently run un-halted.

The file records who set it, when, and why, so an emergency stop is auditable:

    {
      "halted": true,
      "changed_by": "operator",
      "changed_at": "2026-09-20T01:02:03+00:00",
      "reason": "manual emergency stop",
      "history": [{"action": "halt", "changed_by": ..., "changed_at": ..., "reason": ...}]
    }

Halting is *not* forward evidence: it produces no plan, never advances the
120-session counter, and never changes ``evidence_kind`` (the runner simply
fails the order-generation call). Resuming restores order generation.

Stickiness: only the *path* is mandatory. A **missing flag file reads as OFF**,
so deleting the file returns the runner to normal order generation — the halt
does not survive file loss. That is deliberate (a lost file must not stop the
system forever) and means the flag file must be treated as durable state by
operations; only an unconfigured path fails closed.
"""

from __future__ import annotations

import datetime
import json
import os
import tempfile

ENV_HALT_FILE = "CAIFUBAO_STRATEGY_HALT_FILE"

#: Stored shape for a file that has never been written (default OFF).
DEFAULT_STATE = {
    "halted": False,
    "changed_by": None,
    "changed_at": None,
    "reason": None,
    "history": [],
}

_HISTORY_LIMIT = 50


class HaltError(RuntimeError):
    """Order generation was refused because the halt flag is engaged."""


def resolve_halt_path(path: str | None = None) -> str:
    """Resolve the halt-flag path; there is no silent default.

    Precedence: explicit argument, then ``CAIFUBAO_STRATEGY_HALT_FILE``. An
    empty/blank value is an error: running order generation with an unconfigured
    kill switch would defeat it.
    """
    resolved = path or os.environ.get(ENV_HALT_FILE)
    if resolved is None or not str(resolved).strip():
        raise ValueError(
            "halt flag path is not configured; set the "
            f"{ENV_HALT_FILE} environment variable or pass halt_path explicitly "
            "(refusing to run order generation with an unconfigured kill switch)"
        )
    return str(resolved).strip()


def read_halt_state(path: str | None = None) -> dict:
    """Current halt state; a missing file reads as default OFF.

    A corrupt/unreadable file raises: an unreadable kill switch must not be
    silently interpreted as "not halted".
    """
    resolved = resolve_halt_path(path)
    if not os.path.exists(resolved):
        return dict(DEFAULT_STATE)
    try:
        with open(resolved, encoding="utf-8") as handle:
            raw = json.load(handle)
    except (OSError, ValueError) as exc:
        raise ValueError(f"cannot read halt flag file {resolved!r}: {exc}") from None
    if not isinstance(raw, dict) or "halted" not in raw:
        raise ValueError(f"halt flag file {resolved!r} is malformed")
    state = dict(DEFAULT_STATE)
    state.update(raw)
    state["halted"] = bool(state["halted"])
    if not isinstance(state.get("history"), list):
        state["history"] = []
    return state


def _write_state(path: str, state: dict) -> None:
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    # Atomic replace so a crash mid-write cannot leave a half-written flag that
    # reads as "not halted".
    handle = tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=directory, delete=False
    )
    try:
        with handle:
            json.dump(state, handle, ensure_ascii=False, indent=2, default=str)
        os.replace(handle.name, path)
    except BaseException:
        try:
            os.unlink(handle.name)
        except OSError:
            pass
        raise


def _transition(*, path, halted: bool, changed_by: str, reason: str, when=None) -> dict:
    if not changed_by or not str(changed_by).strip():
        raise ValueError("changed_by is required to change the halt flag")
    if not reason or not str(reason).strip():
        raise ValueError("reason is required to change the halt flag")
    resolved = resolve_halt_path(path)
    state = read_halt_state(resolved)
    changed_at = when or datetime.datetime.now(datetime.UTC)
    action = "halt" if halted else "resume"
    state.update(
        {
            "halted": halted,
            "changed_by": str(changed_by).strip(),
            "changed_at": changed_at.isoformat(),
            "reason": str(reason).strip(),
        }
    )
    history = list(state.get("history") or [])
    history.append(
        {
            "action": action,
            "changed_by": state["changed_by"],
            "changed_at": state["changed_at"],
            "reason": state["reason"],
        }
    )
    state["history"] = history[-_HISTORY_LIMIT:]
    _write_state(resolved, state)
    return state


def engage_halt(*, changed_by: str, reason: str, halt_path: str | None = None) -> dict:
    """Engage the kill switch (default-OFF flag -> halted)."""
    return _transition(
        path=halt_path, halted=True, changed_by=changed_by, reason=reason
    )


def resume_halt(*, changed_by: str, reason: str, halt_path: str | None = None) -> dict:
    """Lift the kill switch (halted -> default OFF)."""
    return _transition(
        path=halt_path, halted=False, changed_by=changed_by, reason=reason
    )


def is_halted(halt_path: str | None = None) -> bool:
    return bool(read_halt_state(halt_path).get("halted"))


def assert_not_halted(halt_path: str | None = None) -> None:
    """Fail loudly when the halt flag is engaged.

    Order generation MUST call this FIRST. A halt is a refusal, never a silent
    empty success: the caller reports HALTED and produces no orders.
    """
    state = read_halt_state(halt_path)
    if state.get("halted"):
        raise HaltError(
            "strategy order generation is HALTED "
            f"(by {state.get('changed_by')!r} at {state.get('changed_at')}: "
            f"{state.get('reason')!r}); no orders were produced"
        )
