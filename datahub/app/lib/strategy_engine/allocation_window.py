# -*- coding: utf-8 -*-
"""Allocation forward windows: certify, close and count.

Plan C of the window-key decision: an allocation strategy has no scoring model, so
its evidence clock lives in its own collection keyed by ``strategy_id``. Three
properties matter more than the plumbing:

* **Isolation.** Certifying or closing one strategy must not touch another's
  window, and must never appear in scoring-window progress.
* **The clock is the scarce resource.** A window is refused when no plan path can
  fill it, because opening one without a way to produce plans burns 120 sessions
  that cannot be backfilled.
* **No promotion by accident.** A window is research evidence; it grants no
  execution authority.

The pure functions below take plain records so they are testable without Mongo;
the thin glue at the bottom is what touches the database.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

REQUIRED_SESSIONS = 120


class AllocationWindowError(ValueError):
    """A window rule was violated; the caller must not treat it as a warning."""


def _as_date(value, field: str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str) and len(value) >= 10:
        try:
            return date.fromisoformat(value[:10])
        except ValueError as exc:
            raise AllocationWindowError(f"{field} must be an ISO-8601 date") from exc
    raise AllocationWindowError(f"{field} must be an ISO-8601 date")


def validate_certification(
    spec: dict, *, strategy_id: str, has_plan_path: bool
) -> dict:
    """Return the identity a window would be certified with, or refuse.

    ``has_plan_path`` is the caller's assertion that this declaration can actually
    produce plans today. Without it the window would start a clock nothing fills.
    """
    if not isinstance(spec, dict):
        raise AllocationWindowError("spec must be an object")
    if not isinstance(strategy_id, str) or not strategy_id.strip():
        raise AllocationWindowError("strategy_id must be a non-empty string")
    if spec.get("spec_version") != "2":
        raise AllocationWindowError("allocation windows require a v2 declaration")
    if spec.get("family") != "asset_allocation":
        raise AllocationWindowError(
            "only asset_allocation declarations use an allocation window; "
            "rank-selection strategies are keyed by (model_version, horizon)"
        )
    if not isinstance(spec.get("accounting"), dict) or not spec["accounting"]:
        raise AllocationWindowError(
            "an allocation window needs the accounting policy it will be measured under"
        )
    if not isinstance(spec.get("benchmark"), dict) or not spec["benchmark"]:
        raise AllocationWindowError(
            "an allocation window needs its benchmark: excess is meaningless without it"
        )
    if not has_plan_path:
        raise AllocationWindowError(
            "refusing to certify: no allocation plan path exists yet, so the "
            "120-session clock would start with nothing able to fill it"
        )
    return {
        "strategy_id": strategy_id.strip(),
        "accounting_policy": dict(spec["accounting"]),
        "benchmark": dict(spec["benchmark"]),
        "family": spec["family"],
    }


def plan_certification(
    *,
    identity: dict,
    spec_hash: str,
    active_windows: list[dict],
    today,
) -> dict:
    """Decide which predecessor to close and what the new window looks like."""
    if not isinstance(spec_hash, str) or len(spec_hash) < 8:
        raise AllocationWindowError("spec_hash must be a non-empty digest")
    today = _as_date(today, "today")

    to_close = []
    for window in active_windows or []:
        if window.get("status") != "ACTIVE":
            continue
        # Only the same strategy's clock is replaced; another sleeve keeps running.
        if window.get("strategy_id") == identity["strategy_id"]:
            to_close.append(window.get("strategy_id"))

    return {
        "close": sorted(set(to_close)),
        "window": {
            "strategy_id": identity["strategy_id"],
            "spec_hash": spec_hash,
            "hash_kind": "spec",
            "accounting_policy": identity["accounting_policy"],
            "benchmark": identity["benchmark"],
            "start_date": today.isoformat(),
            "status": "ACTIVE",
        },
    }


def allocation_forward_progress(
    *,
    window: dict,
    decided_dates: list[str],
    trading_days: list[str],
) -> dict:
    """Count distinct decision dates inside the window and report the gaps.

    ``decided_dates`` are the dates of plans that qualify (same strategy and spec
    hash, decision before execution). ``trading_days`` is the session calendar the
    window is measured against.
    """
    if not isinstance(window, dict):
        raise AllocationWindowError("window must be an object")
    start = _as_date(window.get("start_date"), "window.start_date")
    status = window.get("status")
    if status not in ("ACTIVE", "CLOSED"):
        raise AllocationWindowError("window.status must be ACTIVE or CLOSED")

    end = (
        _as_date(window["closed_at"], "window.closed_at")
        if window.get("closed_at")
        else None
    )
    sessions = [
        day
        for day in sorted(set(trading_days or []))
        if _as_date(day, "trading_days") >= start
        and (end is None or _as_date(day, "trading_days") <= end)
    ]
    counted = {
        _as_date(day, "decided_dates").isoformat()
        for day in (decided_dates or [])
        if _as_date(day, "decided_dates") >= start
        and (end is None or _as_date(day, "decided_dates") <= end)
    }
    counted &= set(sessions)

    gaps = [day for day in sessions if day not in counted]
    leading = 0
    for day in sessions:
        if day in counted:
            leading += 1
        else:
            break
    return {
        "strategy_id": window.get("strategy_id"),
        "status": status,
        "start_date": start.isoformat(),
        "observed_sessions": len(counted),
        "required_sessions": REQUIRED_SESSIONS,
        "remaining_sessions": max(0, REQUIRED_SESSIONS - len(counted)),
        "sessions_in_window": len(sessions),
        "contiguous_leading_span": leading,
        "gaps": gaps,
        "complete": len(counted) >= REQUIRED_SESSIONS,
    }


# --------------------------------------------------------------------------
# database glue
# --------------------------------------------------------------------------


def certify_allocation_window(
    *,
    spec: dict,
    strategy_id: str,
    spec_hash: str,
    has_plan_path: bool,
    today=None,
) -> dict:
    """Open the window, closing only this strategy's predecessor."""
    from app.model.strategy import AllocationForwardWindow

    identity = validate_certification(
        spec, strategy_id=strategy_id, has_plan_path=has_plan_path
    )
    today = today or datetime.now(timezone.utc).date()
    active = list(
        AllocationForwardWindow.objects(
            strategy_id=identity["strategy_id"], status="ACTIVE"
        )
    )
    decision = plan_certification(
        identity=identity,
        spec_hash=spec_hash,
        active_windows=[
            {"strategy_id": window.strategy_id, "status": window.status}
            for window in active
        ],
        today=today,
    )
    now = datetime.now(timezone.utc)
    closed = []
    for window in active:
        window.status = "CLOSED"
        window.closed_at = now
        window.save()
        closed.append(window.spec_hash)
    created = AllocationForwardWindow(
        strategy_id=identity["strategy_id"],
        spec_hash=spec_hash,
        hash_kind="spec",
        accounting_policy=identity["accounting_policy"],
        benchmark=identity["benchmark"],
        start_date=datetime.combine(
            date.fromisoformat(decision["window"]["start_date"]),
            datetime.min.time(),
            tzinfo=timezone.utc,
        ),
        status="ACTIVE",
        decision_at=now,
    )
    created.save()
    return {
        "strategy_id": identity["strategy_id"],
        "spec_hash": spec_hash,
        "start_date": decision["window"]["start_date"],
        "status": "ACTIVE",
        "closed_predecessors": sorted(closed),
        "decision_at": now,
    }


def close_allocation_window(*, strategy_id: str) -> dict:
    """Close the ACTIVE window for one strategy; a no-op if none is open."""
    from app.model.strategy import AllocationForwardWindow

    if not isinstance(strategy_id, str) or not strategy_id.strip():
        raise AllocationWindowError("strategy_id must be a non-empty string")
    now = datetime.now(timezone.utc)
    closed = []
    for window in AllocationForwardWindow.objects(
        strategy_id=strategy_id.strip(), status="ACTIVE"
    ):
        window.status = "CLOSED"
        window.closed_at = now
        window.save()
        closed.append(window.spec_hash)
    return {
        "strategy_id": strategy_id.strip(),
        "closed": sorted(closed),
        "closed_at": now,
    }
