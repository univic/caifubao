# -*- coding: utf-8 -*-
"""Account-side input to a strategy decision, and its content hash.

Why this exists
---------------
"Same declaration, same snapshot, same session" does **not** determine a target
book: a stop-loss needs the recorded average cost, a maximum holding period needs
the hold count, the exit band needs the previous book, and de-risking needs the
high-water mark. None of that lives in the declaration or in the market snapshot,
so it has to be an explicit input rather than something an executor infers.

A position that could not be sold is part of this state too: the account still
holds it, so it still occupies a slot even though the target book does not want
it. Keeping it in the state — and hashing the state — is what makes a divergence
between paper and backtest attributable to a fill instead of to the decision.

This module is pure: it validates and hashes plain records, owns no storage and
knows nothing about brokers.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math

DECISION_STATE_SCHEMA = "decision-state-v1"
#: Reasons a held position could not be sold. Mirrors the ledger rejection codes.
BLOCKED_REASONS = frozenset(
    {"suspended", "limit_down", "limit_up_unknown", "unexecuted_order"}
)


def _instant(value, field: str) -> str:
    if not isinstance(value, str) or len(value) < 10:
        raise ValueError(f"{field} must be an ISO-8601 date string")
    return value


def _money(value, field: str, *, allow_zero: bool = True) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) < 0
        or (not allow_zero and float(value) == 0)
    ):
        raise ValueError(f"{field} must be a finite non-negative number")
    return float(value)


def _positive_int(value, field: str) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f"{field} must be a positive integer")
    return value


def validate_decision_state(state: dict) -> dict:
    """Copy and validate a decision state; fail closed on anything unexpected."""
    if not isinstance(state, dict):
        raise ValueError("decision state must be an object")
    allowed = {
        "schema",
        "as_of",
        "cash",
        "high_water_mark",
        "positions",
        "outstanding_orders",
        "blocked_exits",
    }
    unknown = set(state) - allowed
    if unknown:
        raise ValueError(f"unknown decision state fields: {sorted(unknown)}")
    if state.get("schema") != DECISION_STATE_SCHEMA:
        raise ValueError(f"decision state schema must be {DECISION_STATE_SCHEMA}")

    result = copy.deepcopy(state)
    _instant(result.get("as_of"), "as_of")
    result["cash"] = _money(result.get("cash"), "cash")
    result["high_water_mark"] = _money(result.get("high_water_mark"), "high_water_mark")

    positions = result.get("positions") or []
    if not isinstance(positions, list):
        raise ValueError("positions must be a list")
    seen: set[str] = set()
    for index, position in enumerate(positions):
        if not isinstance(position, dict):
            raise ValueError(f"positions[{index}] must be an object")
        extra = set(position) - {
            "stock_code",
            "quantity",
            "average_cost",
            "opened_on",
            "hold_count",
            "entry_reason",
        }
        if extra:
            raise ValueError(f"positions[{index}] has unknown fields: {sorted(extra)}")
        code = position.get("stock_code")
        if not isinstance(code, str) or not code:
            raise ValueError(f"positions[{index}].stock_code must be nonempty")
        if code in seen:
            raise ValueError(f"duplicate position for {code}")
        seen.add(code)
        _positive_int(position.get("quantity"), f"positions[{index}].quantity")
        _money(
            position.get("average_cost"),
            f"positions[{index}].average_cost",
            allow_zero=False,
        )
        # ``opened_on`` is optional: no exit rule reads it, and requiring it would
        # force the research plan path to invent a date it does not track. The
        # holding period is carried by ``hold_count``, which is the authority.
        if position.get("opened_on") is not None:
            _instant(position.get("opened_on"), f"positions[{index}].opened_on")
        _positive_int(position.get("hold_count"), f"positions[{index}].hold_count")
        if not isinstance(position.get("entry_reason"), str):
            raise ValueError(f"positions[{index}].entry_reason must be a string")

    orders = result.get("outstanding_orders") or []
    if not isinstance(orders, list):
        raise ValueError("outstanding_orders must be a list")

    blocked = result.get("blocked_exits") or {}
    if not isinstance(blocked, dict):
        raise ValueError("blocked_exits must be a map of code to reason")
    for code, reason in blocked.items():
        if not isinstance(code, str) or not code:
            raise ValueError("blocked_exits keys must be nonempty stock codes")
        if reason not in BLOCKED_REASONS:
            raise ValueError(
                f"blocked_exits[{code}] must be one of {sorted(BLOCKED_REASONS)}"
            )
        if code not in seen:
            raise ValueError(
                f"blocked_exits[{code}] names a position the state does not hold"
            )
    return result


def canonical_json(value) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def decision_state_hash(state: dict) -> str:
    """Content hash of the state, recorded with every run so drift is locatable."""
    return hashlib.sha256(
        canonical_json(validate_decision_state(state)).encode("utf-8")
    ).hexdigest()


def held_codes(state: dict) -> frozenset[str]:
    return frozenset(p["stock_code"] for p in state.get("positions") or [])


def blocked_slots(state: dict) -> int:
    """Slots still occupied by positions the account could not sell."""
    blocked = state.get("blocked_exits") or {}
    return len(set(blocked) & set(held_codes(state)))
