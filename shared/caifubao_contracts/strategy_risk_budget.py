# -*- coding: utf-8 -*-
"""Pre-trade risk budget: the ``risk`` block finally does something.

Every field here was validated, hashed and documented while no consumer read it —
the same defect as ``rebalance.cadence_days``, repeated seven times. This module
is the consumer. It answers one question before a target book is acted on: does
this book fit the declared budget, and if not, by how much must it shrink?

Ordering matters and is deliberate:

1. a breach of cash floor or drawdown is a **stop**, not a scale — the book is
   refused;
2. a regime gate can force a reduction even when nothing else is breached;
3. position count, gross exposure and single-name caps **scale** the book down,
   because those are sizing limits rather than solvency limits;
4. turnover is checked last: a book that must trade more than the budget allows
   is refused rather than silently trimmed, because trimming a rebalance changes
   which names are held.

Pure contract: no datahub, Mongo or broker imports.
"""

from __future__ import annotations

import math

REGIME_NONE = "none"
REGIME_BULL_ONLY = "bull_only"
REGIME_BEAR_REDUCE_ONLY = "bear_reduce_only"
REGIME_POLICIES = frozenset({REGIME_NONE, REGIME_BULL_ONLY, REGIME_BEAR_REDUCE_ONLY})

BULL = "bull"
BEAR = "bear"
SIDEWAYS = "sideways"
REGIMES = frozenset({BULL, BEAR, SIDEWAYS})

#: Fraction of gross exposure kept when a bear regime forces de-risking.
BEAR_REDUCTION = 0.5


def _fraction(value, field: str) -> float | None:
    if value is None:
        return None
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or not 0 <= float(value) < 1
    ):
        raise ValueError(f"{field} must be a fraction in [0, 1)")
    return float(value)


def validate_risk_budget(risk: dict) -> dict:
    """Validate the budget fields this module consumes."""
    if not isinstance(risk, dict):
        raise ValueError("risk must be an object")
    result = {
        "max_turnover_pct": _fraction(
            risk.get("max_turnover_pct"), "risk.max_turnover_pct"
        ),
        "cash_floor_pct": _fraction(risk.get("cash_floor_pct"), "risk.cash_floor_pct"),
        "max_drawdown_pct": _fraction(
            risk.get("max_drawdown_pct"), "risk.max_drawdown_pct"
        ),
        "max_single_stock_pct": _fraction(
            risk.get("max_single_stock_pct"), "risk.max_single_stock_pct"
        ),
        "max_gross_exposure_pct": _fraction(
            risk.get("max_gross_exposure_pct"), "risk.max_gross_exposure_pct"
        ),
    }
    positions = risk.get("max_positions")
    if positions is not None and (type(positions) is not int or positions < 1):
        raise ValueError("risk.max_positions must be a positive integer or null")
    result["max_positions"] = positions
    policy = risk.get("regime_policy", REGIME_NONE)
    if policy not in REGIME_POLICIES:
        raise ValueError(f"risk.regime_policy must be one of {sorted(REGIME_POLICIES)}")
    result["regime_policy"] = policy
    return result


def check_budget(
    *,
    risk: dict,
    position_count: int,
    gross_exposure_pct: float,
    cash_pct: float,
    turnover_pct: float,
    drawdown_pct: float,
    largest_weight_pct: float,
    regime: str = SIDEWAYS,
) -> dict:
    """Return whether the book is admissible, and how much to scale it down.

    ``scale`` is the factor to apply to every weight; ``1.0`` means unchanged.
    A ``stop`` means the book must not be acted on at all.
    """
    considered = validate_risk_budget(risk)
    if type(position_count) is not int or position_count < 0:
        raise ValueError("position_count must be a non-negative integer")
    for value, field in (
        (gross_exposure_pct, "gross_exposure_pct"),
        (cash_pct, "cash_pct"),
        (turnover_pct, "turnover_pct"),
        (largest_weight_pct, "largest_weight_pct"),
        (abs(float(drawdown_pct)), "drawdown_pct"),
    ):
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or float(value) < 0
        ):
            raise ValueError(f"{field} must be a finite non-negative number")
    if regime not in REGIMES:
        raise ValueError(f"regime must be one of {sorted(REGIMES)}")

    stops: list[str] = []
    violations: list[str] = []
    scale = 1.0

    floor = considered["cash_floor_pct"]
    if floor is not None and float(cash_pct) < floor:
        stops.append("cash_floor_pct")
    drawdown_cap = considered["max_drawdown_pct"]
    if drawdown_cap is not None and abs(float(drawdown_pct)) > drawdown_cap:
        stops.append("max_drawdown_pct")

    policy = considered["regime_policy"]
    if policy == REGIME_BULL_ONLY and regime != BULL:
        stops.append("regime_bull_only")
    elif policy == REGIME_BEAR_REDUCE_ONLY and regime == BEAR:
        scale = min(scale, BEAR_REDUCTION)

    max_positions = considered["max_positions"]
    truncate_to = None
    if max_positions is not None and position_count > max_positions:
        violations.append("max_positions")
        # Scaling every weight down leaves the *count* unchanged, so the cap
        # cannot be met that way: the caller must drop the smallest names.
        truncate_to = max_positions

    gross_cap = considered["max_gross_exposure_pct"]
    if gross_cap is not None and float(gross_exposure_pct) > gross_cap > 0:
        violations.append("max_gross_exposure_pct")
        scale = min(scale, gross_cap / float(gross_exposure_pct))

    single_cap = considered["max_single_stock_pct"]
    if single_cap is not None and float(largest_weight_pct) > single_cap:
        violations.append("max_single_stock_pct")
        scale = min(scale, single_cap / float(largest_weight_pct))

    turnover_cap = considered["max_turnover_pct"]
    if turnover_cap is not None and float(turnover_pct) > turnover_cap:
        violations.append("max_turnover_pct")

    admissible = not stops and "max_turnover_pct" not in violations
    return {
        "admissible": admissible,
        "stops": stops,
        "violations": violations,
        "scale": round(scale, 6) if admissible else 0.0,
        "truncate_to": truncate_to,
        "regime": regime,
    }
