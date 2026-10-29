# -*- coding: utf-8 -*-
"""Allocation-family book semantics: fixed asset set, target weights, bands.

The rank family exits on ranking; an allocation book has no ranking to exit on.
Its contract is instead a fixed asset set with target weights, a rebalance band,
and a cadence that is genuinely honoured.

Cadence matters because it was the original defect: ``rebalance.cadence_days``
was validated, hashed and documented while every session re-planned the book.
Here it is read: a session that is not a rebalance session may only act on a
stop-loss or on a holding that is no longer part of the declared asset set. Band
rebalancing waits for the next scheduled session.

Pure contract: no datahub, Mongo or broker imports.
"""

from __future__ import annotations

import math

FAMILY_ASSET_ALLOCATION = "asset_allocation"
#: Bands are absolute weight deviations, in (0, 1).
MAX_BAND = 0.5
#: Cash is the remainder once the declared weights are placed.
WEIGHT_SUM_TOLERANCE = 1e-9


def _fraction(value, field: str, *, allow_zero: bool = True) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) < 0
        or float(value) > 1
        or (not allow_zero and float(value) == 0)
    ):
        raise ValueError(f"{field} must be a finite fraction in [0, 1]")
    return float(value)


def validate_allocation(allocation: dict) -> dict:
    """Validate a fixed asset set with target weights and a rebalance band."""
    if not isinstance(allocation, dict):
        raise ValueError("allocation must be an object")
    unknown = set(allocation) - {"assets", "band_pct"}
    if unknown:
        raise ValueError(f"unknown allocation fields: {sorted(unknown)}")

    assets = allocation.get("assets")
    if not isinstance(assets, dict) or not assets:
        raise ValueError("allocation.assets must be a non-empty map of code to weight")
    targets: dict[str, float] = {}
    for code, weight in assets.items():
        if not isinstance(code, str) or not code:
            raise ValueError("allocation.assets keys must be non-empty stock codes")
        targets[code] = _fraction(weight, f"allocation.assets.{code}")
    total = sum(targets.values())
    if total - 1.0 > WEIGHT_SUM_TOLERANCE:
        raise ValueError(
            f"allocation.assets weights must not exceed 1.0, got {total:.6f}"
        )

    band = allocation.get("band_pct")
    if band is None:
        raise ValueError("allocation.band_pct is required")
    band = _fraction(band, "allocation.band_pct", allow_zero=False)
    if band > MAX_BAND:
        raise ValueError(f"allocation.band_pct must not exceed {MAX_BAND}")
    return {"assets": targets, "band_pct": band}


def should_rebalance(sessions_since_rebalance: int, cadence_days: int) -> bool:
    """Whether this session is a scheduled rebalance session.

    ``cadence_days`` is read here — that is the whole point. A declaration that
    asks for a five-session cadence trades on the fifth session, not every one.
    """
    if type(sessions_since_rebalance) is not int or sessions_since_rebalance < 0:
        raise ValueError("sessions_since_rebalance must be a non-negative integer")
    if type(cadence_days) is not int or cadence_days < 1:
        raise ValueError("cadence_days must be a positive integer")
    return sessions_since_rebalance >= cadence_days


def plan_allocation(
    *,
    targets: dict[str, float],
    band_pct: float,
    current_weights: dict[str, float],
    marks: dict[str, float] | None = None,
    cost_basis: dict[str, float] | None = None,
    stop_loss_pct: float | None = None,
    rebalance_session: bool = True,
) -> dict:
    """Return target weights and the action taken for each asset.

    Actions: ``hold`` (inside the band), ``rebalance`` (outside the band, moved
    back to target), ``open`` (targeted but not held), ``close`` (held but no
    longer in the declared set) and ``stop_out`` (per-asset stop breached).
    Only a rebalance session may produce ``rebalance``; stops and closures are
    evaluated every session.
    """
    prices = marks or {}
    basis = cost_basis or {}
    actions: dict[str, str] = {}
    result: dict[str, float] = {}

    for code, weight in targets.items():
        held = code in current_weights
        mark = prices.get(code)
        cost = basis.get(code)
        if (
            stop_loss_pct is not None
            and held
            and mark is not None
            and cost
            and float(mark) < float(cost) * (1.0 - float(stop_loss_pct))
        ):
            actions[code] = "stop_out"
            result[code] = 0.0
            continue
        if not held:
            actions[code] = "open"
            result[code] = weight
            continue
        drift = abs(float(current_weights[code]) - weight)
        if drift > band_pct and rebalance_session:
            actions[code] = "rebalance"
            result[code] = weight
        else:
            actions[code] = "hold"
            result[code] = float(current_weights[code])

    for code in current_weights:
        if code in targets:
            continue
        actions[code] = "close"
        result[code] = 0.0

    invested = sum(result.values())
    return {
        "targets": result,
        "actions": actions,
        "cash_weight": max(0.0, 1.0 - invested),
        "rebalance_session": bool(rebalance_session),
    }
