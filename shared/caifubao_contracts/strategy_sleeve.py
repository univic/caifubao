# -*- coding: utf-8 -*-
"""Sleeve capital shares, isolation, and horizon claims.

Two declarations the aggressive sleeve cannot exist without:

* **Capital isolation.** A sleeve's share is declared, not emergent, and a loss
  inside one sleeve must not be able to consume another's allocation. Without
  that rule a high-variance sleeve is indistinguishable from a leveraged bet on
  the whole account.
* **Horizon discipline.** A hypothesis is stated at a horizon, and evidence
  produced at a different horizon cannot support it. This repository has already
  conflated a five-factor composite scan (which only varied holding period and
  exit band) with a trend claim about a different signal; the claim has to carry
  the horizon so that substitution is a contract error rather than a footnote.

Pure contract: no datahub, Mongo or broker imports.
"""

from __future__ import annotations

import math

ON_SHORTFALL_ISOLATE = "isolate"
ON_SHORTFALL_REPORT = "report"
ON_SHORTFALL_ACTIONS = frozenset({ON_SHORTFALL_ISOLATE, ON_SHORTFALL_REPORT})

MIN_HORIZON = 1
MAX_HORIZON = 250


def _fraction(value, field: str, *, allow_zero: bool = False) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) < 0
        or float(value) > 1
        or (not allow_zero and float(value) == 0)
    ):
        raise ValueError(f"{field} must be a fraction in (0, 1]")
    return float(value)


def validate_sleeve(sleeve: dict) -> dict:
    """Validate one sleeve declaration."""
    if not isinstance(sleeve, dict):
        raise ValueError("sleeve must be an object")
    allowed = {"name", "capital_share", "accounting_id", "evidence_id", "on_shortfall"}
    unknown = set(sleeve) - allowed
    if unknown:
        raise ValueError(f"unknown sleeve fields: {sorted(unknown)}")
    for field in ("name", "accounting_id", "evidence_id"):
        value = sleeve.get(field)
        if not isinstance(value, str) or not value:
            raise ValueError(f"sleeve.{field} must be a non-empty string")
    action = sleeve.get("on_shortfall", ON_SHORTFALL_ISOLATE)
    if action not in ON_SHORTFALL_ACTIONS:
        raise ValueError(
            f"sleeve.on_shortfall must be one of {sorted(ON_SHORTFALL_ACTIONS)}"
        )
    return {
        "name": sleeve["name"],
        "capital_share": _fraction(sleeve.get("capital_share"), "sleeve.capital_share"),
        "accounting_id": sleeve["accounting_id"],
        "evidence_id": sleeve["evidence_id"],
        "on_shortfall": action,
    }


def validate_sleeves(sleeves: list[dict]) -> list[dict]:
    """Validate a sleeve set: unique names and identities, shares within capital."""
    if not isinstance(sleeves, list) or not sleeves:
        raise ValueError("sleeves must be a non-empty list")
    validated = [validate_sleeve(sleeve) for sleeve in sleeves]
    for field in ("name", "accounting_id", "evidence_id"):
        values = [sleeve[field] for sleeve in validated]
        if len(set(values)) != len(values):
            raise ValueError(f"sleeve {field} values must be unique")
    total = sum(sleeve["capital_share"] for sleeve in validated)
    if total - 1.0 > 1e-9:
        raise ValueError(f"sleeve capital shares must not exceed 1.0, got {total:.6f}")
    return validated


def allocate_sleeves(sleeves: list[dict], capital: float) -> dict:
    """Split capital by the declared shares; the remainder stays unallocated."""
    if (
        isinstance(capital, bool)
        or not isinstance(capital, (int, float))
        or not math.isfinite(float(capital))
        or float(capital) <= 0
    ):
        raise ValueError("capital must be a positive number")
    validated = validate_sleeves(sleeves)
    allocation = {
        sleeve["name"]: round(float(capital) * sleeve["capital_share"], 2)
        for sleeve in validated
    }
    allocated = sum(allocation.values())
    return {
        "sleeves": allocation,
        "allocated": round(allocated, 2),
        "unallocated": round(float(capital) - allocated, 2),
    }


def apply_losses(sleeves: list[dict], losses: dict[str, float]) -> dict:
    """Apply per-sleeve losses without letting one sleeve consume another.

    A sleeve can only lose its own allocation. Any excess is reported as a
    shortfall against that sleeve — never charged to a different one.
    """
    validated = validate_sleeves(sleeves)
    for name, amount in losses.items():
        if (
            isinstance(amount, bool)
            or not isinstance(amount, (int, float))
            or not math.isfinite(float(amount))
            or float(amount) < 0
        ):
            raise ValueError(f"loss for {name!r} must be a finite non-negative number")
    known = {sleeve["name"] for sleeve in validated}
    unknown = sorted(set(losses) - known)
    if unknown:
        raise ValueError(f"losses name unknown sleeves: {unknown}")

    applied: dict[str, float] = {}
    shortfall: list[dict] = []
    for sleeve in validated:
        name = sleeve["name"]
        amount = float(losses.get(name, 0.0))
        capacity = sleeve["capital_share"]
        if amount > capacity:
            applied[name] = capacity
            shortfall.append(
                {
                    "sleeve": name,
                    "requested": round(amount, 6),
                    "absorbed": round(capacity, 6),
                    "excess": round(amount - capacity, 6),
                    "action": sleeve["on_shortfall"],
                }
            )
        else:
            applied[name] = round(amount, 6)
    return {
        "applied": applied,
        "shortfall": shortfall,
        "breached": bool(shortfall),
        "isolated": all(item["action"] == ON_SHORTFALL_ISOLATE for item in shortfall),
    }


def validate_horizon_claim(claim: dict) -> dict:
    """Validate a horizon claim: the stated horizon and the measured one must agree."""
    if not isinstance(claim, dict):
        raise ValueError("horizon claim must be an object")
    allowed = {"hypothesis_horizon", "evidence_horizon", "rationale"}
    unknown = set(claim) - allowed
    if unknown:
        raise ValueError(f"unknown horizon claim fields: {sorted(unknown)}")
    values = {}
    for field in ("hypothesis_horizon", "evidence_horizon"):
        value = claim.get(field)
        if type(value) is not int or not MIN_HORIZON <= value <= MAX_HORIZON:
            raise ValueError(
                f"{field} must be an integer in [{MIN_HORIZON}, {MAX_HORIZON}]"
            )
        values[field] = value
    if values["hypothesis_horizon"] != values["evidence_horizon"]:
        raise ValueError(
            "evidence produced at horizon "
            f"{values['evidence_horizon']} cannot support a claim stated at "
            f"{values['hypothesis_horizon']}; re-evaluate at the claimed horizon or "
            "restate the claim"
        )
    rationale = claim.get("rationale")
    if rationale is not None and not isinstance(rationale, str):
        raise ValueError("rationale must be a string when present")
    result = dict(values)
    if rationale:
        result["rationale"] = rationale
    return result
