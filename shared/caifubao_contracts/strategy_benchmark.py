# -*- coding: utf-8 -*-
"""Benchmark contract and the failure criteria that end a strategy.

Two declarations that were missing, and both are about honesty rather than
method:

* **Benchmark.** An excess number is meaningless unless the comparison set is
  part of the declaration. The repo has reported "excess" against a same-day
  equal-weight universe in one layer and against a broader labelled pool in
  another, which is why two documents can disagree without either being wrong.
* **Kill criteria.** A strategy with no declared failure condition is never
  retired, only repeatedly observed. The criteria are evaluated against a stated
  review cadence and return ``insufficient_evidence`` until that cadence is met,
  so a short window cannot be reported as a pass.

Pure contract: no datahub, Mongo or broker imports.
"""

from __future__ import annotations

import math

BENCHMARK_STATIC_ALLOCATION = "static_allocation"
BENCHMARK_EQUAL_WEIGHT_UNIVERSE = "equal_weight_universe"
BENCHMARK_BROAD_INDEX = "broad_index"
BENCHMARK_KINDS = frozenset(
    {
        BENCHMARK_STATIC_ALLOCATION,
        BENCHMARK_EQUAL_WEIGHT_UNIVERSE,
        BENCHMARK_BROAD_INDEX,
    }
)

ON_BREACH_RETIRE = "retire"
ON_BREACH_REVIEW = "review"
ON_BREACH_ACTIONS = frozenset({ON_BREACH_RETIRE, ON_BREACH_REVIEW})

STATUS_INSUFFICIENT = "insufficient_evidence"
STATUS_PASS = "pass"
STATUS_BREACH = "breach"


def _fraction(value, field: str, *, allow_zero: bool = True) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) < 0
        or (not allow_zero and float(value) == 0)
    ):
        raise ValueError(f"{field} must be a finite non-negative number")
    return float(value)


def validate_benchmark(benchmark: dict) -> dict:
    """Validate the comparison set a strategy is measured against."""
    if not isinstance(benchmark, dict):
        raise ValueError("benchmark must be an object")
    unknown = set(benchmark) - {"kind", "assets", "index"}
    if unknown:
        raise ValueError(f"unknown benchmark fields: {sorted(unknown)}")

    kind = benchmark.get("kind")
    if kind not in BENCHMARK_KINDS:
        raise ValueError(f"benchmark.kind must be one of {sorted(BENCHMARK_KINDS)}")

    result: dict = {"kind": kind}
    if kind == BENCHMARK_STATIC_ALLOCATION:
        assets = benchmark.get("assets")
        if not isinstance(assets, dict) or not assets:
            raise ValueError(
                "benchmark.assets must be a non-empty map for static_allocation"
            )
        weights = {}
        for code, weight in assets.items():
            if not isinstance(code, str) or not code:
                raise ValueError("benchmark.assets keys must be non-empty codes")
            weights[code] = _fraction(weight, f"benchmark.assets.{code}")
        total = sum(weights.values())
        if abs(total - 1.0) > 1e-9:
            raise ValueError(
                f"benchmark.assets weights must sum to 1.0, got {total:.6f}"
            )
        result["assets"] = weights
    elif kind == BENCHMARK_BROAD_INDEX:
        index = benchmark.get("index")
        if not isinstance(index, str) or not index:
            raise ValueError("benchmark.index is required for broad_index")
        result["index"] = index
    else:
        if "assets" in benchmark or "index" in benchmark:
            raise ValueError(f"benchmark.{kind} must not carry assets or index fields")
    return result


def excess_return(portfolio_return: float, benchmark_return: float) -> float:
    """Arithmetic excess; the benchmark identity is what makes it comparable."""
    for value, field in (
        (portfolio_return, "portfolio_return"),
        (benchmark_return, "benchmark_return"),
    ):
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
        ):
            raise ValueError(f"{field} must be a finite number")
    return float(portfolio_return) - float(benchmark_return)


def validate_kill_criteria(criteria: dict) -> dict:
    """Validate the conditions under which the strategy is retired."""
    if not isinstance(criteria, dict):
        raise ValueError("kill_criteria must be an object")
    unknown = set(criteria) - {
        "min_excess_per_period",
        "max_drawdown",
        "review_after_sessions",
        "on_breach",
    }
    if unknown:
        raise ValueError(f"unknown kill_criteria fields: {sorted(unknown)}")

    for field in ("min_excess_per_period", "max_drawdown", "review_after_sessions"):
        if field not in criteria:
            raise ValueError(f"kill_criteria.{field} is required")

    drawdown = _fraction(criteria["max_drawdown"], "kill_criteria.max_drawdown")
    if drawdown == 0:
        raise ValueError("kill_criteria.max_drawdown must be greater than zero")
    sessions = criteria["review_after_sessions"]
    if type(sessions) is not int or sessions < 1:
        raise ValueError(
            "kill_criteria.review_after_sessions must be a positive integer"
        )
    action = criteria.get("on_breach", ON_BREACH_RETIRE)
    if action not in ON_BREACH_ACTIONS:
        raise ValueError(
            f"kill_criteria.on_breach must be one of {sorted(ON_BREACH_ACTIONS)}"
        )
    excess = criteria["min_excess_per_period"]
    if (
        isinstance(excess, bool)
        or not isinstance(excess, (int, float))
        or not math.isfinite(float(excess))
    ):
        raise ValueError("kill_criteria.min_excess_per_period must be a finite number")

    return {
        "min_excess_per_period": float(excess),
        "max_drawdown": drawdown,
        "review_after_sessions": sessions,
        "on_breach": action,
    }


def evaluate_kill_criteria(
    criteria: dict,
    *,
    sessions: int,
    excess_per_period: float | None = None,
    max_drawdown: float | None = None,
) -> dict:
    """Evaluate the criteria; a short history is never reported as a pass."""
    validated = validate_kill_criteria(criteria)
    if type(sessions) is not int or sessions < 0:
        raise ValueError("sessions must be a non-negative integer")

    required = validated["review_after_sessions"]
    if sessions < required:
        return {
            "status": STATUS_INSUFFICIENT,
            "sessions": sessions,
            "required_sessions": required,
            "breaches": [],
        }

    breaches = []
    if (
        excess_per_period is not None
        and float(excess_per_period) < validated["min_excess_per_period"]
    ):
        breaches.append("excess")
    if (
        max_drawdown is not None
        and abs(float(max_drawdown)) > validated["max_drawdown"]
    ):
        breaches.append("drawdown")

    if breaches:
        return {
            "status": STATUS_BREACH,
            "sessions": sessions,
            "required_sessions": required,
            "breaches": breaches,
            "recommendation": validated["on_breach"],
        }
    return {
        "status": STATUS_PASS,
        "sessions": sessions,
        "required_sessions": required,
        "breaches": [],
    }
