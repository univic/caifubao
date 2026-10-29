# -*- coding: utf-8 -*-
"""Structural and capacity gates, evaluated before a candidate may be promoted.

Two questions that a backtest result cannot answer on its own:

* **Structure.** Is this a book anybody should run? A five-name book with no
  stop-loss and no turnover budget can post a great number and still be a bad
  strategy. The floors are declared, so changing them changes the verdict.
* **Capacity.** A small-capital edge that disappears at ten times the capital is
  a capacity artifact, not an edge. The small account is the one place this can
  be tested cheaply, so it is a gate rather than a footnote.

Both return findings rather than a boolean, because "rejected" with no reason is
indistinguishable from a bug. Neither decides alone: the pipeline combines them.
"""

from __future__ import annotations

import math

FINDING_MIN_HOLDINGS = "min_holdings"
FINDING_MAX_TURNOVER = "max_turnover"
FINDING_DRAWDOWN_BUDGET = "drawdown_budget"
FINDING_STOP_LOSS = "stop_loss_missing"
FINDING_CAPACITY = "capacity_collapse"


def _finite(value, field: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
    ):
        raise ValueError(f"{field} must be a finite number")
    return float(value)


def check_structure(
    *,
    holdings: int,
    min_holdings: int,
    annual_turnover: float,
    max_annual_turnover: float,
    max_drawdown: float,
    drawdown_budget: float,
    has_stop_loss: bool,
) -> dict:
    """Structural gate: diversity, turnover, drawdown and a declared stop."""
    if type(holdings) is not int or holdings < 0:
        raise ValueError("holdings must be a non-negative integer")
    if type(min_holdings) is not int or min_holdings < 1:
        raise ValueError("min_holdings must be a positive integer")
    turnover = _finite(annual_turnover, "annual_turnover")
    turnover_cap = _finite(max_annual_turnover, "max_annual_turnover")
    drawdown = abs(_finite(max_drawdown, "max_drawdown"))
    budget = _finite(drawdown_budget, "drawdown_budget")
    if turnover < 0:
        raise ValueError("annual_turnover must be non-negative")
    if budget <= 0:
        raise ValueError("drawdown_budget must be greater than zero")
    if not isinstance(has_stop_loss, bool):
        raise ValueError("has_stop_loss must be a boolean")

    findings = []
    if holdings < min_holdings:
        findings.append(
            {
                "code": FINDING_MIN_HOLDINGS,
                "detail": f"{holdings} holdings is below the declared floor {min_holdings}",
            }
        )
    if turnover > turnover_cap:
        findings.append(
            {
                "code": FINDING_MAX_TURNOVER,
                "detail": f"annual turnover {turnover:.2f} exceeds {turnover_cap:.2f}",
            }
        )
    if drawdown > budget:
        findings.append(
            {
                "code": FINDING_DRAWDOWN_BUDGET,
                "detail": f"drawdown {drawdown:.4f} exceeds the budget {budget:.4f}",
            }
        )
    if not has_stop_loss:
        findings.append(
            {
                "code": FINDING_STOP_LOSS,
                "detail": "no stop-loss was declared; a falling book has no exit",
            }
        )
    return {"passed": not findings, "findings": findings}


def evaluate_capacity(
    *,
    base_capital: float,
    scaled_capital: float,
    base_metric: float,
    scaled_metric: float,
    min_retained_ratio: float,
) -> dict:
    """Capacity gate: does the result survive at a larger capital?

    ``*_metric`` is whatever the candidate is judged on (excess per period, net
    annual return). ``min_retained_ratio`` is the share of the base metric that
    must remain at the larger capital.
    """
    base_capital = _finite(base_capital, "base_capital")
    scaled_capital = _finite(scaled_capital, "scaled_capital")
    if base_capital <= 0 or scaled_capital <= base_capital:
        raise ValueError("scaled_capital must exceed a positive base_capital")
    base = _finite(base_metric, "base_metric")
    scaled = _finite(scaled_metric, "scaled_metric")
    ratio_floor = _finite(min_retained_ratio, "min_retained_ratio")
    if not 0 <= ratio_floor <= 1:
        raise ValueError("min_retained_ratio must be in [0, 1]")

    if base <= 0:
        # A non-positive base cannot be scaled down; there is nothing to retain.
        return {
            "passed": False,
            "retained_ratio": 0.0,
            "findings": [
                {
                    "code": FINDING_CAPACITY,
                    "detail": f"base metric {base:.6f} is not positive; capacity is moot",
                }
            ],
        }

    retained = scaled / base
    findings = []
    if retained < ratio_floor:
        findings.append(
            {
                "code": FINDING_CAPACITY,
                "detail": (
                    f"retained {retained:.3f} of the base metric at "
                    f"{scaled_capital / base_capital:.1f}x capital, below the floor "
                    f"{ratio_floor:.3f}"
                ),
            }
        )
    return {
        "passed": not findings,
        "retained_ratio": round(retained, 6),
        "findings": findings,
    }
