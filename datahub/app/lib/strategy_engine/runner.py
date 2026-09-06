# -*- coding: utf-8 -*-
"""Daily-run orchestration for the paper-first strategy runner.

Pure, dependency-injected: the caller (jobs/strategy_runner) supplies concrete
query functions for VERIFIED predictions and stock flags; this module assembles
a daily plan (eligible universe -> target holdings -> rebalance vs previous),
decides skip, and never touches Mongo itself. So the full decision path is
unit-testable without a database.
"""

from __future__ import annotations

import datetime

from app.lib.strategy_engine.config import (
    DEFAULT_HORIZON,
    validate_strategy_config,
)
from app.lib.strategy_engine.selection import compute_rebalance, select_target_holdings

# Prediction record field for the eligibility source of truth. The runner maps
# quote/stock rows onto this shape.
STOCK_FLAG_KEYS = ("is_st", "is_bse", "trade_status")


def eligible_codes_from_flags(
    flags: dict[str, dict],  # stock_code -> {is_st?, is_bse?, trade_status?}
    config: dict,
) -> set[str]:
    """Return stock codes that pass the configured eligibility constraints.

    flags maps stock_code to a dict with optional keys is_st (bool/int),
    is_bse (bool/int), trade_status (1 = tradable). Codes missing from flags
    are NOT eligible (unknown liquidity/status is fail-closed). A code with no
    flags entry but present in the prediction set is excluded — the runner must
    pass the full tradable-universe flag map for the date.
    """
    config = validate_strategy_config(config)
    constraints = config["constraints"]

    def _truthy(value) -> bool:
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        return bool(int(value))

    eligible = set()
    for code, row in flags.items():
        if constraints.get("exclude_st") and _truthy(row.get("is_st")):
            continue
        if constraints.get("exclude_bse") and _truthy(row.get("is_bse")):
            continue
        if constraints.get("exclude_suspended") and _truthy(
            row.get("trade_status", 1) != 1
        ):
            continue
        eligible.add(code)
    return eligible


def assemble_daily_plan(
    *,
    config: dict,
    date: datetime.datetime,
    predictions,  # iterable of VERIFIED predictions for the configured version
    previous_holdings: list[dict] | None,
    flags: dict[str, dict] | None = None,
    horizon: int | None = None,
) -> dict:
    """Build one day's paper plan.

    Returns {"skipped": bool, "reason"?: str, "date", "horizon",
    "target_holdings": [...], "rebalance": {...}}. When no VERIFIED predictions
    exist for the configured model version, the plan is skipped (no empty
    portfolio is written).
    """
    config = validate_strategy_config(config)
    horizon = int(horizon or config.get("horizon", DEFAULT_HORIZON))
    prediction_list = list(predictions)
    if not prediction_list:
        return {
            "skipped": True,
            "reason": (
                f"no VERIFIED predictions for model_version="
                f"{config['score_model_version']} on {date.date()} "
                f"(horizon {horizon})"
            ),
            "date": date,
            "horizon": horizon,
        }

    eligible = eligible_codes_from_flags(flags, config) if flags is not None else None
    target = select_target_holdings(prediction_list, config, eligible_codes=eligible)
    if not target:
        return {
            "skipped": True,
            "reason": "selection produced no eligible holdings",
            "date": date,
            "horizon": horizon,
        }

    return {
        "skipped": False,
        "date": date,
        "horizon": horizon,
        "target_holdings": target,
        "rebalance": compute_rebalance(previous_holdings, target),
    }
