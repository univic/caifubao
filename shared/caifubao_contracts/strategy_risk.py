# -*- coding: utf-8 -*-
"""Pure portfolio-level pre-trade risk checks; no account or broker I/O.

Risk approval is never a simulated fill. Cash and position reservations made
within this decision batch are provisional. Actual account changes belong to
the caller's domain-local fill ledger.
"""

from __future__ import annotations

import copy
import math


def _nonnegative(value, field: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value < 0
    ):
        raise ValueError(f"{field} must be finite and non-negative")
    return float(value)


def evaluate_risk(
    *,
    intents: list[dict],
    account: dict,
    policy: dict,
    halted: bool = False,
    market: dict | None = None,
) -> dict:
    """Evaluate sell-first, then higher-priority buys, without mutating inputs.

    policy fields: max_turnover_pct, max_positions, cash_floor_pct,
    max_drawdown_pct, max_single_stock_pct, max_gross_exposure_pct and
    regime_policy (none/bull_only/bear_reduce_only). Existing holdings'
    market_value must be supplied when an exposure cap is configured; otherwise
    the risk check fails closed rather than guessing prices.
    """
    if halted:
        return {
            "status": "HALTED",
            "approved": [],
            "rejected": [
                {"intent": copy.deepcopy(intent), "reason_code": "HALTED"}
                for intent in intents
            ],
        }
    if not isinstance(policy, dict) or not isinstance(account, dict):
        raise ValueError("risk policy and account must be objects")
    cash = _nonnegative(account["cash"], "account.cash")
    nav = _nonnegative(account["nav"], "account.nav")
    if nav <= 0:
        raise ValueError("account.nav must be positive")
    drawdown = account.get("drawdown", 0) or 0
    if (
        isinstance(drawdown, bool)
        or not isinstance(drawdown, (int, float))
        or not math.isfinite(drawdown)
    ):
        raise ValueError("account.drawdown must be finite")
    position_rows = account.get("positions", {})
    if not isinstance(position_rows, dict):
        raise ValueError("account.positions must be a map")
    held = {}
    for code, row in position_rows.items():
        qty = row.get("quantity") if isinstance(row, dict) else None
        if type(qty) is not int or qty <= 0:
            raise ValueError("account position quantity must be positive integer")
        held[code] = qty

    caps = (
        "max_turnover_pct",
        "cash_floor_pct",
        "max_drawdown_pct",
        "max_single_stock_pct",
        "max_gross_exposure_pct",
    )
    normalized = {}
    for key in caps:
        value = policy.get(key)
        if value is not None:
            value = _nonnegative(value, f"risk.{key}")
            if value > 1:
                raise ValueError(f"risk.{key} must be <= 1")
        normalized[key] = value
    max_positions = policy.get("max_positions")
    if max_positions is not None and (
        type(max_positions) is not int or max_positions <= 0
    ):
        raise ValueError("risk.max_positions must be positive integer")
    regime_policy = policy.get("regime_policy", "none")
    if regime_policy not in ("none", "bull_only", "bear_reduce_only"):
        raise ValueError("unsupported risk.regime_policy")
    market_regime = (market or {}).get("regime")
    if regime_policy != "none" and market_regime not in ("bull", "bear", "sideways"):
        raise ValueError("market.regime required by risk.regime_policy")

    exposure_cap = normalized["max_single_stock_pct"]
    gross_cap = normalized["max_gross_exposure_pct"]
    exposed = {}
    if exposure_cap is not None or gross_cap is not None:
        for code, row in position_rows.items():
            if not isinstance(row, dict) or "market_value" not in row:
                raise ValueError(f"market_value required for held position {code}")
            exposed[code] = _nonnegative(
                row["market_value"], f"positions.{code}.market_value"
            )
    exposure_total = sum(exposed.values())
    approved, rejected = [], []
    sell_reserved: dict[str, int] = {}
    reserved_positions = set(held)
    turnover = 0.0

    # Sellers first, then higher priority; original index breaks ties.
    def rank(entry):
        index, item = entry
        priority = item.get("priority", 0)
        if (
            isinstance(priority, bool)
            or not isinstance(priority, (int, float))
            or not math.isfinite(priority)
        ):
            raise ValueError("intent.priority must be finite")
        return (item.get("side") != "SELL", -priority, index)

    for _, original in sorted(enumerate(intents), key=rank):
        intent = copy.deepcopy(original)
        side = intent.get("side")
        quantity = intent.get("quantity")
        price = intent.get("price")
        code = intent.get("stock_code")
        if (
            not isinstance(intent.get("intent_id"), str)
            or not intent["intent_id"]
            or side not in ("BUY", "SELL")
            or type(quantity) is not int
            or quantity <= 0
            or not isinstance(code, str)
            or not code
            or isinstance(price, bool)
            or not isinstance(price, (int, float))
            or not math.isfinite(price)
            or price <= 0
        ):
            raise ValueError("malformed order intent")
        notional = quantity * float(price)
        reason = None
        if side == "SELL":
            if held.get(code, 0) - sell_reserved.get(code, 0) < quantity:
                reason = "INSUFFICIENT_HOLDINGS"
        elif (
            normalized["max_drawdown_pct"] is not None
            and drawdown <= -normalized["max_drawdown_pct"]
        ):
            reason = "DRAWDOWN_BREAKER"
        elif regime_policy == "bull_only" and market_regime != "bull":
            reason = "REGIME_BLOCKED"
        elif regime_policy == "bear_reduce_only" and market_regime == "bear":
            reason = "REGIME_BLOCKED"
        elif (
            max_positions is not None
            and code not in reserved_positions
            and len(reserved_positions) >= max_positions
        ):
            reason = "MAX_POSITIONS"
        elif notional > cash - (normalized["cash_floor_pct"] or 0) * nav + 1e-8:
            reason = "CASH_FLOOR"
        elif (
            exposure_cap is not None
            and (exposed.get(code, 0.0) + notional) > exposure_cap * nav + 1e-8
        ):
            reason = "CONCENTRATION"
        elif (
            gross_cap is not None and exposure_total + notional > gross_cap * nav + 1e-8
        ):
            reason = "GROSS_EXPOSURE"
        if (
            reason is None
            and normalized["max_turnover_pct"] is not None
            and turnover + notional > normalized["max_turnover_pct"] * nav + 1e-8
        ):
            reason = "TURNOVER_BUDGET"

        if reason is not None:
            rejected.append({"intent": intent, "reason_code": reason})
            continue
        approved.append(intent)
        turnover += notional
        if side == "SELL":
            # Approved sells reserve quantity only: no assumed fill proceeds.
            sell_reserved[code] = sell_reserved.get(code, 0) + quantity
        else:
            cash -= notional
            reserved_positions.add(code)
            exposed[code] = exposed.get(code, 0.0) + notional
            exposure_total += notional
    return {"status": "OK", "approved": approved, "rejected": rejected}
