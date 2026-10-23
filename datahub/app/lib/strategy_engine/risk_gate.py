# -*- coding: utf-8 -*-
"""Deterministic research risk-gate evaluator over explicit account snapshots.

Operational trading halt, order persistence and brokerage remain trading-owned.
"""

from __future__ import annotations

import copy
import math


def evaluate_risk(
    *,
    intents: list[dict],
    account: dict,
    policy: dict,
    halted: bool = False,
) -> dict:
    """Return approved and rejected intents, without mutating input or state.

    Each intent contains intent_id, side, stock_code, quantity and price.
    Sells are considered first; blocked exits remain in the account snapshot.
    """
    if halted:
        return {
            "status": "HALTED",
            "approved": [],
            "rejected": [{"intent": copy.deepcopy(x), "reason_code": "HALTED"} for x in intents],
        }
    cash = float(account["cash"])
    nav = float(account["nav"])
    if not math.isfinite(cash) or not math.isfinite(nav) or nav <= 0 or cash < 0:
        raise ValueError("account cash/nav must be finite and non-negative")
    positions = {
        k: int(v["quantity"]) for k, v in account.get("positions", {}).items()
    }
    max_positions = policy.get("max_positions")
    max_turnover = policy.get("max_turnover_pct")
    cash_floor = float(policy.get("cash_floor_pct") or 0)
    max_drawdown = policy.get("max_drawdown_pct")
    drawdown = float(account.get("drawdown") or 0)
    approved, rejected = [], []
    turnover = 0.0
    ordered = sorted(enumerate(intents), key=lambda pair: (pair[1].get("side") != "SELL", pair[0]))
    for _, raw in ordered:
        intent = copy.deepcopy(raw)
        side = intent.get("side")
        quantity = intent.get("quantity")
        price = intent.get("price")
        code = intent.get("stock_code")
        if not intent.get("intent_id") or side not in ("BUY", "SELL") or type(quantity) is not int or quantity <= 0 or not isinstance(code, str) or not code or isinstance(price, bool) or not isinstance(price, (int, float)) or not math.isfinite(price) or price <= 0:
            raise ValueError("malformed order intent")
        notional = quantity * float(price)
        reason = None
        if side == "SELL":
            if positions.get(code, 0) < quantity:
                reason = "INSUFFICIENT_HOLDINGS"
            else:
                positions[code] -= quantity
                if not positions[code]:
                    del positions[code]
                cash += notional
        elif max_drawdown is not None and drawdown <= -float(max_drawdown):
            reason = "DRAWDOWN_BREAKER"
        elif max_positions is not None and code not in positions and len(positions) >= int(max_positions):
            reason = "MAX_POSITIONS"
        elif notional > cash - cash_floor * nav + 1e-8:
            reason = "CASH_FLOOR"
        elif max_turnover is not None and turnover + notional > float(max_turnover) * nav + 1e-8:
            reason = "TURNOVER_BUDGET"
        else:
            positions[code] = positions.get(code, 0) + quantity
            cash -= notional

        if reason:
            rejected.append({"intent": intent, "reason_code": reason})
        else:
            approved.append(intent)
            turnover += notional
    return {"status": "OK", "approved": approved, "rejected": rejected}
