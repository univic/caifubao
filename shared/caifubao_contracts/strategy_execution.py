# -*- coding: utf-8 -*-
"""Deterministic, side-effect-free A-share order rules.

Shared between research simulation and future trading adapters. This module
does not read databases, place orders, or own any account/broker state.
"""

from __future__ import annotations

import hashlib
import json
import math

from caifubao_contracts.friction import DEFAULT_COST_PROFILE, CostProfile


def _positive(value) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) > 0
    )


def board_lot_rule(code: str, default_lot: int = 100) -> tuple[int, int]:
    """Minimum BUY quantity and subsequent step (STAR: 200 + 1n)."""
    normalized = (code or "").strip().lower()
    if normalized.startswith(("sh688", "sh689", "688", "689")):
        return 200, 1
    if type(default_lot) is not int or default_lot < 1:
        raise ValueError("default_lot must be positive integer")
    return default_lot, default_lot


def adjusted_price(open_price: float, side: str, slippage: float) -> float:
    if side not in ("BUY", "SELL"):
        raise ValueError("side must be BUY or SELL")
    if (
        not _positive(open_price)
        or not isinstance(slippage, (int, float))
        or not 0 <= slippage < 1
    ):
        raise ValueError("invalid open price or slippage")
    return float(open_price) * (1 + slippage if side == "BUY" else 1 - slippage)


def execution_fees(
    price: float, quantity: int, side: str, profile: CostProfile = DEFAULT_COST_PROFILE
) -> dict:
    """Fees EXCLUDING slippage: the fill price already includes slippage."""
    if side not in ("BUY", "SELL"):
        raise ValueError("side must be BUY or SELL")
    if not _positive(price) or not _positive(quantity):
        raise ValueError("price and quantity must be positive")
    notional = float(price) * quantity
    return {
        "commission": max(
            notional * profile.commission_rate, profile.minimum_commission_cny
        ),
        "transfer_fee": notional * profile.transfer_fee_rate,
        "stamp_duty": notional * profile.sell_stamp_duty_rate
        if side == "SELL"
        else 0.0,
    }


def fit_buy_quantity(
    budget: float,
    price: float,
    minimum: int,
    step: int,
    profile: CostProfile = DEFAULT_COST_PROFILE,
) -> int:
    """Largest legal quantity with fee-inclusive cash spend <= budget."""
    if not _positive(budget) or not _positive(price) or minimum <= 0 or step <= 0:
        return 0

    def spend(quantity):
        fees = execution_fees(price, quantity, "BUY", profile)
        return price * quantity + fees["commission"] + fees["transfer_fee"]

    if spend(minimum) > budget:
        return 0
    lo, hi = 0, int(budget // (price * step)) + 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if spend(minimum + mid * step) <= budget:
            lo = mid
        else:
            hi = mid - 1
    return minimum + lo * step


def tradable_open(
    quote: dict | None,
    side: str,
    *,
    unknown_limit_policy: str = "fail_closed",
) -> tuple[float | None, str]:
    """Resolve a preclassified market snapshot without inventing board rules.

    The market-data adapter supplies explicit flags (or obtains them through
    its authoritative board-limit classifier). Missing limit information
    defaults to refusal. Replay compatibility is explicit and research-only.
    """
    if side not in ("BUY", "SELL"):
        raise ValueError("side must be BUY or SELL")
    if unknown_limit_policy not in ("fail_closed", "replay_compat"):
        raise ValueError("unknown limit policy")
    if quote is None:
        return None, "no_quote"
    if quote.get("trade_status") != 1:
        return None, "suspended"
    price = quote.get("open")
    if not _positive(price):
        return None, "no_open"
    flag = quote.get("limit_up" if side == "BUY" else "limit_down")
    if flag is None and unknown_limit_policy == "fail_closed":
        return None, "limit_unknown"
    if flag:
        return None, "at_limit_up" if side == "BUY" else "at_limit_down"
    return float(price), ""



def session_limit_flags(
    open_price: float | None,
    previous_close: float | None,
    board_limit_pct: float,
    *,
    tolerance_pct: float = 0.1,
) -> tuple[bool | None, bool | None]:
    """Pure board-limit comparison, with explicit point-in-time board policy.

    A domain-local data adapter classifies the board/ST status. This function
    owns only the deterministic price comparison and unknown-value semantics.
    Limits and tolerance are percentage points, not decimal fractions.
    """
    if (
        not _positive(open_price)
        or not _positive(previous_close)
        or not _positive(board_limit_pct)
    ):
        return None, None
    if (
        isinstance(tolerance_pct, bool)
        or not isinstance(tolerance_pct, (int, float))
        or not math.isfinite(tolerance_pct)
        or tolerance_pct < 0
        or tolerance_pct >= board_limit_pct
    ):
        raise ValueError("tolerance_pct must be finite and below board_limit_pct")
    band = (board_limit_pct - tolerance_pct) / 100.0
    return (
        float(open_price) >= float(previous_close) * (1 + band),
        float(open_price) <= float(previous_close) * (1 - band),
    )

def deterministic_fill_id(
    *,
    order_id: str,
    attempt_id: str,
    execution_id: str,
) -> str:
    """Unique event identity: never key fills merely by date/side/symbol."""
    if not all(isinstance(x, str) and x for x in (order_id, attempt_id, execution_id)):
        raise ValueError("stable order, attempt, and execution identities are required")
    data = json.dumps([order_id, attempt_id, execution_id], separators=(",", ":"))
    return hashlib.sha256(data.encode("utf-8")).hexdigest()
