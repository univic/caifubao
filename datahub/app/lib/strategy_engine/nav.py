# -*- coding: utf-8 -*-
"""Paper NAV simulation with realistic T+1 cost semantics.

Pure, dependency-injected: callers feed prices as
{stock_code: {date: quote}} where quote exposes open / close / trade_status
(1 = tradable, 0 = suspended). On each rebalance date the engine executes
sells first then buys at that date's open (the T+1 executable date must be
supplied by the runner), with commission/minimum/slippage, sell stamp duty,
and board-lot rounding; suspended names roll forward (sell kept, buy skipped,
valuation held at the last observed close, never a forced mark). NAV is marked
to close of each schedule date. Turnover per cycle = (buy + sell notional) /
pre-cycle opening NAV (valid opens, otherwise the last known close).
Baseline = same-date equal-weight return of the tradable
universe (subset supplied by the caller).
"""

from __future__ import annotations

import math

from app.lib.strategy_engine.config import PAPER_EXECUTION


class QuoteView:
    """Minimal read interface the NAV engine expects from a quote."""

    def __init__(self, open_price=None, close_price=None, trade_status=1):
        self.open = open_price
        self.close = close_price
        self.trade_status = trade_status  # 1 = tradable, 0 = suspended


def _exec_price(price: float, side: str, cfg: dict) -> float:
    slippage = float(cfg["slippage_per_side"])
    return price * (1 + slippage if side == "BUY" else 1 - slippage)


def _valid_price(price) -> bool:
    try:
        return (
            not isinstance(price, bool)
            and math.isfinite(float(price))
            and float(price) > 0
        )
    except (TypeError, ValueError, OverflowError):
        return False


def _tradable(quote) -> bool:
    try:
        return quote is not None and int(quote.trade_status) == 1
    except (TypeError, ValueError, OverflowError):
        return False


def _order_cost(exec_price: float, quantity: float, side: str, cfg: dict) -> dict:
    value = exec_price * quantity
    commission = max(
        value * float(cfg["commission_rate"]),
        float(cfg["minimum_commission_cny"]),
    )
    stamp_duty = value * float(cfg["sell_stamp_duty_rate"]) if side == "SELL" else 0.0
    return {"value": value, "commission": commission, "stamp_duty": stamp_duty}


def _buy_spend(exec_price: float, quantity: int, cfg: dict) -> float:
    """Total cash cost of a BUY of ``quantity`` shares: value at the
    slippage-adjusted exec price plus commission (min-commission aware)."""
    cost = _order_cost(exec_price, quantity, "BUY", cfg)
    return cost["value"] + cost["commission"]


def _fit_buy_quantity(budget: float, exec_price: float, lot: int, cfg: dict) -> int:
    """Largest board-lot quantity (``k * lot``, k >= 1) whose total spend
    (value at exec price + commission) fits ``budget``.

    Roadmap 1.3a: fees (slippage inside exec_price, commission) are reserved
    in sizing so an order is skipped only when even one lot cannot fit —
    never because the fee-unadjusted lot count overshoots. Returns 0 when
    nothing fits (caller skips the order).
    """
    if budget <= 0 or lot <= 0 or not _valid_price(exec_price):
        return 0
    # Binary search over the lot count k in [0, hi]. k = 0 is the sentinel for
    # "nothing fits" (returned as no order); spend(k) grows monotonically in k
    # (commission may floor at the minimum, but never decreases), so the
    # largest feasible k is found by bisection. spend(0) is not evaluated
    # (it would be the minimum commission, not 0, under the default config).
    hi = int(budget // (exec_price * lot)) + 1
    lo = 0
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if _buy_spend(exec_price, mid * lot, cfg) <= budget:
            lo = mid
        else:
            hi = mid - 1
    return lo * lot


def simulate_paper_nav(
    *,
    prices: dict[str, dict[str, QuoteView]],  # stock_code -> {date: QuoteView}
    schedule: list[dict],  # [{date, holdings: {code: weight}}]
    benchmark_returns: dict[str, float] | None = None,  # date -> equal-weight ret
    execution: dict | None = None,
    initial_nav: float | None = None,
    board_lot: int | None = None,
) -> dict:
    """Simulate a paper portfolio over a rebalance schedule.

    Schedule dates are execution sessions supplied by the caller. Opening
    sizing never reads the session's close; close marks follow all fills.
    Returns {"initial_nav", "terminal_nav", "curve", "trades"} where each curve point is
    {"date", "nav", "daily_return", "turnover", "drawdown",
    "benchmark_return"?, "positions_count"}.
    """
    if not schedule:
        raise ValueError("schedule must contain at least one rebalance decision")

    cfg = dict(PAPER_EXECUTION)
    if execution:
        cfg.update(execution)
    lot = int(board_lot or cfg["board_lot"])
    start_nav = float(initial_nav or cfg["initial_nav"])
    benchmark = benchmark_returns or {}

    cash = start_nav
    # code -> {"qty": shares, "last_price": last observed mark}
    positions: dict[str, dict] = {}
    curve = []
    trades = []
    peak = start_nav

    def _mark(date) -> float:
        total = cash
        for code, pos in positions.items():
            quote = (prices.get(code) or {}).get(date)
            if _tradable(quote) and _valid_price(quote.close):
                pos["last_price"] = float(quote.close)
            total += pos["qty"] * pos["last_price"]
        return total

    def _executable(code, date):
        quote = (prices.get(code) or {}).get(date)
        if not _tradable(quote) or not _valid_price(quote.open):
            return None
        return quote

    for decision in schedule:
        date = decision["date"]
        targets = decision.get("holdings") or {}
        target_codes = set(targets)
        # Only information observable at execution is available for sizing.
        # Do not mutate the prior close carried for missing/suspended quotes.
        total_before = cash
        for code, pos in positions.items():
            quote = _executable(code, date)
            mark = float(quote.open) if quote is not None else pos["last_price"]
            total_before += pos["qty"] * mark
        sell_notional = 0.0
        buy_notional = 0.0

        # 1) Sell names that dropped out of the target (executable at open).
        for code in sorted(positions):
            if code in target_codes:
                continue
            quote = _executable(code, date)
            if quote is None:
                continue  # suspended / no quote: roll forward
            pos = positions.pop(code)
            exec_price = _exec_price(float(quote.open), "SELL", cfg)
            cost = _order_cost(exec_price, pos["qty"], "SELL", cfg)
            cash += cost["value"] - cost["commission"] - cost["stamp_duty"]
            sell_notional += cost["value"]
            trades.append(
                {
                    "date": date,
                    "stock_code": code,
                    "side": "SELL",
                    "quantity": pos["qty"],
                    "price": exec_price,
                    "costs": {
                        "commission": cost["commission"],
                        "stamp_duty": cost["stamp_duty"],
                    },
                }
            )

        # 2) Buy target names not yet held (each target's weight is its
        # notional share of the portfolio, as produced by selection).
        for code in sorted(target_codes):
            if code in positions:
                continue
            quote = _executable(code, date)
            if quote is None:
                continue  # suspended / no quote: skip this cycle (roll forward)
            weight = targets[code]
            if not isinstance(weight, (int, float)) or weight <= 0:
                continue
            budget = min(cash, total_before * weight)
            raw_open = float(quote.open)
            exec_price = _exec_price(raw_open, "BUY", cfg)
            # Fee-aware board-lot sizing: pick the largest quantity whose
            # total spend (value at the slippage-adjusted exec price +
            # commission) fits min(cash, target budget). Skip only when even
            # one lot cannot fit — never skip an order just because the
            # fee-unadjusted lot count overshoots the budget.
            qty = _fit_buy_quantity(budget, exec_price, lot, cfg)
            if qty <= 0:
                continue
            cost = _order_cost(exec_price, qty, "BUY", cfg)
            spend = cost["value"] + cost["commission"]
            cash -= spend
            positions[code] = {"qty": qty, "last_price": raw_open}
            buy_notional += cost["value"]
            trades.append(
                {
                    "date": date,
                    "stock_code": code,
                    "side": "BUY",
                    "quantity": qty,
                    "price": exec_price,
                    "costs": {
                        "commission": cost["commission"],
                        "stamp_duty": cost["stamp_duty"],
                    },
                }
            )

        nav = _mark(date)
        daily_return = None
        if curve:
            prev = curve[-1]["nav"]
            daily_return = (nav / prev - 1.0) if prev else None
        turnover = (
            (sell_notional + buy_notional) / total_before if total_before else None
        )
        peak = max(peak, nav)
        drawdown = (nav / peak - 1.0) if peak else None
        point = {
            "date": date,
            "nav": round(nav, 2),
            "daily_return": round(daily_return, 6)
            if daily_return is not None
            else None,
            "turnover": round(turnover, 6) if turnover is not None else None,
            "drawdown": round(drawdown, 6) if drawdown is not None else None,
            "positions_count": len(positions),
        }
        if date in benchmark:
            point["benchmark_return"] = round(benchmark[date], 6)
        curve.append(point)

    return {
        "initial_nav": round(start_nav, 2),
        "terminal_nav": curve[-1]["nav"],
        "curve": curve,
        "trades": trades,
    }
