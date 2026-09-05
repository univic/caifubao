# -*- coding: utf-8 -*-
"""Paper NAV simulation with realistic T+1 cost semantics.

Pure, dependency-injected: callers feed prices as {date: {stock_code: quote}}
where quote exposes open/close/trade_status (1 = tradable, 0 = suspended).
On each rebalance date the engine executes sells first then buys at that
date's open (T+1 executable date must be supplied by the runner), with
commission/minimum/slippage, sell stamp duty, and board-lot rounding;
suspended names roll forward (stays in the portfolio, no forced mark). NAV is
marked to close of each schedule date. Baseline = same-date equal-weight
return of the tradable universe (subset supplied by the caller).
"""

from __future__ import annotations

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


def _order_cost(exec_price: float, quantity: float, side: str, cfg: dict) -> dict:
    value = exec_price * quantity
    commission = max(
        value * float(cfg["commission_rate"]),
        float(cfg["minimum_commission_cny"]),
    )
    stamp_duty = value * float(cfg["sell_stamp_duty_rate"]) if side == "SELL" else 0.0
    return {"value": value, "commission": commission, "stamp_duty": stamp_duty}


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

    Returns {"initial_nav", "terminal_nav", "curve"} where each curve point is
    {"date", "nav", "daily_return", "drawdown", "benchmark_return"?,
    "positions_count"}.
    """
    cfg = dict(PAPER_EXECUTION)
    if execution:
        cfg.update(execution)
    lot = int(board_lot or cfg["board_lot"])
    start_nav = float(initial_nav or cfg["initial_nav"])
    benchmark = benchmark_returns or {}

    cash = start_nav
    # code -> {"qty": shares, "weight_target": float}
    positions: dict[str, dict] = {}
    curve = []
    peak = start_nav

    def _mark(date) -> float:
        total = cash
        for code, pos in positions.items():
            quote = (prices.get(code) or {}).get(date)
            if quote is not None and quote.close is not None:
                total += pos["qty"] * float(quote.close)
            else:
                # Suspended / no quote: value at last known cost (roll forward).
                total += pos["qty"] * pos["last_price"]
        return total

    for decision in schedule:
        date = decision["date"]
        targets = decision.get("holdings") or {}
        target_codes = set(targets)
        total_before = _mark(date)

        # 1) Sell names that dropped out of the target (executable at open).
        for code in list(positions):
            if code in target_codes:
                continue
            quote = (prices.get(code) or {}).get(date)
            if quote is None or int(quote.trade_status) != 1 or quote.open is None:
                continue  # suspended / no quote: roll forward
            pos = positions.pop(code)
            exec_price = _exec_price(float(quote.open), "SELL", cfg)
            cost = _order_cost(exec_price, pos["qty"], "SELL", cfg)
            cash += cost["value"] - cost["commission"] - cost["stamp_duty"]

        # 2) Buy target names not yet held (equal-weight notional per name).
        target_value = total_before * (1.0 / len(target_codes)) if target_codes else 0.0
        for code in target_codes:
            if code in positions:
                continue
            quote = (prices.get(code) or {}).get(date)
            if quote is None or int(quote.trade_status) != 1 or quote.open is None:
                continue  # suspended / no quote: skip this cycle (roll forward)
            budget = min(cash, target_value)
            exec_price = _exec_price(float(quote.open), "BUY", cfg)
            max_qty = int(budget / (exec_price * (1 + cfg["slippage_per_side"])))
            qty = (max_qty // lot) * lot
            if qty <= 0:
                continue
            cost = _order_cost(exec_price, qty, "BUY", cfg)
            spend = cost["value"] + cost["commission"]
            if spend > cash:
                continue
            cash -= spend
            positions[code] = {
                "qty": qty,
                "last_price": float(quote.open),
                "entry_open": float(quote.open),
            }

        nav = _mark(date)
        daily_return = None
        if curve:
            prev = curve[-1]["nav"]
            daily_return = (nav / prev - 1.0) if prev else None
        peak = max(peak, nav)
        drawdown = (nav / peak - 1.0) if peak else None
        point = {
            "date": date,
            "nav": round(nav, 2),
            "daily_return": round(daily_return, 6)
            if daily_return is not None
            else None,
            "drawdown": round(drawdown, 6) if drawdown is not None else None,
            "positions_count": len(positions),
        }
        if date in benchmark:
            point["benchmark_return"] = round(benchmark[date], 6)
        curve.append(point)

    return {
        "initial_nav": round(start_nav, 2),
        "terminal_nav": round(_mark(schedule[-1]["date"]), 2),
        "curve": curve,
    }
