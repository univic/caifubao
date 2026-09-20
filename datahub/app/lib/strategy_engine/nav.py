# -*- coding: utf-8 -*-
"""Paper NAV simulation with realistic T+1 cost semantics.

Pure, dependency-injected: callers feed prices as
{stock_code: {date: quote}} where quote exposes open / close / trade_status
(1 = tradable, 0 = suspended) and, optionally, limit_up / limit_down /
previous_close. On each rebalance date the engine executes sells first then buys
at that date's open (the T+1 executable date must be supplied by the runner),
with commission/minimum/slippage, the two-way transfer fee, sell stamp duty, and
board-aware minimum order sizes; a session that is limit-up (BUY) or limit-down
(SELL) is refused and reported in ``blocked``; suspended names roll forward
(sell kept, buy skipped, valuation held at the last observed close, never a
forced mark). NAV is marked to close of each schedule date. Turnover per cycle =
(buy + sell notional) / pre-cycle opening NAV (valid opens, otherwise the last
known close). Baseline = same-date equal-weight return of the tradable
universe (subset supplied by the caller).
"""

from __future__ import annotations

import math

from app.lib.strategy_engine.config import PAPER_EXECUTION
from app.lib.strategy_engine.tradability import tradeable_open


class QuoteView:
    """Minimal read interface the NAV engine expects from a quote."""

    def __init__(
        self,
        open_price=None,
        close_price=None,
        trade_status=1,
        *,
        limit_up=None,
        limit_down=None,
        previous_close=None,
        stock_code="",
        is_st=False,
    ):
        self.open = open_price
        self.close = close_price
        self.trade_status = trade_status  # 1 = tradable, 0 = suspended
        # Unknown (None) is fail-closed for the side the flag would block; a
        # derived verdict is used when the caller passes previous_close.
        self.limit_up = limit_up
        self.limit_down = limit_down
        self.previous_close = previous_close
        self.stock_code = stock_code
        self.is_st = is_st


#: STAR-market (科创板) board rule: a 200-share minimum, then 1-share
#: increments. Every other A-share board trades in 100-share lots.
STAR_MINIMUM = 200
DEFAULT_MINIMUM = 100


def board_lot_rule(
    stock_code: str, default_lot: int = DEFAULT_MINIMUM
) -> tuple[int, int]:
    """(minimum quantity, increment) for a BUY in one A-share code.

    STAR/科创板 (``sh688``/``sh689``, bare ``688``/``689``) requires at least
    200 shares and then accepts any 1-share increment; ChiNext/创业板 and the
    main board trade in ``default_lot`` (normally 100) share lots. SELL sizing
    is deliberately exempt (see ``simulate_paper_nav``) so an odd-lot position
    can always be closed out.
    """
    code = (stock_code or "").strip().lower()
    if code.startswith(("sh688", "sh689", "688", "689")):
        return STAR_MINIMUM, 1
    lot = int(default_lot or DEFAULT_MINIMUM)
    return lot, lot


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
    # A-share transfer fee (过户费) is charged on BOTH sides.
    transfer_fee = value * float(cfg.get("transfer_fee_rate", 0.0))
    return {
        "value": value,
        "commission": commission,
        "stamp_duty": stamp_duty,
        "transfer_fee": transfer_fee,
    }


def _buy_spend(exec_price: float, quantity: int, cfg: dict) -> float:
    """Total cash cost of a BUY of ``quantity`` shares: value at the
    slippage-adjusted exec price plus commission (min-commission aware) and the
    two-way transfer fee's buy leg."""
    cost = _order_cost(exec_price, quantity, "BUY", cfg)
    return cost["value"] + cost["commission"] + cost["transfer_fee"]


def _fit_buy_quantity(
    budget: float,
    exec_price: float,
    minimum: int,
    increment: int,
    cfg: dict,
) -> int:
    """Largest board-legal BUY quantity whose total spend fits ``budget``.

    Quantity is either 0 (nothing fits; caller skips the order) or
    ``minimum + k * increment`` for the largest feasible ``k >= 0`` (main board /
    ChiNext: k lots of 100; STAR: the 200-share minimum plus 1-share steps).
    Fees (slippage inside exec_price, commission, transfer fee) are reserved in
    sizing so an order is skipped only when even the minimum cannot fit — never
    because the fee-unadjusted quantity overshoots.

    Binary search is valid because spend grows monotonically in k (commission
    may floor at the minimum, but never decreases).
    """
    if budget <= 0 or minimum <= 0 or increment <= 0 or not _valid_price(exec_price):
        return 0
    if _buy_spend(exec_price, minimum, cfg) > budget:
        return 0
    # qty(k) = minimum + k*increment, so hi is a safe upper bound on k.
    hi = int(budget // (exec_price * increment)) + 1
    lo = 0
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if _buy_spend(exec_price, minimum + mid * increment, cfg) <= budget:
            lo = mid
        else:
            hi = mid - 1
    return minimum + lo * increment


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
    blocked = []
    peak = start_nav

    def _mark(date) -> float:
        total = cash
        for code, pos in positions.items():
            quote = (prices.get(code) or {}).get(date)
            if _tradable(quote) and _valid_price(quote.close):
                pos["last_price"] = float(quote.close)
            total += pos["qty"] * pos["last_price"]
        return total

    def _open_price(code, date, side):
        """(price, reason): the open this side may fill at, or the refusal.

        Limit-up/limit-down gating and the fail-closed rule for unknown limit
        flags live in ``tradability.tradeable_open`` (shared with the research
        rule). A refusal is reported, never silently dropped.
        """
        quote = (prices.get(code) or {}).get(date)
        return tradeable_open(quote, side, stock_code=code)

    def _record_block(date, code, side, reason):
        blocked.append(
            {
                "date": date,
                "stock_code": code,
                "side": side,
                "reason": reason,
            }
        )

    for decision in schedule:
        date = decision["date"]
        targets = decision.get("holdings") or {}
        target_codes = set(targets)
        # Only information observable at execution is available for sizing.
        # Do not mutate the prior close carried for missing/suspended quotes.
        total_before = cash
        for code, pos in positions.items():
            quote = (prices.get(code) or {}).get(date)
            # A mark is not a fill: a limit-locked session still marks to the
            # last observable price (the open when the session is live).
            if _tradable(quote) and _valid_price(getattr(quote, "open", None)):
                mark = float(quote.open)
            else:
                mark = pos["last_price"]
            total_before += pos["qty"] * mark
        sell_notional = 0.0
        buy_notional = 0.0

        # 1) Sell names that dropped out of the target (executable at open).
        for code in sorted(positions):
            if code in target_codes:
                continue
            price, reason = _open_price(code, date, "SELL")
            if price is None:
                # Blocked exit (suspended / limit-down / unknown flag): the
                # position rolls forward and the refusal is reported.
                _record_block(date, code, "SELL", reason)
                continue
            pos = positions.pop(code)
            exec_price = _exec_price(price, "SELL", cfg)
            cost = _order_cost(exec_price, pos["qty"], "SELL", cfg)
            cash += (
                cost["value"]
                - cost["commission"]
                - cost["stamp_duty"]
                - cost["transfer_fee"]
            )
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
                        "transfer_fee": cost["transfer_fee"],
                    },
                }
            )

        # 2) Buy target names not yet held (each target's weight is its
        # notional share of the portfolio, as produced by selection).
        for code in sorted(target_codes):
            if code in positions:
                continue
            price, reason = _open_price(code, date, "BUY")
            if price is None:
                # Blocked entry (suspended / limit-up / unknown flag): skip this
                # cycle, report the refusal.
                _record_block(date, code, "BUY", reason)
                continue
            weight = targets[code]
            if not isinstance(weight, (int, float)) or weight <= 0:
                continue
            budget = min(cash, total_before * weight)
            raw_open = float(price)
            exec_price = _exec_price(raw_open, "BUY", cfg)
            minimum, increment = board_lot_rule(code, lot)
            # Fee-aware board-lot sizing: pick the largest quantity whose
            # total spend (value at the slippage-adjusted exec price +
            # commission + transfer fee) fits min(cash, target budget). Skip
            # only when even the board minimum cannot fit — never skip an order
            # just because the fee-unadjusted lot count overshoots the budget.
            qty = _fit_buy_quantity(budget, exec_price, minimum, increment, cfg)
            if qty <= 0:
                continue
            cost = _order_cost(exec_price, qty, "BUY", cfg)
            spend = cost["value"] + cost["commission"] + cost["transfer_fee"]
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
                        "transfer_fee": cost["transfer_fee"],
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
        # Orders refused at the session open (limit-up BUY / limit-down SELL /
        # unknown limit verdict / suspension). Fail-loud reporting: the caller
        # persists these next to the plan instead of seeing a silent skip.
        "blocked": blocked,
        "blocked_count": len(blocked),
    }
