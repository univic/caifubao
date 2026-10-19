# -*- coding: utf-8 -*-
"""Core-satellite research accounting.

Two pure pieces live here:

``simulate_core``
    A static ETF core sleeve (default 40/30/30 for the 50k balanced account)
    with board-lot sizing, the paper fee model and a quarterly rebalance. The
    sleeve is an accounting simulation, not an order router.

``combine_sleeves``
    A daily accounting combination of a core curve, a satellite curve and a
    zero-yield cash sleeve. Cross-sleeve moves happen only outside a ±5pp drift
    band and are charged once on the moved notional.

Both functions return JSON-ready dicts. No Mongo, no broker, no scheduling.
"""

from __future__ import annotations

import math
from typing import Iterable

import pandas as pd

#: Default balanced 50k core weights (three-ETF, lot-feasible variant).
DEFAULT_CORE_WEIGHTS = {
    "510300.SH": 0.40,
    "518880.SH": 0.30,
    "513100.SH": 0.30,
}

#: The documented four-leg variant is only buildable at AUM >= 100k because one
#: treasury lot exceeds a 6,250 leg at 50k.
FOUR_LEG_WEIGHTS = {
    "510300.SH": 0.25,
    "518880.SH": 0.25,
    "513100.SH": 0.25,
    "511010.SH": 0.25,
}
FOUR_LEG_MIN_AUM = 100_000.0

DEFAULT_CORE_WEIGHT = 0.50
DEFAULT_SATELLITE_WEIGHT = 0.40
DEFAULT_CASH_WEIGHT = 0.10
DEFAULT_DRIFT_BAND = 0.05
DEFAULT_TRANSFER_COST_RATE = 0.0005
DEFAULT_MIN_TRANSFER_COST = 5.0

#: The paper execution model, imported from its single source of truth. The
#: research module has no Mongo/job dependency; this is the same cost contract
#: the strategy engine and factor lab use.
from app.lib.strategy_engine.config import PAPER_EXECUTION  # noqa: E402

COMMISSION_RATE = PAPER_EXECUTION["commission_rate"]
MIN_COMMISSION = PAPER_EXECUTION["minimum_commission_cny"]
TRANSFER_FEE_RATE = PAPER_EXECUTION["transfer_fee_rate"]
SLIPPAGE_PER_SIDE = PAPER_EXECUTION["slippage_per_side"]
SELL_STAMP_DUTY_RATE = PAPER_EXECUTION["sell_stamp_duty_rate"]
ETF_SELL_STAMP_DUTY_RATE = 0.0


def _validate_weights(weights: dict[str, float]) -> dict[str, float]:
    cleaned = {str(code): float(weight) for code, weight in weights.items()}
    if not cleaned:
        raise ValueError("weights must not be empty")
    if any(weight <= 0 or not math.isfinite(weight) for weight in cleaned.values()):
        raise ValueError("every weight must be finite and positive")
    total = sum(cleaned.values())
    if not math.isclose(total, 1.0, rel_tol=1e-9, abs_tol=1e-9):
        raise ValueError(f"weights must sum to 1, got {total}")
    return cleaned


def core_weights_for_aum(aum: float, *, four_leg: bool = False) -> dict[str, float]:
    """Return the lot-feasible core weights for an AUM.

    At 50k the four-leg 25/25/25/25 variant cannot buy one treasury lot
    (``511010.SH`` is near par, so one 100-share lot is ~10k > 6,250), so it is
    refused rather than silently approximated.
    """
    if four_leg:
        if aum < FOUR_LEG_MIN_AUM:
            raise ValueError(
                "the four-leg 25/25/25/25 core needs an AUM of at least "
                f"{FOUR_LEG_MIN_AUM:.0f} because one treasury lot exceeds the "
                "leg; use the three-ETF 40/30/30 core at 50k"
            )
        return dict(FOUR_LEG_WEIGHTS)
    return dict(DEFAULT_CORE_WEIGHTS)


def _valid_price(value) -> bool:
    try:
        return value is not None and math.isfinite(float(value)) and float(value) > 0
    except (TypeError, ValueError):
        return False


def _commission(notional: float, rate: float, minimum: float) -> float:
    return max(notional * rate, minimum)


def _fit_lot(notional: float, price: float, lot: int) -> int:
    """Largest whole-lot quantity whose notional value fits ``notional``."""
    if notional <= 0 or price <= 0 or lot <= 0:
        return 0
    return int(notional // (price * lot)) * lot


def simulate_core(
    prices: pd.DataFrame,
    *,
    aum: float,
    weights: dict[str, float] | None = None,
    cadence: int = 60,
    lot: int = 100,
    commission_rate: float = COMMISSION_RATE,
    minimum_commission: float = MIN_COMMISSION,
    transfer_fee_rate: float = TRANSFER_FEE_RATE,
    slippage_per_side: float = SLIPPAGE_PER_SIDE,
    start=None,
    end=None,
) -> dict:
    """Simulate the static core sleeve with lot sizing and fees.

    ``prices`` is a wide frame (index = sessions, columns = ETF codes) of
    adjusted closes. Rebalances execute at that session's close; the caller must
    disclose this close-execution convention. A leg whose one lot does not fit
    is skipped and reported. Cash never goes negative.
    """
    if not isinstance(prices.index, pd.DatetimeIndex):
        raise TypeError("prices must be indexed by session timestamps")
    weights = _validate_weights(weights or DEFAULT_CORE_WEIGHTS)
    if set(weights) == set(FOUR_LEG_WEIGHTS) and all(
        math.isclose(weights[code], FOUR_LEG_WEIGHTS[code], abs_tol=1e-9)
        for code in FOUR_LEG_WEIGHTS
    ):
        core_weights_for_aum(aum, four_leg=True)
    missing = [code for code in weights if code not in prices.columns]
    if missing:
        raise ValueError(f"core price frame is missing columns: {missing}")
    frame = prices.sort_index()
    if start is not None:
        frame = frame[frame.index >= pd.Timestamp(start)]
    if end is not None:
        frame = frame[frame.index <= pd.Timestamp(end)]
    if len(frame) < 2:
        raise ValueError("core price frame needs at least two sessions")
    if aum <= 0:
        raise ValueError("aum must be positive")
    if cadence < 1:
        raise ValueError("cadence must be >= 1")

    codes = list(weights)
    cash = float(aum)
    shares = {code: 0 for code in codes}
    curve: list[dict] = []
    decisions: list[dict] = []
    fills: list[dict] = []
    skipped: list[dict] = []
    rebalance_positions = set(range(0, len(frame), cadence))

    for position, (day, row) in enumerate(frame.iterrows()):
        day_str = pd.Timestamp(day).date().isoformat()
        prices_today = {}
        for code in codes:
            value = row.get(code)
            if _valid_price(value):
                prices_today[code] = float(value)
            else:
                raise ValueError(f"core leg {code} has no usable price on {day_str}")
        nav_before = cash + sum(shares[code] * prices_today[code] for code in codes)
        if position in rebalance_positions:
            cash_pre = cash
            targets = {
                code: nav_before * weights[code] / prices_today[code] for code in codes
            }
            # Sells first: a target below the current holding frees cash.
            for code in codes:
                target_qty = _fit_lot(
                    targets[code] * prices_today[code], prices_today[code], lot
                )
                delta = target_qty - shares[code]
                if delta >= 0:
                    continue
                quantity = -delta
                price = prices_today[code] * (1 - slippage_per_side)
                notional = quantity * price
                fee = _commission(notional, commission_rate, minimum_commission)
                transfer = notional * transfer_fee_rate
                cash += notional - fee - transfer
                shares[code] -= quantity
                fills.append(
                    {
                        "date": day_str,
                        "code": code,
                        "side": "SELL",
                        "quantity": quantity,
                        "price": round(price, 6),
                        "commission": round(fee, 2),
                        "stamp_duty": 0.0,
                        "transfer_fee": round(transfer, 2),
                    }
                )
            # Buys second, reducing by whole lots until the cash fits.
            for code in codes:
                target_notional = targets[code] * prices_today[code]
                target_qty = _fit_lot(target_notional, prices_today[code], lot)
                if target_qty == 0 and target_notional > 0 and shares[code] == 0:
                    skipped.append(
                        {
                            "date": day_str,
                            "code": code,
                            "reason": "board minimum does not fit the target leg",
                        }
                    )
                    continue
                delta = target_qty - shares[code]
                if delta <= 0:
                    continue
                quantity = delta
                price = prices_today[code] * (1 + slippage_per_side)
                while quantity >= lot:
                    notional = quantity * price
                    fee = _commission(notional, commission_rate, minimum_commission)
                    transfer = notional * transfer_fee_rate
                    if notional + fee + transfer <= cash + 1e-9:
                        break
                    quantity -= lot
                if quantity < lot:
                    skipped.append(
                        {
                            "date": day_str,
                            "code": code,
                            "reason": "board minimum does not fit available sleeve cash",
                        }
                    )
                    continue
                notional = quantity * price
                fee = _commission(notional, commission_rate, minimum_commission)
                transfer = notional * transfer_fee_rate
                cash -= notional + fee + transfer
                shares[code] += quantity
                fills.append(
                    {
                        "date": day_str,
                        "code": code,
                        "side": "BUY",
                        "quantity": quantity,
                        "price": round(price, 6),
                        "commission": round(fee, 2),
                        "stamp_duty": 0.0,
                        "transfer_fee": round(transfer, 2),
                    }
                )
            decisions.append(
                {
                    "date": day_str,
                    "nav_before": round(nav_before, 2),
                    "cash_before": round(cash_pre, 2),
                    "cash_after": round(cash, 2),
                    "targets": {code: round(weights[code], 6) for code in codes},
                    "positions": {code: shares[code] for code in codes},
                    "fills": len(fills),
                }
            )
        if cash < -1e-6:
            raise ValueError(f"negative core cash on {day_str}: {cash}")
        nav = cash + sum(shares[code] * prices_today[code] for code in codes)
        curve.append(
            {
                "date": day_str,
                "nav": round(nav, 2),
                "cash": round(cash, 2),
                "positions_count": sum(1 for qty in shares.values() if qty > 0),
            }
        )

    return {
        "curve": curve,
        "decisions": decisions,
        "fills": fills,
        "skipped": skipped,
        "initial_nav": round(float(aum), 2),
        "terminal_nav": curve[-1]["nav"],
        "weights": {code: round(weight, 6) for code, weight in weights.items()},
    }


def combine_sleeves(
    core_curve: Iterable[dict],
    satellite_curve: Iterable[dict],
    *,
    cash: float,
    core_weight: float = DEFAULT_CORE_WEIGHT,
    satellite_weight: float = DEFAULT_SATELLITE_WEIGHT,
    cash_weight: float = DEFAULT_CASH_WEIGHT,
    cadence: int = 60,
    drift_band: float = DEFAULT_DRIFT_BAND,
    transfer_cost_rate: float = DEFAULT_TRANSFER_COST_RATE,
    min_transfer_cost: float = DEFAULT_MIN_TRANSFER_COST,
    benchmark_by_date: dict[str, float] | None = None,
) -> dict:
    """Combine daily core/satellite NAV curves into a 50/40/10 account."""
    if not math.isclose(
        core_weight + satellite_weight + cash_weight, 1.0, abs_tol=1e-9
    ):
        raise ValueError("sleeve weights must sum to 1")
    if cash < 0:
        raise ValueError("cash must be non-negative")
    core_rows = {row["date"]: row for row in core_curve}
    sat_rows = {row["date"]: row for row in satellite_curve}
    core_dates = set(core_rows)
    sat_dates = set(sat_rows)
    if core_dates != sat_dates:
        missing = sorted(core_dates ^ sat_dates)[:5]
        raise ValueError(
            "core and satellite curves must cover the same sessions; "
            f"first mismatch(es): {missing}"
        )
    core_by_date = {day: float(row["nav"]) for day, row in core_rows.items()}
    sat_by_date = {day: float(row["nav"]) for day, row in sat_rows.items()}
    common = sorted(core_dates)
    if len(common) < 2:
        raise ValueError("core and satellite curves share fewer than two sessions")
    benchmark = benchmark_by_date or {}
    if cadence < 1:
        raise ValueError("cadence must be >= 1")

    core_nav = core_by_date[common[0]]
    sat_nav = sat_by_date[common[0]]
    cash_nav = float(cash)
    curve: list[dict] = []
    decisions: list[dict] = []
    transfers: list[dict] = []
    peak = core_nav + sat_nav + cash_nav
    rebalance_positions = set(range(0, len(common), cadence))

    for position, day in enumerate(common):
        if position > 0:
            prev_day = common[position - 1]
            core_nav *= core_by_date[day] / core_by_date[prev_day]
            sat_nav *= sat_by_date[day] / sat_by_date[prev_day]
        total = core_nav + sat_nav + cash_nav
        if position in rebalance_positions and position > 0:
            weights_now = {
                "core": core_nav / total,
                "satellite": sat_nav / total,
                "cash": cash_nav / total,
            }
            target = {
                "core": core_weight,
                "satellite": satellite_weight,
                "cash": cash_weight,
            }
            breached = any(
                abs(weights_now[key] - target[key]) > drift_band for key in target
            )
            if breached:
                # Restore in a deterministic order; a move is an accounting
                # transfer between sleeves, not an ETF lot trade.
                deltas = {
                    key: total * target[key] - value
                    for key, value in (
                        ("core", core_nav),
                        ("satellite", sat_nav),
                        ("cash", cash_nav),
                    )
                }
                moved = sum(value for value in deltas.values() if value > 0)
                cost = (
                    max(min_transfer_cost, moved * transfer_cost_rate) if moved else 0.0
                )
                # Deduct the cost from the largest receiving sleeve, then set
                # each sleeve to its target.
                if cost:
                    payer = max(deltas, key=lambda key: deltas[key])
                    deltas[payer] -= cost
                core_nav += deltas["core"]
                sat_nav += deltas["satellite"]
                cash_nav += deltas["cash"]
                transfers.append(
                    {
                        "date": day,
                        "moved_notional": round(moved, 2),
                        "cost": round(cost, 2),
                        "weights_before": {
                            key: round(value, 6) for key, value in weights_now.items()
                        },
                        "weights_after": {key: round(target[key], 6) for key in target},
                    }
                )
            else:
                transfers.append(
                    {
                        "date": day,
                        "moved_notional": 0.0,
                        "cost": 0.0,
                        "weights_before": {
                            key: round(value, 6) for key, value in weights_now.items()
                        },
                        "weights_after": {
                            key: round(value, 6) for key, value in weights_now.items()
                        },
                    }
                )
            if core_nav < -1e-6 or sat_nav < -1e-6 or cash_nav < -1e-6:
                raise ValueError(f"negative sleeve NAV on {day}")
            decisions.append(
                {
                    "date": day,
                    "nav_before": round(total, 2),
                    "weights_before": {
                        key: round(value, 6) for key, value in weights_now.items()
                    },
                    "weights_after": {
                        "core": round(core_nav / (core_nav + sat_nav + cash_nav), 6),
                        "satellite": round(
                            sat_nav / (core_nav + sat_nav + cash_nav), 6
                        ),
                        "cash": round(cash_nav / (core_nav + sat_nav + cash_nav), 6),
                    },
                    "moved_notional": round(transfers[-1]["moved_notional"], 2),
                    "cost": round(transfers[-1]["cost"], 2),
                }
            )
        total = core_nav + sat_nav + cash_nav
        peak = max(peak, total)
        curve.append(
            {
                "date": day,
                "nav": round(total, 2),
                "core_nav": round(core_nav, 2),
                "satellite_nav": round(sat_nav, 2),
                "cash_nav": round(cash_nav, 2),
                "drawdown": round(total / peak - 1.0, 6) if peak else None,
                "cash": round(cash_nav, 2),
                "benchmark_nav": round(float(benchmark[day]), 2)
                if day in benchmark and _valid_price(benchmark.get(day))
                else None,
                "positions_count": int(core_rows[day].get("positions_count", 0) or 0)
                + int(
                    sat_rows[day].get(
                        "positions_count", sat_rows[day].get("positions", 0)
                    )
                    or 0
                ),
                "gross_exposure": round(1.0 - cash_nav / total, 6) if total else None,
            }
        )

    return {
        "curve": curve,
        "decisions": decisions,
        "transfers": transfers,
        "initial_nav": round(curve[0]["nav"], 2),
        "terminal_nav": curve[-1]["nav"],
        "sleeve_weights": {
            "core": core_weight,
            "satellite": satellite_weight,
            "cash": cash_weight,
        },
    }
