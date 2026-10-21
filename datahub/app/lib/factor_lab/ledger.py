# -*- coding: utf-8 -*-
"""Causal T+1 research ledger with strict position caps.

This module is deliberately separate from the legacy ``factor_lab_account_replay``
script: the audit found that the old loop filled at D+1 open but marked the same
book at D close, and that ``names=10`` capped the target rather than actual
holdings. The ledger here is the corrected accounting primitive. It is
research-only: no Mongo, broker, scheduler or order router.

Contract:

* a target decided at the close of D is filled at the open of D+1;
* the resulting book is marked at the close of D+1;
* pending sells are retried, and no new name is opened while the account is at
  the configured actual-position cap;
* share multipliers and cash-per-share distributions are explicit inputs;
* a delisting exit uses an explicit price or is reported as a write-off with
  ``lots × board_lot × price``; missing quotes alone never zero a position.

The caller must supply a panel whose ``open``/``close`` are raw prices and whose
corporate actions are complete enough for the accounting claim it makes. A
``close_hfq`` proxy is intentionally not accepted here: mixing adjusted prices
with share/cash actions is exactly the ambiguity this module exists to remove.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class LedgerConfig:
    initial_cash: float
    max_positions: int = 10
    board_lot: int = 100
    commission_rate: float = 0.00025
    minimum_commission: float = 5.0
    stamp_duty_rate: float = 0.0005
    transfer_fee_rate: float = 0.00001
    slippage_per_side: float = 0.001


def _side_cost(notional: float, config: LedgerConfig, side: str) -> float:
    """Exact fee for one side, including the minimum-commission top-up."""
    if notional <= 0:
        return 0.0
    commission = max(config.minimum_commission, config.commission_rate * notional)
    transfer = config.transfer_fee_rate * notional
    slippage = config.slippage_per_side * notional
    stamp = config.stamp_duty_rate * notional if side == "sell" else 0.0
    return commission + transfer + slippage + stamp


def _flag(value) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        return False
    return bool(value)


def _date(value) -> pd.Timestamp:
    return pd.Timestamp(value).normalize()


def _targets_by_date(targets: Mapping) -> dict[pd.Timestamp, list[str]]:
    out: dict[pd.Timestamp, list[str]] = {}
    for key, codes in targets.items():
        out[_date(key)] = [str(code) for code in codes]
    return out


def _actions_by_date(actions: pd.DataFrame | None) -> dict[pd.Timestamp, list[dict]]:
    if actions is None or actions.empty:
        return {}
    required = {"code", "ex_date", "share_multiplier", "cash_per_share"}
    missing = required - set(actions.columns)
    if missing:
        raise ValueError(f"corporate actions missing columns: {sorted(missing)}")
    out: dict[pd.Timestamp, list[dict]] = {}
    for row in actions.to_dict("records"):
        out.setdefault(_date(row["ex_date"]), []).append(row)
    return out


def _exits_by_date(exits: pd.DataFrame | None) -> dict[pd.Timestamp, list[dict]]:
    if exits is None or exits.empty:
        return {}
    required = {"code", "date", "price"}
    missing = required - set(exits.columns)
    if missing:
        raise ValueError(f"delisting exits missing columns: {sorted(missing)}")
    out: dict[pd.Timestamp, list[dict]] = {}
    for row in exits.to_dict("records"):
        out.setdefault(_date(row["date"]), []).append(row)
    return out


def _quote(lookup: pd.DataFrame, day: pd.Timestamp, code: str):
    try:
        return lookup.loc[(day, code)]
    except KeyError:
        return None


def _tradeable_open(row, side: str, config: LedgerConfig) -> float | None:
    if row is None:
        return None
    try:
        open_price = float(row["open"])
    except (TypeError, ValueError, KeyError):
        return None
    if not np.isfinite(open_price) or open_price <= 0:
        return None
    status = row.get("trade_status")
    try:
        if status is None or pd.isna(status) or int(status) != 1:
            return None
    except (TypeError, ValueError):
        return None
    if side == "buy" and _flag(row.get("limit_up")):
        return None
    if side == "sell" and _flag(row.get("limit_down")):
        return None
    return open_price


def _close(row, fallback: float | None) -> tuple[float | None, bool]:
    if row is not None:
        try:
            close = float(row["close"])
        except (TypeError, ValueError, KeyError):
            close = None
        if close is not None and np.isfinite(close) and close > 0:
            return close, False
    return fallback, True


def replay_ledger(
    frame: pd.DataFrame,
    sessions: Sequence,
    targets: Mapping,
    config: LedgerConfig,
    *,
    initial_positions: Mapping[str, int] | None = None,
    initial_prices: Mapping[str, float] | None = None,
    corporate_actions: pd.DataFrame | None = None,
    delist_exits: pd.DataFrame | None = None,
) -> dict:
    """Run one causal T+1 account. All prices in ``frame`` are raw prices."""
    required = {"date", "stock_code", "open", "close", "trade_status"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"ledger panel missing columns: {sorted(missing)}")
    if len(sessions) == 0:
        raise ValueError("sessions must not be empty")

    dates = [_date(value) for value in sessions]
    if list(dates) != sorted(dates):
        raise ValueError("sessions must be ascending")
    targets_by_date = _targets_by_date(targets)
    actions = _actions_by_date(corporate_actions)
    exits = _exits_by_date(delist_exits)

    work = frame.copy()
    work["date"] = pd.to_datetime(work["date"]).dt.normalize()
    work["stock_code"] = work["stock_code"].astype(str)
    if work.duplicated(["date", "stock_code"]).any():
        raise ValueError("ledger panel has duplicate (date, stock_code) keys")
    lookup = work.set_index(["date", "stock_code"], drop=False).sort_index()

    positions: dict[str, int] = {
        str(code): int(lots) for code, lots in (initial_positions or {}).items()
    }
    if any(lots <= 0 for lots in positions.values()):
        raise ValueError("initial positions must be positive lots")
    if len(positions) > config.max_positions:
        raise ValueError(
            "initial positions exceed the strict cap; reduce them before the run"
        )
    cash = float(config.initial_cash)
    if cash < 0:
        raise ValueError("initial cash must be non-negative")

    last_price: dict[str, float] = {}
    for code, lots in positions.items():
        price = None
        if initial_prices and code in initial_prices:
            price = float(initial_prices[code])
        if price is None or not np.isfinite(price) or price <= 0:
            row = _quote(lookup, dates[0], code)
            price = _close(row, None)[0] if row is not None else None
        if price is None:
            raise ValueError(f"no initial price for held code {code}")
        last_price[code] = price

    pending_sells: set[str] = set()
    pending_buys: list[str] = []
    current_target: list[str] = []
    stale_days: dict[str, int] = {code: 0 for code in positions}
    fills: list[dict] = []
    blocked: list[dict] = []
    write_off_cny = 0.0
    fees = 0.0
    traded = 0.0
    nav_rows: list[dict] = []
    max_actual = len(positions)

    def record_block(day, code, side, reason):
        blocked.append(
            {"date": str(day.date()), "code": code, "side": side, "reason": reason}
        )

    for day in dates:
        # 1. Corporate actions apply before the open.
        for action in actions.get(day, []):
            code = str(action["code"])
            if code not in positions:
                continue
            lots = positions[code]
            multiplier = float(action.get("share_multiplier") or 1.0)
            cash_per_share = float(action.get("cash_per_share") or 0.0)
            if not np.isfinite(multiplier) or multiplier <= 0:
                raise ValueError(f"invalid share multiplier for {code}: {multiplier}")
            if cash_per_share:
                cash += lots * config.board_lot * cash_per_share
            positions[code] = int(round(lots * multiplier))

        # 2. Explicit delisting exits win over a missing quote.
        for event in exits.get(day, []):
            code = str(event["code"])
            if code not in positions:
                continue
            lots = positions.pop(code)
            price = float(event.get("price") or 0.0)
            if not np.isfinite(price) or price < 0:
                raise ValueError(f"invalid delisting exit price for {code}: {price}")
            if price > 0:
                notional = lots * config.board_lot * price
                cost = _side_cost(notional, config, "sell")
                cash += notional - cost
                fees += cost
                traded += notional
                side = "SELL"
            else:
                write_off_cny += lots * config.board_lot * last_price.get(code, 0.0)
                side = "WRITE_OFF"
            pending_sells.discard(code)
            pending_buys = [item for item in pending_buys if item != code]
            last_price.pop(code, None)
            stale_days.pop(code, None)
            fills.append(
                {
                    "date": str(day.date()),
                    "code": code,
                    "side": side,
                    "lots": lots,
                    "price": round(price, 6),
                }
            )

        # 3. Sells queued at the previous close fill at today's open.
        for code in sorted(pending_sells):
            if code not in positions:
                pending_sells.discard(code)
                continue
            row = _quote(lookup, day, code)
            price = _tradeable_open(row, "sell", config)
            if price is None:
                record_block(day, code, "SELL", "blocked_exit")
                continue
            lots = positions.pop(code)
            notional = lots * config.board_lot * price
            cost = _side_cost(notional, config, "sell")
            cash += notional - cost
            fees += cost
            traded += notional
            last_price.pop(code, None)
            stale_days.pop(code, None)
            pending_sells.discard(code)
            fills.append(
                {
                    "date": str(day.date()),
                    "code": code,
                    "side": "SELL",
                    "lots": lots,
                    "price": round(price, 6),
                }
            )

        # 4. Buys queued at the previous close fill at today's open, after sells.
        if pending_buys:
            open_book = 0.0
            for code, lots in positions.items():
                row = _quote(lookup, day, code)
                open_price = _tradeable_open(row, "sell", config)
                mark = (
                    open_price if open_price is not None else last_price.get(code, 0.0)
                )
                open_book += lots * config.board_lot * mark
            target_codes = list(pending_buys)
            pending_buys = []
            if target_codes:
                slice_amount = (cash + open_book) / len(target_codes)
                for code in target_codes:
                    if code in positions:
                        row = _quote(lookup, day, code)
                        price = _tradeable_open(row, "buy", config)
                        if price is None:
                            record_block(day, code, "BUY", "blocked_entry")
                            continue
                        desired = int(
                            np.floor(slice_amount / (price * config.board_lot))
                        )
                        delta = max(0, desired - positions[code])
                    else:
                        if len(positions) >= config.max_positions:
                            record_block(day, code, "BUY", "max_positions")
                            continue
                        row = _quote(lookup, day, code)
                        price = _tradeable_open(row, "buy", config)
                        if price is None:
                            record_block(day, code, "BUY", "blocked_entry")
                            continue
                        if len(positions) > config.max_positions:
                            record_block(day, code, "BUY", "max_positions")
                            continue
                        desired = int(
                            np.floor(slice_amount / (price * config.board_lot))
                        )
                        delta = desired
                    if delta <= 0:
                        continue
                    notional = delta * config.board_lot * price
                    cost = _side_cost(notional, config, "buy")
                    # Leave enough cash for the fee; an exact-size order would
                    # otherwise always fail the cash check.
                    while delta > 0 and notional + cost > cash:
                        delta -= 1
                        notional = delta * config.board_lot * price
                        cost = _side_cost(notional, config, "buy")
                    if delta <= 0:
                        record_block(day, code, "BUY", "insufficient_cash")
                        continue
                    cash -= notional + cost
                    fees += cost
                    traded += notional
                    positions[code] = positions.get(code, 0) + delta
                    last_price[code] = price
                    stale_days[code] = 0
                    fills.append(
                        {
                            "date": str(day.date()),
                            "code": code,
                            "side": "BUY",
                            "lots": delta,
                            "price": round(price, 6),
                        }
                    )

        # 5. Mark at today's close; a missing close carries the last price and
        #    increments staleness. It is never silently written down.
        book = 0.0
        for code, lots in positions.items():
            row = _quote(lookup, day, code)
            close, stale = _close(row, last_price.get(code))
            if close is None:
                raise ValueError(f"no mark for held code {code} on {day.date()}")
            last_price[code] = close
            stale_days[code] = stale_days.get(code, 0) + (1 if stale else 0)
            book += lots * config.board_lot * close
        nav = cash + book
        if nav < -1e-6:
            raise ValueError(f"negative NAV on {day.date()}: {nav}")
        if cash < -1e-6:
            raise ValueError(f"negative cash on {day.date()}: {cash}")
        max_actual = max(max_actual, len(positions))
        nav_rows.append(
            {
                "date": str(day.date()),
                "cash": round(cash, 2),
                "book": round(book, 2),
                "nav": round(nav, 2),
                "positions": len(positions),
                "stale_names": sum(1 for value in stale_days.values() if value > 0),
            }
        )

        # 6. Decide at this close; orders execute at the next session's open.
        if day in targets_by_date:
            current_target = targets_by_date[day]
            pending_sells = set(positions) - set(current_target)
            pending_buys = list(current_target)

    nav = np.array([row["nav"] for row in nav_rows], dtype="float64")
    first_day = dates[0]
    last_day = dates[-1]
    years = max((last_day - first_day).days / 365.25, 0.0)
    peak = np.maximum.accumulate(nav)
    average_nav = float(nav[1:].mean()) if len(nav) > 1 else float(nav[0])
    monthly = pd.Series(nav, index=pd.to_datetime([row["date"] for row in nav_rows]))
    # Match the audit protocol: the first month has no prior mark and the last
    # month is partial, so neither is a complete-month observation.
    monthly_returns = monthly.resample("ME").last().pct_change().dropna().iloc[:-1]
    positive_months = int((monthly_returns > 0).sum())
    complete_months = int(len(monthly_returns))

    return {
        "config": {
            "initial_cash": config.initial_cash,
            "max_positions": config.max_positions,
            "board_lot": config.board_lot,
            "commission_rate": config.commission_rate,
            "minimum_commission": config.minimum_commission,
            "stamp_duty_rate": config.stamp_duty_rate,
            "transfer_fee_rate": config.transfer_fee_rate,
            "slippage_per_side": config.slippage_per_side,
        },
        "summary": {
            "sessions": len(nav_rows),
            "first_session": str(first_day.date()),
            "last_session": str(last_day.date()),
            "total_return": float(nav[-1] / config.initial_cash - 1.0),
            "annualised": float((nav[-1] / config.initial_cash) ** (1.0 / years) - 1.0)
            if years > 0 and nav[-1] > 0
            else None,
            "max_drawdown": float((nav / peak - 1.0).min()),
            "positive_months": positive_months,
            "complete_months": complete_months,
            "positive_month_share": round(positive_months / complete_months, 4)
            if complete_months
            else None,
            "max_actual_positions": int(max_actual),
            "ending_positions": len(positions),
            "ending_cash": round(cash, 2),
            "fees": round(fees, 2),
            "traded_notional": round(traded, 2),
            "turnover_initial": round(traded / config.initial_cash / years, 4)
            if years > 0
            else None,
            "turnover_average_nav": round(traded / average_nav / years, 4)
            if years > 0 and average_nav > 0
            else None,
            "write_off_cny": round(write_off_cny, 2),
            "stale_names": int(sum(1 for value in stale_days.values() if value > 0)),
        },
        "nav": nav_rows,
        "fills": fills,
        "blocked_orders": blocked,
        "ending_positions": dict(positions),
        "ending_last_price": dict(last_price),
    }
