# -*- coding: utf-8 -*-
"""Build a point-in-time total-return index from raw prices and the event ledger.

The raw OHLC series is the traded price.  The return index replaces the return
on ex-dates with the account-based event return and compounds the rest, so it
stays positive by construction (unlike 前复权 prices, which can go negative for
high-dividend stocks) and never invents a jump on a non-event day.

Adjusted OHLC for research is ``raw * (return_index / raw_close)``; the factor
changes only on ex-dates.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from app.lib.prices.corporate_actions import CorporateAction

DEFAULT_TOLERANCE = 0.005


@dataclass(frozen=True)
class PriceContractViolation:
    code: str
    date: str
    rule: str
    detail: str

    def as_dict(self) -> dict:
        return {
            "code": self.code,
            "date": self.date,
            "rule": self.rule,
            "detail": self.detail,
        }


def board_limit(code: str, date: pd.Timestamp) -> float:
    """Daily price limit for the board at a given date (10%/20%)."""
    if code.startswith(("688", "689")):
        return 0.20
    if code.startswith(("300", "301")) and date >= pd.Timestamp("2020-08-24"):
        return 0.20
    return 0.10


def build_return_index(
    raw: pd.DataFrame,
    actions: list[CorporateAction],
    *,
    code: str,
    tolerance: float = DEFAULT_TOLERANCE,
) -> tuple[pd.DataFrame, list[PriceContractViolation]]:
    """Return an adjusted frame and the list of contract violations.

    ``raw`` must be indexed by date with ``open/close/high/low`` columns.
    """
    frame = raw.sort_index().copy()
    if frame.index.has_duplicates:
        raise ValueError("raw price frame has duplicate dates")
    for column in ("open", "close", "high", "low"):
        if column not in frame.columns:
            raise ValueError(f"raw price frame missing {column!r}")
        if (frame[column] <= 0).any():
            raise ValueError("raw prices must be positive")
    violations: list[PriceContractViolation] = []
    close = frame["close"]
    returns = close.pct_change(fill_method=None)
    event_rows = set()
    for action in actions:
        if action.ex_date not in frame.index:
            violations.append(
                PriceContractViolation(
                    code,
                    str(action.ex_date.date()),
                    "event_date_missing",
                    action.action_type,
                )
            )
            continue
        position = frame.index.get_loc(action.ex_date)
        if position == 0:
            violations.append(
                PriceContractViolation(
                    code,
                    str(action.ex_date.date()),
                    "no_previous_close",
                    action.action_type,
                )
            )
            continue
        close_prev = float(close.iloc[position - 1])
        close_ex = float(close.iloc[position])
        event_ret = action.event_return(close_prev, close_ex)
        returns.iloc[position] = event_ret
        event_rows.add(frame.index[position])

    returns = returns.fillna(0.0)
    index = (1.0 + returns).cumprod()
    factor = index / close

    adjusted = frame.copy()
    for column in ("open", "close", "high", "low"):
        adjusted[column] = frame[column] * factor
    adjusted["return_index"] = index
    adjusted["adj_factor"] = factor
    adjusted["event_return"] = [
        returns.iloc[i] if frame.index[i] in event_rows else np.nan
        for i in range(len(frame))
    ]

    # The exchange limit applies to the raw traded move.  On an ex-date the
    # adjusted return legitimately differs from the raw move (bonus/rights), so
    # the check is applied to non-event days only.
    limit = np.array([board_limit(code, d) for d in frame.index])
    breaches = [
        i
        for i in range(len(frame))
        if frame.index[i] not in event_rows
        and abs(float(returns.iloc[i])) > limit[i] + tolerance
    ]
    for i in breaches:
        violations.append(
            PriceContractViolation(
                code,
                str(frame.index[i].date()),
                "daily_limit",
                f"return {returns.iloc[i]:.4%} exceeds {limit[i]:.0%}+{tolerance:.1%}",
            )
        )
    if (index <= 0).any():
        violations.append(
            PriceContractViolation(code, "", "non_positive_index", "return index <= 0")
        )
    if not np.allclose(adjusted["close"].to_numpy(), index.to_numpy()):
        violations.append(
            PriceContractViolation(
                code, "", "index_mismatch", "adj close != return index"
            )
        )
    return adjusted, violations


def validate_return_index(
    adjusted: pd.DataFrame,
    raw: pd.DataFrame,
    actions: list[CorporateAction],
    *,
    code: str,
    tolerance: float = DEFAULT_TOLERANCE,
) -> list[PriceContractViolation]:
    """Re-check an already-built index (used by the CLI and tests)."""
    rebuilt, violations = build_return_index(
        raw, actions, code=code, tolerance=tolerance
    )
    if not np.allclose(
        rebuilt["return_index"].to_numpy(), adjusted["return_index"].to_numpy()
    ):
        violations.append(
            PriceContractViolation(
                code, "", "rebuild_mismatch", "stored index differs from rebuild"
            )
        )
    return violations
