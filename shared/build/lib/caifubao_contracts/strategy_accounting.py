# -*- coding: utf-8 -*-
"""Accounting policy: how a period return is measured.

Paper NAV in this repository marks raw open/close, so a cash dividend is invisible
and an ex-dividend date looks like a loss. The contract's ledger can already carry
``DIVIDEND``/``SPLIT`` events; what was missing is a declaration of which
convention a strategy is measured under, so two runs cannot silently disagree.

Two knobs, and one rule that matters more than both:

* ``corporate_actions``: ``ex_date_cash`` credits a cash distribution on its ex
  date; ``price_only`` does not, and therefore understates total return.
* ``mark_price``: ``close_raw`` marks unadjusted prices; ``close_hfq`` marks an
  adjusted series in which the distribution is already embedded.

Combining ``close_hfq`` with ``ex_date_cash`` double counts the distribution, so
it is rejected rather than reconciled.

Pure contract: no datahub, Mongo or broker imports.
"""

from __future__ import annotations

import math

CORPORATE_ACTIONS_EX_DATE_CASH = "ex_date_cash"
CORPORATE_ACTIONS_PRICE_ONLY = "price_only"
CORPORATE_ACTION_POLICIES = frozenset(
    {CORPORATE_ACTIONS_EX_DATE_CASH, CORPORATE_ACTIONS_PRICE_ONLY}
)

MARK_PRICE_RAW = "close_raw"
MARK_PRICE_HFQ = "close_hfq"
MARK_PRICES = frozenset({MARK_PRICE_RAW, MARK_PRICE_HFQ})


def _number(value, field: str, *, allow_zero: bool = True) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) < 0
        or (not allow_zero and float(value) == 0)
    ):
        raise ValueError(f"{field} must be a finite non-negative number")
    return float(value)


def validate_accounting(policy: dict) -> dict:
    """Validate an accounting policy; reject the double-counting combination."""
    if not isinstance(policy, dict):
        raise ValueError("accounting must be an object")
    unknown = set(policy) - {"corporate_actions", "mark_price"}
    if unknown:
        raise ValueError(f"unknown accounting fields: {sorted(unknown)}")

    actions = policy.get("corporate_actions")
    if actions not in CORPORATE_ACTION_POLICIES:
        raise ValueError(
            f"accounting.corporate_actions must be one of "
            f"{sorted(CORPORATE_ACTION_POLICIES)}"
        )
    mark = policy.get("mark_price")
    if mark not in MARK_PRICES:
        raise ValueError(f"accounting.mark_price must be one of {sorted(MARK_PRICES)}")

    if mark == MARK_PRICE_HFQ and actions == CORPORATE_ACTIONS_EX_DATE_CASH:
        raise ValueError(
            "accounting must not combine close_hfq with ex_date_cash: the adjusted "
            "series already contains the distribution, so crediting it again "
            "double counts the return"
        )
    return {"corporate_actions": actions, "mark_price": mark}


def period_return(
    *,
    policy: dict,
    start_mark: float,
    end_mark: float,
    cash_dividends: float = 0.0,
) -> float:
    """Return over the period under the declared policy.

    With ``close_raw`` the marks are unadjusted, so a cash distribution only shows
    up if the policy credits it. With ``close_hfq`` the marks already carry it and
    any credited amount would be counted twice, which validation forbids.
    """
    validated = validate_accounting(policy)
    start = _number(start_mark, "start_mark", allow_zero=False)
    end = _number(end_mark, "end_mark")
    dividends = _number(cash_dividends, "cash_dividends")
    if validated["corporate_actions"] == CORPORATE_ACTIONS_EX_DATE_CASH:
        return (end + dividends - start) / start
    return (end - start) / start


def total_return(
    *, policy: dict, marks: list[float], cash_dividends: list[float] | None = None
) -> float:
    """Chain period returns; dividends are indexed positionally after the first mark."""
    if not isinstance(marks, list) or len(marks) < 2:
        raise ValueError("marks must be a list of at least two prices")
    dividends = list(cash_dividends or [])
    if dividends and len(dividends) != len(marks) - 1:
        raise ValueError(
            "cash_dividends must supply one value per period between the marks"
        )
    growth = 1.0
    for index in range(1, len(marks)):
        growth *= 1.0 + period_return(
            policy=policy,
            start_mark=marks[index - 1],
            end_mark=marks[index],
            cash_dividends=dividends[index - 1] if dividends else 0.0,
        )
    return growth - 1.0
