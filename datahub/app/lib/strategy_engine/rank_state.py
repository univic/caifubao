# -*- coding: utf-8 -*-
"""Build an account-side decision state for the rank-selection paper path.

The rank runner plans target weights, not shares, so it has no account of its
own. ``plan_from_state`` needs a state, and fabricating one inside the runner
would put invented numbers in an object the exit rules trust. This module builds
the state from what the paper path actually records, and refuses when it cannot:

* the previous book and its hold counts come from the previous run;
* the cost basis comes from the prices recorded when that book was planned;
* the quantity is the notional implied by the recorded weight, the recorded
  price and the declared NAV, rounded down to a board lot;
* cash is whatever the weights do not invest;
* no position is marked blocked, because the research plan path never observes a
  refused fill - that belongs to the simulated account, not to the plan.

The state is an input to a decision, not an account: it exists so a stop-loss has
a cost basis to measure against, and nothing here may be read as a holding.
"""

from __future__ import annotations

from caifubao_contracts.strategy_decision_state import (
    DECISION_STATE_SCHEMA,
    validate_decision_state,
)


class RankStateError(ValueError):
    """The paper path does not record enough to build a decision state."""


def build_decision_state(
    *,
    as_of: str,
    previous_holdings: list[dict] | None,
    held_meta: dict | None,
    reference_prices: dict | None,
    closes: dict | None,
    initial_nav: float,
    board_lot: int = 100,
) -> dict:
    """Return a validated decision state for the previous book."""
    if not as_of:
        raise RankStateError("as_of is required")
    if float(initial_nav) <= 0:
        raise RankStateError("initial_nav must be positive")
    if type(board_lot) is not int or board_lot < 1:
        raise RankStateError("board_lot must be a positive integer")

    holdings = list(previous_holdings or [])
    meta = dict(held_meta or {})
    prices = dict(reference_prices or {})
    marks = dict(closes or {})

    positions = []
    invested = 0.0
    for holding in holdings:
        code = holding.get("stock_code")
        weight = float(holding.get("weight") or 0.0)
        if not code or weight <= 0:
            continue
        close = marks.get(code)
        cost = prices.get(code) or close
        if close is None or not cost:
            raise RankStateError(
                f"no price recorded for held name {code!r}; refusing to build a "
                "state that would make its stop-loss unmeasurable"
            )
        budget = float(initial_nav) * weight
        lots = int(budget // (float(close) * board_lot))
        quantity = lots * board_lot
        if quantity <= 0:
            # A position below one lot was never bought; keep it at one lot so the
            # state stays a faithful record of the weight the plan asked for.
            quantity = board_lot
        record = meta.get(code) or {}
        positions.append(
            {
                "stock_code": code,
                "quantity": quantity,
                "average_cost": float(cost),
                "hold_count": max(1, int(record.get("hold_count") or 1)),
                "entry_reason": str(record.get("entry_reason") or "added_by_rank"),
            }
        )
        invested += weight

    return validate_decision_state(
        {
            "schema": DECISION_STATE_SCHEMA,
            "as_of": str(as_of)[:10],
            "cash": max(0.0, float(initial_nav) * (1.0 - invested)),
            "high_water_mark": float(initial_nav),
            "positions": positions,
            "outstanding_orders": [],
            "blocked_exits": {},
        }
    )
