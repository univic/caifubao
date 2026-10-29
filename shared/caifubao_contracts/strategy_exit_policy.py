# -*- coding: utf-8 -*-
"""The exit contract of a strategy declaration, validated per family.

Two families share risk, execution, accounting and ledger semantics but not the
shape of their exits:

* ``rank_selection`` sells on ranking, so it must declare **all four** conditions
  — the exit band, a stop-loss, a maximum holding period and the action taken
  when a name loses eligibility. Leaving one implicit is what let the paper book
  trade on rank alone with no stop at all.
* ``asset_allocation`` rebalances weights, so a rank exit is meaningless for it
  and is rejected rather than ignored. It still needs a **single-asset
  stop-loss**: portfolio-level de-risking does not protect one asset that
  collapses while the book is inside its bands.

Mandatory exits (stop-loss, maximum holding period, loss of eligibility) take
precedence over retention rules (the exit band, the minimum holding period). A
retention rule that could keep a stopped position is a bug, and an unsatisfiable
pair such as a minimum above the maximum is rejected here rather than silently
clamped by the planner.

Pure contract: no datahub, Mongo or broker imports.
"""

from __future__ import annotations

import copy
import math

FAMILY_RANK_SELECTION = "rank_selection"
FAMILY_ASSET_ALLOCATION = "asset_allocation"
FAMILIES = frozenset({FAMILY_RANK_SELECTION, FAMILY_ASSET_ALLOCATION})

ON_INELIGIBLE_SELL_NEXT_OPEN = "sell_next_open"
ON_INELIGIBLE_ACTIONS = frozenset({ON_INELIGIBLE_SELL_NEXT_OPEN})

_EXIT_KEYS = {
    "rank_below",
    "stop_loss_pct",
    "max_hold_sessions",
    "on_ineligible",
    "min_hold_sessions",
}


def _fraction(value, field: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or not 0 < float(value) < 1
    ):
        raise ValueError(f"{field} must be a finite fraction in (0, 1)")
    return float(value)


def _count(value, field: str, *, allow_zero: bool) -> int:
    if type(value) is not int or value < (0 if allow_zero else 1):
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def validate_exit_policy(family: str, policy: dict) -> dict:
    """Return a normalised exit policy or raise a specific error.

    ``rank_below``/``max_hold_sessions``/``on_ineligible`` are required for the
    rank family and forbidden for the allocation family; ``stop_loss_pct`` is
    required for both.
    """
    if family not in FAMILIES:
        raise ValueError(f"family must be one of {sorted(FAMILIES)}")
    if not isinstance(policy, dict):
        raise ValueError("exit policy must be an object")
    unknown = set(policy) - _EXIT_KEYS
    if unknown:
        raise ValueError(f"unknown exit policy fields: {sorted(unknown)}")

    result = {"family": family}
    if "stop_loss_pct" not in policy:
        raise ValueError(
            "exit.stop_loss_pct is required: no book may run without a stop-loss"
        )
    result["stop_loss_pct"] = _fraction(policy["stop_loss_pct"], "exit.stop_loss_pct")

    min_hold = policy.get("min_hold_sessions")
    if min_hold is not None:
        result["min_hold_sessions"] = _count(
            min_hold, "exit.min_hold_sessions", allow_zero=True
        )

    rank_only = ("rank_below", "max_hold_sessions", "on_ineligible")
    if family == FAMILY_ASSET_ALLOCATION:
        present = [key for key in rank_only if policy.get(key) is not None]
        if present:
            raise ValueError(
                "asset_allocation does not use rank exits; remove " + ", ".join(present)
            )
        return result

    for key in rank_only:
        if policy.get(key) is None:
            raise ValueError(
                f"rank_selection requires exit.{key}: ranking alone is not an exit contract"
            )
    result["rank_below"] = _count(
        policy["rank_below"], "exit.rank_below", allow_zero=False
    )
    result["max_hold_sessions"] = _count(
        policy["max_hold_sessions"], "exit.max_hold_sessions", allow_zero=False
    )
    action = policy["on_ineligible"]
    if action not in ON_INELIGIBLE_ACTIONS:
        raise ValueError(
            f"exit.on_ineligible must be one of {sorted(ON_INELIGIBLE_ACTIONS)}"
        )
    result["on_ineligible"] = action

    minimum = result.get("min_hold_sessions")
    if minimum is not None and minimum > result["max_hold_sessions"]:
        raise ValueError(
            "exit.min_hold_sessions must not exceed exit.max_hold_sessions: the "
            "mandatory exit would fire first and the declared minimum would be "
            "silently clamped"
        )
    return copy.deepcopy(result)


def mandatory_exit_reason(
    policy: dict, *, hold_count: int, mark, average_cost
) -> str | None:
    """Reason a held position must be sold regardless of its rank, else None.

    ``mark`` is the current close; ``None`` means no usable price, in which case
    the stop-loss cannot be evaluated and is not invented.
    """
    stop = policy.get("stop_loss_pct")
    if stop is not None and mark is not None and average_cost:
        if float(mark) < float(average_cost) * (1.0 - float(stop)):
            return "sold_stop_loss"
    max_hold = policy.get("max_hold_sessions")
    if max_hold is not None and int(hold_count) > int(max_hold):
        return "sold_max_hold"
    return None


def exit_rank(policy: dict, portfolio_size: int, fallback_buffer: float) -> int:
    """Rank beyond which a held name is sold, from the policy when declared."""
    declared = policy.get("rank_below")
    if declared is not None:
        return int(declared)
    from math import ceil

    return max(int(portfolio_size), int(ceil(portfolio_size * float(fallback_buffer))))
