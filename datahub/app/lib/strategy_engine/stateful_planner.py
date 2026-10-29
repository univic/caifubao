# -*- coding: utf-8 -*-
"""Plan a target book from an account state instead of from the previous book.

The current research path chains one target book into the next
(``previous_run.target_holdings``), which silently assumes every exit executed.
An account does not work that way: a suspended or limit-locked name cannot be
sold, keeps occupying a slot, and must therefore shrink the number of names the
next session may open.

This entry point takes the account state as the source of truth for the previous
book, the hold counts, the reserved slots and — when a declaration supplies one —
the exit contract. It is the v2 chaining mode; v1 keeps chaining target books so
the certified window ``01c196b8…`` does not change behaviour.
"""

from __future__ import annotations

from caifubao_contracts.strategy_decision_state import (
    blocked_slots,
    decision_state_hash,
    validate_decision_state,
)
from caifubao_contracts.strategy_exit_policy import (
    FAMILY_RANK_SELECTION,
    exit_rank,
    mandatory_exit_reason,
    validate_exit_policy,
)

from app.lib.strategy_engine.selection import plan_target_holdings


def _mandatory_exits(exit_policy: dict, held: list[dict], marks: dict | None) -> dict:
    """Positions that must be sold whatever their rank."""
    prices = marks or {}
    forced: dict[str, str] = {}
    for position in held:
        reason = mandatory_exit_reason(
            exit_policy,
            hold_count=position["hold_count"],
            mark=prices.get(position["stock_code"]),
            average_cost=position["average_cost"],
        )
        if reason:
            forced[position["stock_code"]] = reason
    return forced


def plan_from_state(
    *,
    state: dict,
    predictions,
    config: dict,
    eligible_codes: set[str] | None = None,
    industry_by_code: dict[str, str] | None = None,
    exit_buffer: float = 1.0,
    min_hold_sessions: int = 0,
    family: str = FAMILY_RANK_SELECTION,
    exit_policy: dict | None = None,
    marks: dict[str, float] | None = None,
) -> dict:
    """Return the next target book computed from the account state.

    Positions the account could not sell are reserved: they reduce new entries by
    one slot each, and sizing divides by the declared cap so the remaining names
    keep ``1/N`` instead of absorbing the reserved slot's weight.

    ``marks`` supplies the close used to evaluate the stop-loss. A held name with
    no mark keeps its position unless another mandatory exit fires — a stop-loss
    is never invented from a missing price.
    """
    state = validate_decision_state(state)
    held = state.get("positions") or []
    previous_holdings = [
        {"stock_code": position["stock_code"], "weight": 0.0} for position in held
    ]
    held_meta = {
        position["stock_code"]: {
            "hold_count": position["hold_count"],
            "entry_reason": position["entry_reason"],
        }
        for position in held
    }
    reserved = blocked_slots(state)
    size = int(config["selection"]["portfolio_size"])

    forced: dict[str, str] = {}
    rank_override: int | None = None
    policy: dict | None = None
    if exit_policy is not None:
        policy = validate_exit_policy(family, exit_policy)
        forced = _mandatory_exits(policy, held, marks)
        rank_override = exit_rank(policy, size, exit_buffer)
        if policy.get("min_hold_sessions") is not None:
            min_hold_sessions = policy["min_hold_sessions"]

    plan = plan_target_holdings(
        predictions,
        config,
        eligible_codes=eligible_codes,
        industry_by_code=industry_by_code,
        previous_holdings=previous_holdings or None,
        held_meta=held_meta,
        exit_buffer=exit_buffer,
        min_hold_sessions=min_hold_sessions,
        reserved_slots=reserved,
        sizing_basis="cap",
        mandatory_exits=forced or None,
        exit_rank_override=rank_override,
    )
    return {
        **plan,
        "reserved_slots": reserved,
        "state_hash": decision_state_hash(state),
        "entry_cap": max(0, size - reserved),
        "family": family,
        "mandatory_exits": forced,
    }
