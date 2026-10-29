# -*- coding: utf-8 -*-
"""The exit contract: family-specific shape, and mandatory exits outrank retention.

The paper book currently exits on rank alone. These tests pin the contract that
replaces that: rank selection declares all four conditions, allocation declares a
per-asset stop instead of a rank exit, and a stop-loss or an over-held position is
sold even while it sits inside the exit band or its minimum holding period.
"""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "shared") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "shared"))

from caifubao_contracts.strategy_decision_state import DECISION_STATE_SCHEMA  # noqa: E402
from caifubao_contracts.strategy_exit_policy import (  # noqa: E402
    FAMILY_ASSET_ALLOCATION,
    FAMILY_RANK_SELECTION,
    exit_rank,
    mandatory_exit_reason,
    validate_exit_policy,
)
from app.lib.strategy_engine.selection import plan_target_holdings  # noqa: E402
from app.lib.strategy_engine.stateful_planner import plan_from_state  # noqa: E402

RANK_POLICY = {
    "rank_below": 40,
    "stop_loss_pct": 0.15,
    "max_hold_sessions": 40,
    "on_ineligible": "sell_next_open",
    "min_hold_sessions": 2,
}


def _config(size=20):
    return {
        "score_model_version": "flip_wide_shadow_v1",
        "horizon": 20,
        "selection": {
            "mode": "top_percentile",
            "lower": 0.20,
            "upper": 1.00,
            "portfolio_size": size,
        },
    }


def _state(codes_cost, blocked=None):
    positions = [
        {
            "stock_code": code,
            "quantity": 100,
            "average_cost": cost,
            "opened_on": "2026-10-20",
            "hold_count": hold_count,
            "entry_reason": "added_by_rank",
        }
        for code, cost, hold_count in codes_cost
    ]
    return {
        "schema": DECISION_STATE_SCHEMA,
        "as_of": "2026-10-28",
        "cash": 50_000.0,
        "high_water_mark": 100_000.0,
        "positions": positions,
        "outstanding_orders": [],
        "blocked_exits": dict(blocked or {}),
    }


def _predictions(count=64, first_score=100.0):
    return [
        SimpleNamespace(
            stock_code=f"sh60{index:04d}",
            score=first_score - index,
            percentile=0.9,
        )
        for index in range(count)
    ]


# --------------------------------------------------------------------------
# policy validation
# --------------------------------------------------------------------------


def test_rank_family_requires_every_exit_condition():
    for missing in ("rank_below", "max_hold_sessions", "on_ineligible"):
        policy = dict(RANK_POLICY)
        policy.pop(missing)
        with pytest.raises(ValueError, match=missing):
            validate_exit_policy(FAMILY_RANK_SELECTION, policy)


def test_stop_loss_is_required_for_every_family():
    policy = dict(RANK_POLICY)
    policy.pop("stop_loss_pct")
    with pytest.raises(ValueError, match="stop_loss_pct is required"):
        validate_exit_policy(FAMILY_RANK_SELECTION, policy)
    with pytest.raises(ValueError, match="stop_loss_pct is required"):
        validate_exit_policy(FAMILY_ASSET_ALLOCATION, {})


def test_allocation_rejects_a_rank_exit():
    policy = {"stop_loss_pct": 0.1, "rank_below": 20}
    with pytest.raises(ValueError, match="does not use rank exits"):
        validate_exit_policy(FAMILY_ASSET_ALLOCATION, policy)


def test_allocation_needs_only_the_per_asset_stop():
    validated = validate_exit_policy(FAMILY_ASSET_ALLOCATION, {"stop_loss_pct": 0.1})
    assert validated["stop_loss_pct"] == 0.1
    assert "rank_below" not in validated


def test_minimum_above_maximum_is_rejected():
    policy = dict(RANK_POLICY, min_hold_sessions=41)
    with pytest.raises(ValueError, match="silently clamped"):
        validate_exit_policy(FAMILY_RANK_SELECTION, policy)


def test_minimum_equal_to_maximum_is_allowed():
    policy = dict(RANK_POLICY, min_hold_sessions=40)
    assert (
        validate_exit_policy(FAMILY_RANK_SELECTION, policy)["min_hold_sessions"] == 40
    )


def test_unknown_exit_field_is_rejected():
    with pytest.raises(ValueError, match="unknown exit policy fields"):
        validate_exit_policy(
            FAMILY_RANK_SELECTION, dict(RANK_POLICY, trailing_stop=0.1)
        )


def test_unknown_family_is_rejected():
    with pytest.raises(ValueError, match="family must be one of"):
        validate_exit_policy("market_timing", {"stop_loss_pct": 0.1})


# --------------------------------------------------------------------------
# mandatory exit evaluation
# --------------------------------------------------------------------------


def test_stop_loss_uses_the_recorded_cost_basis():
    policy = validate_exit_policy(FAMILY_RANK_SELECTION, RANK_POLICY)
    # 10.0 cost, 15% stop -> sell below 8.5
    assert (
        mandatory_exit_reason(policy, hold_count=1, mark=8.4, average_cost=10.0)
        == "sold_stop_loss"
    )
    assert (
        mandatory_exit_reason(policy, hold_count=1, mark=8.6, average_cost=10.0) is None
    )


def test_stop_loss_is_not_invented_without_a_mark():
    policy = validate_exit_policy(FAMILY_RANK_SELECTION, RANK_POLICY)
    assert (
        mandatory_exit_reason(policy, hold_count=1, mark=None, average_cost=10.0)
        is None
    )


def test_max_hold_fires_only_past_the_limit():
    policy = validate_exit_policy(FAMILY_RANK_SELECTION, RANK_POLICY)
    assert (
        mandatory_exit_reason(policy, hold_count=40, mark=10.0, average_cost=10.0)
        is None
    )
    assert (
        mandatory_exit_reason(policy, hold_count=41, mark=10.0, average_cost=10.0)
        == "sold_max_hold"
    )


def test_declared_rank_replaces_the_derived_buffer():
    policy = validate_exit_policy(FAMILY_RANK_SELECTION, RANK_POLICY)
    assert exit_rank(policy, 20, 2.0) == 40
    assert exit_rank({"stop_loss_pct": 0.1}, 20, 2.0) == 40  # ceil(20 * 2.0)


# --------------------------------------------------------------------------
# planner precedence
# --------------------------------------------------------------------------


def test_stop_loss_sells_a_name_inside_the_band():
    state = _state([("sh600000", 10.0, 3)])
    plan = plan_from_state(
        state=state,
        predictions=_predictions(),
        config=_config(),
        exit_policy=RANK_POLICY,
        marks={"sh600000": 8.0},
    )
    assert plan["mandatory_exits"] == {"sh600000": "sold_stop_loss"}
    assert plan["reasons"]["sh600000"] == "sold_stop_loss"
    assert "sh600000" not in {holding["stock_code"] for holding in plan["holdings"]}


def test_max_hold_sells_even_while_inside_the_minimum_period():
    policy = dict(RANK_POLICY, min_hold_sessions=41, max_hold_sessions=41)
    state = _state([("sh600000", 10.0, 42)])
    plan = plan_from_state(
        state=state,
        predictions=_predictions(),
        config=_config(),
        exit_policy=policy,
        marks={"sh600000": 10.0},
    )
    assert plan["mandatory_exits"] == {"sh600000": "sold_max_hold"}


def test_band_keeps_a_name_when_no_mandatory_exit_fires():
    state = _state([("sh600030", 10.0, 3)])  # rank ~31, inside the 40 band
    plan = plan_from_state(
        state=state,
        predictions=_predictions(),
        config=_config(),
        exit_policy=RANK_POLICY,
        marks={"sh600030": 10.0},
    )
    assert plan["mandatory_exits"] == {}
    assert plan["reasons"]["sh600030"] == "held_in_exit_band"


def test_mandatory_reason_outranks_ineligibility():
    """A stopped name that also has no score keeps the more specific reason."""
    state = _state([("sh600000", 10.0, 3)])
    plan = plan_from_state(
        state=state,
        predictions=[p for p in _predictions() if p.stock_code != "sh600000"],
        config=_config(),
        exit_policy=RANK_POLICY,
        marks={"sh600000": 8.0},
    )
    assert plan["reasons"]["sh600000"] == "sold_stop_loss"


def test_unknown_mandatory_code_is_rejected():
    with pytest.raises(ValueError, match="not held"):
        plan_target_holdings(
            _predictions(),
            _config(),
            previous_holdings=[{"stock_code": "sh600002", "weight": 0.05}],
            mandatory_exits={"sh600001": "sold_stop_loss"},
        )


def test_allocation_policy_plans_without_a_rank_exit():
    state = _state([("sh600030", 10.0, 3)])
    plan = plan_from_state(
        state=state,
        predictions=_predictions(),
        config=_config(),
        family=FAMILY_ASSET_ALLOCATION,
        exit_policy={"stop_loss_pct": 0.1},
        marks={"sh600030": 10.0},
    )
    assert plan["family"] == FAMILY_ASSET_ALLOCATION
    assert plan["mandatory_exits"] == {}
