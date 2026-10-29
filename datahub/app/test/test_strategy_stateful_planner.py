# -*- coding: utf-8 -*-
"""Account-state planning: reserved slots and cap-basis sizing.

Two invariants under test: an unsellable holding must reduce new entries rather
than let the book grow past its cap, and it must not inflate the weight of the
names that are bought. v1 keeps the old target-chaining behaviour, so the golden
regression in ``test_strategy_v1_regression.py`` has to stay green.
"""

import copy
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "shared") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "shared"))

from caifubao_contracts.strategy_decision_state import (  # noqa: E402
    DECISION_STATE_SCHEMA,
    blocked_slots,
    decision_state_hash,
    held_codes,
    validate_decision_state,
)
from app.lib.strategy_engine.selection import plan_target_holdings  # noqa: E402
from app.lib.strategy_engine.stateful_planner import plan_from_state  # noqa: E402

CANDIDATES = 64


def _state(stock_codes=(), blocked=None, cash=100_000.0):
    positions = [
        {
            "stock_code": code,
            "quantity": 100,
            "average_cost": 10.0,
            "opened_on": "2026-10-20",
            "hold_count": 3,
            "entry_reason": "added_by_rank",
        }
        for code in stock_codes
    ]
    return {
        "schema": DECISION_STATE_SCHEMA,
        "as_of": "2026-10-28",
        "cash": cash,
        "high_water_mark": 105_000.0,
        "positions": positions,
        "outstanding_orders": [],
        "blocked_exits": dict(blocked or {}),
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


def _predictions(count=CANDIDATES, skip=()):
    rows = []
    for index in range(count):
        code = f"sh60{index:04d}"
        if code in skip:
            continue
        rows.append(
            SimpleNamespace(
                stock_code=code,
                score=float(count - index),
                percentile=1.0 - index / count,
            )
        )
    return rows


# --------------------------------------------------------------------------
# decision state contract
# --------------------------------------------------------------------------


def test_state_validates_and_copies():
    state = _state(["sh600001"])
    validated = validate_decision_state(state)
    assert validated["positions"][0]["stock_code"] == "sh600001"
    validated["positions"][0]["quantity"] = 999
    assert state["positions"][0]["quantity"] == 100


def test_state_rejects_unknown_fields():
    state = _state(["sh600001"])
    state["shadow_cash"] = 1.0
    with pytest.raises(ValueError, match="unknown decision state fields"):
        validate_decision_state(state)


def test_state_rejects_duplicate_positions():
    state = _state(["sh600001", "sh600001"])
    with pytest.raises(ValueError, match="duplicate position"):
        validate_decision_state(state)


def test_blocked_exit_must_name_a_held_position():
    state = _state(["sh600001"], blocked={"sh600002": "suspended"})
    with pytest.raises(ValueError, match="state does not hold"):
        validate_decision_state(state)


def test_blocked_reason_is_restricted():
    state = _state(["sh600001"], blocked={"sh600001": "felt_wrong"})
    with pytest.raises(ValueError, match="blocked_exits"):
        validate_decision_state(state)


def test_negative_cash_is_rejected():
    state = _state(cash=-1.0)
    with pytest.raises(ValueError, match="cash"):
        validate_decision_state(state)


def test_hash_is_order_insensitive_and_stable():
    first = _state(["sh600001", "sh600002"])
    second = copy.deepcopy(first)
    second = {
        "blocked_exits": second["blocked_exits"],
        "outstanding_orders": second["outstanding_orders"],
        "positions": second["positions"],
        "high_water_mark": second["high_water_mark"],
        "cash": second["cash"],
        "as_of": second["as_of"],
        "schema": second["schema"],
    }
    assert decision_state_hash(first) == decision_state_hash(second)


def test_hash_changes_when_cost_basis_changes():
    first = _state(["sh600001"])
    second = copy.deepcopy(first)
    second["positions"][0]["average_cost"] = 11.0
    assert decision_state_hash(first) != decision_state_hash(second)


def test_blocked_slots_counts_only_held_blocks():
    state = _state(["sh600001"], blocked={"sh600001": "limit_down"})
    assert blocked_slots(state) == 1
    assert held_codes(state) == frozenset({"sh600001"})


# --------------------------------------------------------------------------
# reserved slots and sizing basis
# --------------------------------------------------------------------------


def test_reserved_slot_reduces_new_entries():
    state = _state(["sh600001"], blocked={"sh600001": "suspended"})
    plan = plan_from_state(
        state=state, predictions=_predictions(), config=_config(), exit_buffer=2.0
    )
    assert plan["reserved_slots"] == 1
    assert plan["entry_cap"] == 19
    assert len(plan["holdings"]) == 19


def test_reserved_slot_does_not_inflate_weights():
    state = _state(["sh600001"], blocked={"sh600001": "suspended"})
    plan = plan_from_state(
        state=state, predictions=_predictions(), config=_config(), exit_buffer=2.0
    )
    weights = [holding["weight"] for holding in plan["holdings"]]
    assert weights == [pytest.approx(1 / 20)] * 19
    assert sum(weights) == pytest.approx(19 / 20)


def test_actual_basis_keeps_the_v1_weight():
    """v1 divides by the number of names chosen; that must not change."""
    plan = plan_target_holdings(
        _predictions(),
        _config(),
        previous_holdings=None,
        sizing_basis="actual",
    )
    assert plan["holdings"][0]["weight"] == pytest.approx(1 / 20)


def test_cap_basis_divides_by_the_declared_cap():
    # Constant percentile keeps the assertion off the 0.20 eligibility edge.
    predictions = [
        SimpleNamespace(
            stock_code=f"sh60{index:04d}", score=10.0 - index, percentile=0.9
        )
        for index in range(10)
    ]
    plan = plan_target_holdings(
        predictions,
        _config(),
        previous_holdings=None,
        sizing_basis="cap",
    )
    # Only ten candidates exist, so the book is partial; cap basis keeps 1/20.
    assert len(plan["holdings"]) == 10
    assert plan["holdings"][0]["weight"] == pytest.approx(1 / 20)


def test_reserved_slots_cannot_exceed_the_cap():
    plan = plan_target_holdings(
        _predictions(),
        _config(),
        previous_holdings=None,
        reserved_slots=25,
    )
    assert plan["holdings"] == []


def test_invalid_arguments_are_rejected():
    with pytest.raises(ValueError, match="reserved_slots"):
        plan_target_holdings(_predictions(), _config(), reserved_slots=-1)
    with pytest.raises(ValueError, match="sizing_basis"):
        plan_target_holdings(_predictions(), _config(), sizing_basis="cap-basis")


def test_plan_from_state_ignores_held_without_a_score():
    """A blocked name with no prediction stays sold-out of the target book."""
    state = _state(["sh600001"], blocked={"sh600001": "suspended"})
    plan = plan_from_state(
        state=state,
        predictions=_predictions(skip={"sh600001"}),
        config=_config(),
        exit_buffer=2.0,
    )
    codes = {holding["stock_code"] for holding in plan["holdings"]}
    assert "sh600001" not in codes
    assert len(codes) == 19
