# -*- coding: utf-8 -*-
"""Allocation-family book semantics: bands, cadence and the per-asset stop.

This is the first place ``rebalance.cadence_days`` is actually read. Until now it
was validated, hashed and documented while every session re-planned the book —
the defect that started this program. v1 must stay exactly as it was, so the
consumption is registered as v2-only.
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "shared") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "shared"))

from caifubao_contracts.strategy_allocation import (  # noqa: E402
    MAX_BAND,
    plan_allocation,
    should_rebalance,
    validate_allocation,
)
from caifubao_contracts.strategy_spec import spec_hash, validate_strategy_spec  # noqa: E402

TARGETS = {"sh510300": 0.4, "sh510500": 0.3, "sh511010": 0.2}


def _spec(**overrides):
    spec = {
        "spec_version": "2",
        "universe": {"market": "CN_A", "as_of": "PIT"},
        "signal": {"score_model_version": "etf_allocation_v1", "horizon": 20},
        "selection": {
            "mode": "top_n",
            "portfolio_size": 3,
            "exit": {"stop_loss_pct": 0.1},
        },
        "sizing": {"weighting": "equal", "cash_reserve_pct": 0},
        "constraints": {},
        "rebalance": {"cadence_days": 60},
        "risk": {},
        "execution": {},
        "evidence": {"timing_version": "paper_causal_v1", "policy": "research"},
        "initial_nav": 100000,
        "family": "asset_allocation",
        "allocation": {"assets": dict(TARGETS), "band_pct": 0.05},
    }
    spec.update(overrides)
    return spec


# --------------------------------------------------------------------------
# allocation contract
# --------------------------------------------------------------------------


def test_allocation_normalises_weights_and_band():
    validated = validate_allocation({"assets": TARGETS, "band_pct": 0.05})
    assert validated["assets"] == TARGETS
    assert validated["band_pct"] == 0.05


def test_allocation_rejects_oversized_weights():
    with pytest.raises(ValueError, match="must not exceed 1.0"):
        validate_allocation({"assets": {"a": 0.7, "b": 0.4}, "band_pct": 0.05})


def test_allocation_requires_a_band():
    with pytest.raises(ValueError, match="band_pct is required"):
        validate_allocation({"assets": TARGETS})


def test_allocation_band_is_bounded():
    with pytest.raises(ValueError, match="must not exceed"):
        validate_allocation({"assets": TARGETS, "band_pct": MAX_BAND + 0.01})


def test_empty_asset_set_is_rejected():
    with pytest.raises(ValueError, match="non-empty map"):
        validate_allocation({"assets": {}, "band_pct": 0.05})


# --------------------------------------------------------------------------
# cadence is read here — the original defect
# --------------------------------------------------------------------------


def test_cadence_governs_whether_a_rebalance_happens():
    assert should_rebalance(0, 60) is False
    assert should_rebalance(59, 60) is False
    assert should_rebalance(60, 60) is True
    assert should_rebalance(61, 60) is True


def test_cadence_rejects_nonsense():
    with pytest.raises(ValueError, match="sessions_since_rebalance"):
        should_rebalance(-1, 60)
    with pytest.raises(ValueError, match="cadence_days"):
        should_rebalance(5, 0)


def test_off_cadence_session_does_not_rebalance():
    plan = plan_allocation(
        targets=TARGETS,
        band_pct=0.05,
        current_weights={"sh510300": 0.30, "sh510500": 0.30, "sh511010": 0.20},
        rebalance_session=False,
    )
    assert plan["actions"]["sh510300"] == "hold"
    assert plan["targets"]["sh510300"] == 0.30


def test_on_cadence_session_moves_back_to_target():
    plan = plan_allocation(
        targets=TARGETS,
        band_pct=0.05,
        current_weights={"sh510300": 0.30, "sh510500": 0.30, "sh511010": 0.20},
        rebalance_session=True,
    )
    assert plan["actions"]["sh510300"] == "rebalance"
    assert plan["targets"]["sh510300"] == 0.4


def test_inside_band_is_never_traded_back():
    plan = plan_allocation(
        targets=TARGETS,
        band_pct=0.05,
        current_weights={"sh510300": 0.42, "sh510500": 0.30, "sh511010": 0.20},
        rebalance_session=True,
    )
    assert plan["actions"]["sh510300"] == "hold"
    assert plan["targets"]["sh510300"] == 0.42


# --------------------------------------------------------------------------
# exits and closures are evaluated every session
# --------------------------------------------------------------------------


def test_per_asset_stop_fires_off_cadence():
    plan = plan_allocation(
        targets=TARGETS,
        band_pct=0.05,
        current_weights={"sh510300": 0.4, "sh510500": 0.3, "sh511010": 0.2},
        marks={"sh510300": 0.8},
        cost_basis={"sh510300": 1.0},
        stop_loss_pct=0.1,
        rebalance_session=False,
    )
    assert plan["actions"]["sh510300"] == "stop_out"
    assert plan["targets"]["sh510300"] == 0.0
    assert plan["cash_weight"] == pytest.approx(0.5)


def test_stop_needs_a_mark_and_a_cost_basis():
    plan = plan_allocation(
        targets=TARGETS,
        band_pct=0.05,
        current_weights={"sh510300": 0.4, "sh510500": 0.3, "sh511010": 0.2},
        marks={},
        cost_basis={},
        stop_loss_pct=0.1,
    )
    assert plan["actions"]["sh510300"] == "hold"


def test_asset_outside_the_declared_set_is_closed():
    plan = plan_allocation(
        targets=TARGETS,
        band_pct=0.05,
        current_weights={"sh510300": 0.4, "sh588000": 0.2},
    )
    assert plan["actions"]["sh588000"] == "close"
    assert plan["targets"]["sh588000"] == 0.0


def test_targeted_but_unheld_asset_is_opened():
    plan = plan_allocation(targets=TARGETS, band_pct=0.05, current_weights={})
    assert plan["actions"]["sh510300"] == "open"
    assert plan["targets"]["sh510300"] == 0.4
    assert plan["cash_weight"] == pytest.approx(0.1)


# --------------------------------------------------------------------------
# declaration level
# --------------------------------------------------------------------------


def test_v2_allocation_spec_requires_the_allocation_block():
    spec = _spec()
    spec.pop("allocation")
    with pytest.raises(ValueError, match="requires allocation"):
        validate_strategy_spec(spec)


def test_rank_family_rejects_an_allocation_block():
    spec = _spec(
        family="rank_selection",
        selection={
            "mode": "top_n",
            "portfolio_size": 3,
            "exit": {
                "rank_below": 10,
                "stop_loss_pct": 0.1,
                "max_hold_sessions": 20,
                "on_ineligible": "sell_next_open",
            },
        },
    )
    with pytest.raises(ValueError, match="only meaningful for asset_allocation"):
        validate_strategy_spec(spec)


def test_v2_allocation_spec_hashes_its_asset_set():
    base = spec_hash(_spec())
    changed = spec_hash(
        _spec(
            allocation={"assets": {"sh510300": 0.5, "sh510500": 0.3}, "band_pct": 0.05}
        )
    )
    assert base != changed
