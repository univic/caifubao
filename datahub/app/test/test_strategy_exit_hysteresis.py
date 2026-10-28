# -*- coding: utf-8 -*-
"""Exit hysteresis (exit buffer + minimum holding period) and its spec gate.

These tests pin the research-proven behaviour behind
``rebalance.exit_buffer`` / ``rebalance.min_hold_sessions`` and, just as
importantly, that a spec which does not declare them keeps its exact identity.
"""

import json

import pytest

from app.lib.strategy_engine.selection import (
    plan_target_holdings,
    select_target_holdings,
)
from app.lib.strategy_engine.spec import spec_hash, spec_to_legacy_config
from caifubao_contracts.strategy_spec import validate_strategy_spec

# The certified research window spec (spec_hash c1310492...). Any accidental
# default injected into normalization changes this hash and breaks the window.
CERTIFIED_SPEC = json.loads(r"""
{
    "spec_version": "1",
    "universe": {
        "market": "CN_A",
        "as_of": "PIT"
    },
    "signal": {
        "score_model_version": "flip_wide_shadow_v1",
        "horizon": 20
    },
    "selection": {
        "mode": "top_percentile",
        "lower": 0.2,
        "upper": 1,
        "portfolio_size": 20
    },
    "sizing": {
        "weighting": "equal",
        "cash_reserve_pct": 0
    },
    "constraints": {
        "exclude_st": true,
        "exclude_bse": true,
        "exclude_suspended": true,
        "max_single_stock_pct": 0.05,
        "min_trade_amount_cny": 0,
        "exclude_infeasible_buys": true
    },
    "rebalance": {
        "cadence_days": 5
    },
    "risk": {},
    "execution": {},
    "evidence": {
        "timing_version": "paper_causal_v1",
        "policy": "research"
    },
    "initial_nav": 100000
}
""")
CERTIFIED_HASH = "c13104924c2d92c01fcfe06f40b8b99d46b5edda6b87f8c262638d3ac0c0a3d3"

CFG = {
    "score_model_version": "flip_wide_shadow_v1",
    "selection": {
        "mode": "top_percentile",
        "lower": 0.20,
        "upper": 1.0,
        "portfolio_size": 3,
    },
}


class _Pred:
    def __init__(self, code, score, percentile):
        self.stock_code = code
        self.score = score
        self.percentile = percentile


def _preds():
    return [
        _Pred("sh600001", 100.0, 0.99),
        _Pred("sh600002", 90.0, 0.95),
        _Pred("sh600003", 80.0, 0.90),
        _Pred("sh600004", 70.0, 0.85),
        _Pred("sh600005", 60.0, 0.80),
    ]


def _codes(result):
    return [item["stock_code"] for item in result]


def test_no_policy_is_a_full_resort_and_keeps_the_old_shape():
    predicted = plan_target_holdings(
        _preds(),
        CFG,
        previous_holdings=[
            {"stock_code": "sh600004", "weight": 0.33},
        ],
    )
    legacy = select_target_holdings(_preds(), CFG)
    assert predicted["holdings"] == legacy
    assert _codes(legacy) == ["sh600001", "sh600002", "sh600003"]
    # every emitted holding is exactly {stock_code, weight}: the run document
    # keeps its persisted shape, metadata lives in hold_meta
    assert set(legacy[0]) == {"stock_code", "weight"}
    assert predicted["reasons"]["sh600004"] == "sold_out_of_exit_band"


def test_exit_buffer_keeps_a_held_name_inside_the_wider_band():
    held = [{"stock_code": "sh600004", "weight": 0.33}]
    wide = plan_target_holdings(_preds(), CFG, previous_holdings=held, exit_buffer=2.0)
    assert "sh600004" in _codes(wide["holdings"])
    assert wide["reasons"]["sh600004"] == "held_in_exit_band"
    # the wider band displaces the weakest newcomer, not the held name
    assert _codes(wide["holdings"]) == ["sh600001", "sh600002", "sh600004"]


def test_min_hold_keeps_a_young_position_that_left_the_band():
    held = [{"stock_code": "sh600005", "weight": 0.33}]
    meta = {"sh600005": {"hold_count": 0, "entry_reason": "added_by_rank"}}
    planned = plan_target_holdings(
        _preds(),
        CFG,
        previous_holdings=held,
        held_meta=meta,
        min_hold_sessions=2,
    )
    assert "sh600005" in _codes(planned["holdings"])
    assert planned["reasons"]["sh600005"] == "held_min_period"
    # once the minimum has elapsed the same rank is sold again
    old = plan_target_holdings(
        _preds(),
        CFG,
        previous_holdings=held,
        held_meta={"sh600005": {"hold_count": 2}},
        min_hold_sessions=2,
    )
    assert "sh600005" not in _codes(old["holdings"])
    assert old["reasons"]["sh600005"] == "sold_out_of_exit_band"


def test_hold_meta_counts_sessions_and_records_entry_reason():
    held = [{"stock_code": "sh600004", "weight": 0.33}]
    meta = {"sh600004": {"hold_count": 4, "entry_reason": "added_by_rank"}}
    planned = plan_target_holdings(
        _preds(), CFG, previous_holdings=held, held_meta=meta, exit_buffer=2.0
    )
    assert planned["hold_meta"]["sh600004"] == {
        "hold_count": 5,
        # the opening reason survives for the whole holding period; this run's
        # outcome is recorded in `reasons`, not overwritten here
        "entry_reason": "added_by_rank",
    }
    newcomer = [code for code in _codes(planned["holdings"]) if code != "sh600004"]
    for code in newcomer:
        assert planned["hold_meta"][code] == {
            "hold_count": 1,
            "entry_reason": "added_by_rank",
        }


def test_a_held_name_without_a_score_is_force_sold():
    held = [{"stock_code": "sh600009", "weight": 0.33}]
    planned = plan_target_holdings(
        _preds(), CFG, previous_holdings=held, exit_buffer=3.0
    )
    assert "sh600009" not in _codes(planned["holdings"])
    assert planned["reasons"]["sh600009"] == "sold_ineligible"


def test_spec_without_hysteresis_keeps_its_certified_hash():
    assert spec_hash(CERTIFIED_SPEC) == CERTIFIED_HASH
    validate_strategy_spec(CERTIFIED_SPEC)
    config = spec_to_legacy_config(CERTIFIED_SPEC)
    assert "exit_buffer" not in config["rebalance"]
    assert "min_hold_sessions" not in config["rebalance"]


def test_spec_hysteresis_validates_and_changes_the_identity():
    spec = json.loads(json.dumps(CERTIFIED_SPEC))
    spec["rebalance"] = {
        **spec["rebalance"],
        "exit_buffer": 2.0,
        "min_hold_sessions": 10,
    }
    validate_strategy_spec(spec)
    assert spec_hash(spec) != CERTIFIED_HASH
    # the legacy projection must stay byte-identical: hysteresis is a spec
    # semantic, not a legacy config key
    legacy = spec_to_legacy_config(spec)
    assert legacy["rebalance"] == {"cadence_days": 5}


@pytest.mark.parametrize(
    "rebalance, message",
    [
        ({"cadence_days": 5, "exit_buffer": 0.9}, "finite number"),
        ({"cadence_days": 5, "exit_buffer": "wide"}, "must be a finite number"),
        ({"cadence_days": 5, "min_hold_sessions": -1}, "non-negative integer"),
        ({"cadence_days": 5, "min_hold_sessions": 1.5}, "non-negative integer"),
        ({"cadence_days": 5, "min_hold": 2}, "unknown rebalance"),
    ],
)
def test_spec_rejects_invalid_hysteresis(rebalance, message):
    spec = json.loads(json.dumps(CERTIFIED_SPEC))
    spec["rebalance"] = rebalance
    with pytest.raises(ValueError, match=message):
        validate_strategy_spec(spec)


def test_hysteresis_spec_round_trips_through_the_legacy_projection():
    """P1 regression: a spec that declares the policy must still pass its own
    identity check after the legacy projection strips the spec-only fields."""
    from app.lib.strategy_engine.spec import legacy_to_spec

    spec = json.loads(json.dumps(CERTIFIED_SPEC))
    spec["rebalance"] = {
        **spec["rebalance"],
        "exit_buffer": 2.0,
        "min_hold_sessions": 10,
    }
    identity = spec_hash(spec)
    legacy = spec_to_legacy_config(spec)
    reconstructed = legacy_to_spec(legacy)
    identity_spec = {
        **reconstructed["spec"],
        "rebalance": {
            **reconstructed["spec"]["rebalance"],
            "exit_buffer": 2.0,
            "min_hold_sessions": 10,
        },
    }
    assert spec_hash(identity_spec) == identity
    # without re-attaching the policy the reconstruction must NOT match: that is
    # exactly the failure mode this test pins
    assert reconstructed["spec_hash"] != identity


def test_replay_and_paper_agree_under_hysteresis():
    from app.lib.strategy_engine.replay import replay_strategy_spec_decision
    from app.lib.strategy_engine.runner import assemble_daily_plan
    from app.lib.strategy_engine.spec import spec_to_legacy_config

    spec = json.loads(json.dumps(CERTIFIED_SPEC))
    spec["rebalance"] = {
        **spec["rebalance"],
        "exit_buffer": 2.0,
        "min_hold_sessions": 0,
    }
    spec["selection"] = {**spec["selection"], "portfolio_size": 3}
    config = spec_to_legacy_config(spec)
    preds = _preds()
    held = [{"stock_code": "sh600004", "weight": 0.33}]
    meta = {"sh600004": {"hold_count": 3, "entry_reason": "added_by_rank"}}
    flags = {
        pred.stock_code: {
            "trade_status": 1,
            "is_st": 0,
            "is_bse": 0,
            "trade_amount": 1e8,
            "reference_close": 10.0,
        }
        for pred in preds
    }

    replay = replay_strategy_spec_decision(
        spec=spec,
        date=__import__("datetime").datetime(2026, 10, 27),
        predictions=preds,
        previous_holdings=held,
        held_meta=meta,
        flags=flags,
    )
    paper = assemble_daily_plan(
        config=config,
        date=__import__("datetime").datetime(2026, 10, 27),
        predictions=preds,
        previous_holdings=held,
        held_meta=meta,
        flags=flags,
        exit_buffer=2.0,
        min_hold_sessions=0,
    )
    assert replay["target_holdings"] == paper["target_holdings"]
    assert "sh600004" in _codes(replay["target_holdings"])
