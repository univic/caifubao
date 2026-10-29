# -*- coding: utf-8 -*-
"""v2 declarations: family plus an explicit exit contract.

v1 stays frozen — a v1 declaration carrying a v2-only field is rejected rather
than absorbed into a hash that would later read differently. v2 requires the
family and the exit block, and delegates their semantics to the exit-policy
contract so the rule lives in one place.
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "shared") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "shared"))

from caifubao_contracts.strategy_spec import (  # noqa: E402
    SUPPORTED_SPEC_VERSIONS,
    spec_hash,
    validate_strategy_spec,
)

EXIT = {
    "rank_below": 40,
    "stop_loss_pct": 0.15,
    "max_hold_sessions": 40,
    "on_ineligible": "sell_next_open",
}


def _v1():
    return {
        "spec_version": "1",
        "universe": {"market": "CN_A", "as_of": "PIT"},
        "signal": {"score_model_version": "flip_wide_shadow_v1", "horizon": 20},
        "selection": {"mode": "top_n", "portfolio_size": 20},
        "sizing": {"weighting": "equal", "cash_reserve_pct": 0},
        "constraints": {},
        "rebalance": {"cadence_days": 5},
        "risk": {},
        "execution": {},
        "evidence": {"timing_version": "paper_causal_v1", "policy": "research"},
        "initial_nav": 100000,
    }


ALLOCATION = {"assets": {"sh510300": 0.4, "sh510500": 0.3}, "band_pct": 0.05}


ACCOUNTING = {"corporate_actions": "price_only", "mark_price": "close_raw"}
BENCHMARK = {"kind": "equal_weight_universe"}
KILL = {
    "min_excess_per_period": 0.0,
    "max_drawdown": 0.2,
    "review_after_sessions": 60,
}


def _v2(family="rank_selection", exit_policy=None):
    spec = _v1()
    spec["spec_version"] = "2"
    spec["family"] = family
    spec["selection"] = dict(spec["selection"], exit=dict(exit_policy or EXIT))
    spec["accounting"] = dict(ACCOUNTING)
    spec["benchmark"] = dict(BENCHMARK)
    spec["evidence"] = dict(spec["evidence"], kill_criteria=dict(KILL))
    if family == "asset_allocation":
        # The allocation family's book semantics are its fixed asset set: it
        # declares only its exit contract and no rank-selection blocks.
        spec["allocation"] = dict(ALLOCATION)
        spec["selection"] = {"exit": dict(exit_policy or EXIT)}
        spec.pop("sizing", None)
        spec.pop("constraints", None)
    return spec


def test_both_versions_are_supported():
    assert set(SUPPORTED_SPEC_VERSIONS) == {"1", "2"}


def test_v1_is_unchanged_by_the_v2_work():
    """The certified window's identity must not move."""
    first = spec_hash(_v1())
    assert first == spec_hash(_v1())
    # Adding the v2 fields to the ALLOWED table must not change v1 normalisation.
    assert "family" not in validate_strategy_spec(_v1())


def test_v2_requires_a_family():
    spec = _v2()
    spec.pop("family")
    with pytest.raises(ValueError, match="requires family"):
        validate_strategy_spec(spec)


def test_v2_requires_the_exit_block():
    spec = _v2()
    spec["selection"] = {"mode": "top_n", "portfolio_size": 20}
    with pytest.raises(ValueError, match="selection.exit is required"):
        validate_strategy_spec(spec)


def test_v2_rank_family_accepts_a_full_exit_contract():
    validated = validate_strategy_spec(_v2())
    assert validated["family"] == "rank_selection"
    assert validated["selection"]["exit"]["stop_loss_pct"] == 0.15


def test_v2_rank_family_rejects_a_missing_condition():
    policy = dict(EXIT)
    policy.pop("stop_loss_pct")
    with pytest.raises(ValueError, match="stop_loss_pct is required"):
        validate_strategy_spec(_v2(exit_policy=policy))


def test_v2_allocation_rejects_a_rank_exit():
    policy = {"stop_loss_pct": 0.1, "rank_below": 20}
    with pytest.raises(ValueError, match="does not use rank exits"):
        validate_strategy_spec(_v2(family="asset_allocation", exit_policy=policy))


def test_v2_allocation_accepts_a_per_asset_stop_only():
    validated = validate_strategy_spec(
        _v2(family="asset_allocation", exit_policy={"stop_loss_pct": 0.1})
    )
    assert validated["selection"]["exit"] == {"stop_loss_pct": 0.1}


def test_v1_rejects_v2_only_fields():
    spec = _v1()
    spec["family"] = "rank_selection"
    with pytest.raises(ValueError, match="spec_version 1 does not support"):
        validate_strategy_spec(spec)


def test_v1_rejects_the_exit_block():
    spec = _v1()
    spec["selection"] = dict(spec["selection"], exit=dict(EXIT))
    with pytest.raises(ValueError, match="does not support"):
        validate_strategy_spec(spec)


def test_unknown_family_is_rejected():
    with pytest.raises(ValueError, match="family must be one of"):
        validate_strategy_spec(_v2(family="market_timing"))


def test_v2_hash_covers_the_family_and_the_exit():
    base = spec_hash(_v2())
    changed_exit = spec_hash(_v2(exit_policy=dict(EXIT, stop_loss_pct=0.2)))
    assert base != changed_exit
    allocation = spec_hash(
        _v2(family="asset_allocation", exit_policy={"stop_loss_pct": 0.15})
    )
    assert allocation != base


def test_v2_rejects_an_unknown_exit_key():
    """A typo must fail loudly, whichever validator catches it first."""
    with pytest.raises(ValueError, match="trailing_stop"):
        validate_strategy_spec(_v2(exit_policy=dict(EXIT, trailing_stop=0.05)))


def test_v2_rank_family_allows_score_weighting():
    """`sizing.weighting` is two-valued in v2, not a constant."""
    spec = _v2()
    spec["sizing"] = {"weighting": "score", "cash_reserve_pct": 0}
    assert validate_strategy_spec(spec)["sizing"]["weighting"] == "score"


def test_v1_still_allows_only_equal_weighting():
    spec = _v1()
    spec["sizing"] = {"weighting": "score", "cash_reserve_pct": 0}
    with pytest.raises(ValueError, match="sizing.weighting"):
        validate_strategy_spec(spec)


def test_two_minimum_holding_periods_are_rejected():
    spec = _v2()
    spec["rebalance"] = {"cadence_days": 5, "min_hold_sessions": 5}
    spec["selection"] = dict(spec["selection"], exit=dict(EXIT, min_hold_sessions=3))
    with pytest.raises(ValueError, match="declare min_hold_sessions once"):
        validate_strategy_spec(spec)


def test_allocation_has_no_legacy_projection():
    from caifubao_contracts.strategy_spec import to_legacy_config

    with pytest.raises(ValueError, match="no legacy config projection"):
        to_legacy_config(
            validate_strategy_spec(
                _v2(family="asset_allocation", exit_policy={"stop_loss_pct": 0.1})
            )
        )
