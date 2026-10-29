# -*- coding: utf-8 -*-
"""Accounting and benchmark declarations, and the criteria that retire a strategy.

Paper NAV marks raw prices, so a dividend is invisible and an ex-date looks like a
loss. A strategy also had no declared comparison set, so two documents could
report different "excess" numbers without either being wrong. And without kill
criteria a strategy is only ever observed, never retired.
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "shared") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "shared"))

from caifubao_contracts.strategy_accounting import (  # noqa: E402
    period_return,
    total_return,
    validate_accounting,
)
from caifubao_contracts.strategy_benchmark import (  # noqa: E402
    STATUS_BREACH,
    STATUS_INSUFFICIENT,
    STATUS_PASS,
    evaluate_kill_criteria,
    excess_return,
    validate_benchmark,
    validate_kill_criteria,
)
from caifubao_contracts.strategy_spec import spec_hash, validate_strategy_spec  # noqa: E402

CASH = {"corporate_actions": "ex_date_cash", "mark_price": "close_raw"}
PRICE = {"corporate_actions": "price_only", "mark_price": "close_raw"}
HFQ = {"corporate_actions": "price_only", "mark_price": "close_hfq"}
KILL = {
    "min_excess_per_period": 0.0,
    "max_drawdown": 0.2,
    "review_after_sessions": 60,
}


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
        "evidence": {
            "timing_version": "paper_causal_v1",
            "policy": "research",
            "kill_criteria": dict(KILL),
        },
        "initial_nav": 100000,
        "family": "asset_allocation",
        "allocation": {"assets": {"sh510300": 0.4}, "band_pct": 0.05},
        "accounting": dict(PRICE),
        "benchmark": {"kind": "equal_weight_universe"},
    }
    spec.update(overrides)
    if spec.get("family") == "asset_allocation":
        # The allocation family sizes by asset weights and declares only its
        # exit contract; rank-selection blocks are rejected by validation.
        spec["selection"] = {"exit": spec["selection"]["exit"]}
        spec.pop("sizing", None)
        spec.pop("constraints", None)
    return spec


# --------------------------------------------------------------------------
# accounting
# --------------------------------------------------------------------------


def test_cash_policy_credits_the_distribution():
    credited = period_return(
        policy=CASH, start_mark=10.0, end_mark=9.9, cash_dividends=0.2
    )
    bare = period_return(
        policy=PRICE, start_mark=10.0, end_mark=9.9, cash_dividends=0.2
    )
    assert credited == pytest.approx(0.01)
    assert bare == pytest.approx(-0.01)


def test_hfq_marks_already_contain_the_distribution():
    """Crediting cash on top of an adjusted series would double count."""
    with pytest.raises(ValueError, match="double counts"):
        validate_accounting(
            {"corporate_actions": "ex_date_cash", "mark_price": "close_hfq"}
        )
    assert period_return(policy=HFQ, start_mark=10.0, end_mark=10.2) == pytest.approx(
        0.02
    )


def test_accounting_rejects_unknown_values():
    with pytest.raises(ValueError, match="corporate_actions"):
        validate_accounting({"corporate_actions": "ignore", "mark_price": "close_raw"})
    with pytest.raises(ValueError, match="mark_price"):
        validate_accounting({"corporate_actions": "price_only", "mark_price": "vwap"})


def test_total_return_chains_periods():
    marks = [10.0, 10.1, 10.0]
    assert total_return(policy=PRICE, marks=marks) == pytest.approx(
        (10.0 / 10.0) - 1.0, abs=1e-12
    )
    with_dividend = total_return(policy=CASH, marks=marks, cash_dividends=[0.0, 0.1])
    assert with_dividend > total_return(policy=PRICE, marks=marks)


def test_total_return_needs_matching_dividends():
    with pytest.raises(ValueError, match="one value per period"):
        total_return(policy=CASH, marks=[1.0, 2.0, 3.0], cash_dividends=[0.1])


# --------------------------------------------------------------------------
# benchmark
# --------------------------------------------------------------------------


def test_benchmark_kinds_validate():
    assert validate_benchmark({"kind": "equal_weight_universe"})["kind"] == (
        "equal_weight_universe"
    )
    assert validate_benchmark({"kind": "broad_index", "index": "sh000300"})[
        "index"
    ] == ("sh000300")
    static = validate_benchmark(
        {"kind": "static_allocation", "assets": {"sh510300": 0.6, "sh511010": 0.4}}
    )
    assert static["assets"]["sh510300"] == 0.6


def test_static_benchmark_weights_must_sum_to_one():
    with pytest.raises(ValueError, match="sum to 1.0"):
        validate_benchmark(
            {"kind": "static_allocation", "assets": {"sh510300": 0.6, "sh511010": 0.3}}
        )


def test_benchmark_rejects_inapplicable_fields():
    with pytest.raises(ValueError, match="must not carry"):
        validate_benchmark({"kind": "equal_weight_universe", "index": "sh000300"})


def test_excess_return_is_arithmetic():
    assert excess_return(0.12, 0.08) == pytest.approx(0.04)


# --------------------------------------------------------------------------
# kill criteria
# --------------------------------------------------------------------------


def test_kill_criteria_require_all_three_thresholds():
    for field in ("min_excess_per_period", "max_drawdown", "review_after_sessions"):
        criteria = dict(KILL)
        criteria.pop(field)
        with pytest.raises(ValueError, match=field):
            validate_kill_criteria(criteria)


def test_short_history_is_not_a_pass():
    result = evaluate_kill_criteria(KILL, sessions=10, excess_per_period=-0.5)
    assert result["status"] == STATUS_INSUFFICIENT
    assert result["required_sessions"] == 60


def test_breach_recommends_retirement():
    result = evaluate_kill_criteria(
        KILL, sessions=80, excess_per_period=-0.1, max_drawdown=0.05
    )
    assert result["status"] == STATUS_BREACH
    assert "excess" in result["breaches"]
    assert result["recommendation"] == "retire"


def test_drawdown_breach_is_reported():
    result = evaluate_kill_criteria(
        KILL, sessions=80, excess_per_period=0.01, max_drawdown=-0.35
    )
    assert result["status"] == STATUS_BREACH
    assert "drawdown" in result["breaches"]


def test_clean_history_passes():
    result = evaluate_kill_criteria(
        KILL, sessions=80, excess_per_period=0.01, max_drawdown=0.1
    )
    assert result["status"] == STATUS_PASS


# --------------------------------------------------------------------------
# declaration level
# --------------------------------------------------------------------------


def test_v2_requires_accounting():
    spec = _spec()
    spec.pop("accounting")
    with pytest.raises(ValueError, match="requires accounting"):
        validate_strategy_spec(spec)


def test_v2_requires_benchmark():
    spec = _spec()
    spec.pop("benchmark")
    with pytest.raises(ValueError, match="requires benchmark"):
        validate_strategy_spec(spec)


def test_v2_requires_kill_criteria():
    spec = _spec()
    spec["evidence"] = {"timing_version": "paper_causal_v1", "policy": "research"}
    with pytest.raises(ValueError, match="kill_criteria"):
        validate_strategy_spec(spec)


def test_v2_hash_covers_accounting_and_benchmark():
    base = spec_hash(_spec())
    assert spec_hash(_spec(accounting=dict(CASH))) != base
    assert spec_hash(_spec(accounting=dict(HFQ))) != base
    assert (
        spec_hash(_spec(benchmark={"kind": "broad_index", "index": "sh000300"})) != base
    )


def _v1_from_v2(spec):
    """Strip every v2-only block so the remainder is a legal v1 declaration."""
    base = dict(spec)
    base["spec_version"] = "1"
    base.pop("accounting", None)
    base.pop("benchmark", None)
    base.pop("allocation", None)
    base["family"] = "rank_selection"
    base["selection"] = {
        "mode": "top_n",
        "portfolio_size": 3,
        "exit": {
            "rank_below": 10,
            "stop_loss_pct": 0.1,
            "max_hold_sessions": 20,
            "on_ineligible": "sell_next_open",
        },
    }
    return base


def test_v1_rejects_accounting():
    spec = _v1_from_v2(_spec(family="rank_selection"))
    spec["selection"] = {"mode": "top_n", "portfolio_size": 3}
    spec["accounting"] = dict(PRICE)
    with pytest.raises(ValueError, match="does not support"):
        validate_strategy_spec(spec)


def test_v1_rejects_benchmark():
    spec = _v1_from_v2(_spec(family="rank_selection"))
    spec["selection"] = {"mode": "top_n", "portfolio_size": 3}
    spec["benchmark"] = {"kind": "equal_weight_universe"}
    with pytest.raises(ValueError, match="does not support"):
        validate_strategy_spec(spec)


def test_v1_rejects_kill_criteria():
    spec = _v1_from_v2(_spec(family="rank_selection"))
    spec["selection"] = {"mode": "top_n", "portfolio_size": 3}
    spec["evidence"] = {
        "timing_version": "paper_causal_v1",
        "policy": "research",
        "kill_criteria": dict(KILL),
    }
    with pytest.raises(ValueError, match="does not support"):
        validate_strategy_spec(spec)


def test_on_breach_action_is_validated_and_used():
    """The retire/review choice is read, not decorative."""
    from caifubao_contracts.strategy_benchmark import (
        ON_BREACH_REVIEW,
        evaluate_kill_criteria,
    )

    criteria = dict(KILL, on_breach=ON_BREACH_REVIEW)
    result = evaluate_kill_criteria(
        criteria, sessions=80, excess_per_period=-0.1, max_drawdown=0.05
    )
    assert result["status"] == STATUS_BREACH
    assert result["recommendation"] == ON_BREACH_REVIEW
    with pytest.raises(ValueError, match="on_breach"):
        validate_kill_criteria(dict(KILL, on_breach="ignore"))


def test_horizon_claim_rationale_is_carried():
    """A rationale is optional but must survive validation when supplied."""
    from caifubao_contracts.strategy_sleeve import validate_horizon_claim

    claim = validate_horizon_claim(
        {
            "hypothesis_horizon": 60,
            "evidence_horizon": 60,
            "rationale": "trend claims need a long horizon",
        }
    )
    assert claim["rationale"] == "trend claims need a long horizon"
    with pytest.raises(ValueError, match="rationale"):
        validate_horizon_claim(
            {"hypothesis_horizon": 60, "evidence_horizon": 60, "rationale": 5}
        )
