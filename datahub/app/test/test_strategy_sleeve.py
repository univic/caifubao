# -*- coding: utf-8 -*-
"""Sleeve isolation and horizon discipline.

A high-variance sleeve is only distinguishable from a leveraged bet on the whole
account if its loss can be trapped inside its own share. And a hypothesis stated
at one horizon cannot be supported by evidence produced at another — this
repository has already conflated a scan that only varied holding period with a
claim about a different signal.
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "shared") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "shared"))

from caifubao_contracts.strategy_sleeve import (  # noqa: E402
    allocate_sleeves,
    apply_losses,
    validate_horizon_claim,
    validate_sleeve,
    validate_sleeves,
)
from caifubao_contracts.strategy_spec import validate_strategy_spec  # noqa: E402

STABLE = {
    "name": "stable",
    "capital_share": 0.5,
    "accounting_id": "acc-stable",
    "evidence_id": "ev-stable",
}
AGGRESSIVE = {
    "name": "aggressive",
    "capital_share": 0.1,
    "accounting_id": "acc-aggressive",
    "evidence_id": "ev-aggressive",
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
            "kill_criteria": {
                "min_excess_per_period": 0.0,
                "max_drawdown": 0.2,
                "review_after_sessions": 60,
            },
        },
        "initial_nav": 100000,
        "family": "asset_allocation",
        "allocation": {"assets": {"sh510300": 0.4}, "band_pct": 0.05},
        "accounting": {"corporate_actions": "price_only", "mark_price": "close_raw"},
        "benchmark": {"kind": "equal_weight_universe"},
        "sleeves": [dict(STABLE), dict(AGGRESSIVE)],
        "horizon_claim": {"hypothesis_horizon": 60, "evidence_horizon": 60},
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
# sleeves
# --------------------------------------------------------------------------


def test_sleeve_defaults_to_isolation():
    validated = validate_sleeve(STABLE)
    assert validated["on_shortfall"] == "isolate"
    assert validated["capital_share"] == 0.5


def test_sleeve_shares_cannot_exceed_capital():
    third = {
        "name": "third",
        "capital_share": 0.5,
        "accounting_id": "acc-third",
        "evidence_id": "ev-third",
    }
    # 0.5 + 0.1 + 0.5 = 1.1 > 1.0
    with pytest.raises(ValueError, match="must not exceed 1.0"):
        validate_sleeves([dict(STABLE), dict(AGGRESSIVE), third])


def test_sleeve_identities_must_be_unique():
    with pytest.raises(ValueError, match="accounting_id"):
        validate_sleeves([dict(STABLE), dict(AGGRESSIVE, accounting_id="acc-stable")])


def test_allocation_leaves_a_remainder():
    result = allocate_sleeves([dict(STABLE), dict(AGGRESSIVE)], 100_000)
    assert result["sleeves"] == {"stable": 50_000, "aggressive": 10_000}
    assert result["unallocated"] == 40_000


def test_a_sleeve_cannot_consume_another_allocation():
    result = apply_losses([dict(STABLE), dict(AGGRESSIVE)], {"aggressive": 0.25})
    assert result["applied"]["aggressive"] == 0.1
    assert result["applied"]["stable"] == 0.0
    assert result["breached"] is True
    assert result["isolated"] is True
    assert result["shortfall"][0]["excess"] == pytest.approx(0.15)


def test_within_limit_loss_is_applied_as_is():
    result = apply_losses([dict(STABLE), dict(AGGRESSIVE)], {"aggressive": 0.05})
    assert result["applied"]["aggressive"] == pytest.approx(0.05)
    assert result["breached"] is False


def test_unknown_sleeve_loss_is_rejected():
    with pytest.raises(ValueError, match="unknown sleeves"):
        apply_losses([dict(STABLE)], {"ghost": 0.1})


# --------------------------------------------------------------------------
# horizon
# --------------------------------------------------------------------------


def test_horizon_claim_requires_matching_evidence():
    assert (
        validate_horizon_claim({"hypothesis_horizon": 60, "evidence_horizon": 60})[
            "hypothesis_horizon"
        ]
        == 60
    )
    with pytest.raises(ValueError, match="cannot support a claim"):
        validate_horizon_claim({"hypothesis_horizon": 60, "evidence_horizon": 20})


def test_horizon_bounds():
    for bad in (0, 251, "60"):
        with pytest.raises(ValueError, match="hypothesis_horizon"):
            validate_horizon_claim({"hypothesis_horizon": bad, "evidence_horizon": 60})


# --------------------------------------------------------------------------
# declaration level
# --------------------------------------------------------------------------


def test_v2_accepts_sleeves_and_horizon_claim():
    validated = validate_strategy_spec(_spec())
    assert [sleeve["name"] for sleeve in validated["sleeves"]] == [
        "stable",
        "aggressive",
    ]
    assert validated["horizon_claim"]["evidence_horizon"] == 60


def test_v2_rejects_a_mismatched_horizon():
    with pytest.raises(ValueError, match="cannot support a claim"):
        validate_strategy_spec(
            _spec(horizon_claim={"hypothesis_horizon": 60, "evidence_horizon": 20})
        )


def test_v2_rejects_oversubscribed_sleeves():
    with pytest.raises(ValueError, match="must not exceed 1.0"):
        validate_strategy_spec(
            _spec(
                sleeves=[
                    dict(STABLE, name="a", accounting_id="a", evidence_id="a"),
                    dict(STABLE, name="b", accounting_id="b", evidence_id="b"),
                    dict(STABLE, name="c", accounting_id="c", evidence_id="c"),
                ]
            )
        )


def test_sleeves_are_optional():
    spec = _spec()
    spec.pop("sleeves")
    spec.pop("horizon_claim")
    validated = validate_strategy_spec(spec)
    assert "sleeves" not in validated


def test_v1_rejects_sleeves():
    spec = _spec(family="rank_selection")
    spec["spec_version"] = "1"
    for field in ("accounting", "benchmark", "allocation", "sleeves", "horizon_claim"):
        spec.pop(field, None)
    spec["family"] = "rank_selection"
    spec["selection"] = {"mode": "top_n", "portfolio_size": 3}
    spec["sleeves"] = [dict(STABLE)]
    with pytest.raises(ValueError, match="does not support"):
        validate_strategy_spec(spec)
