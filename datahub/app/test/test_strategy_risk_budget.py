# -*- coding: utf-8 -*-
"""The risk budget and score weighting actually run.

Every field exercised here was validated, hashed and documented while nobody read
it — the same defect as ``rebalance.cadence_days``, repeated seven times. These
tests are the proof that the ``risk`` block and ``sizing.weighting`` now change an
outcome.
"""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "shared") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "shared"))

from caifubao_contracts.strategy_risk_budget import (  # noqa: E402
    BEAR_REDUCTION,
    check_budget,
    validate_risk_budget,
)
from app.lib.strategy_engine.selection import plan_target_holdings  # noqa: E402

FULL = {
    "max_turnover_pct": 0.10,
    "max_positions": 20,
    "cash_floor_pct": 0.05,
    "max_drawdown_pct": 0.25,
    "max_single_stock_pct": 0.10,
    "max_gross_exposure_pct": 0.95,
    "regime_policy": "none",
}


def _check(**overrides):
    args = {
        "risk": dict(FULL),
        "position_count": 20,
        "gross_exposure_pct": 0.95,
        "cash_pct": 0.05,
        "turnover_pct": 0.05,
        "drawdown_pct": 0.05,
        "largest_weight_pct": 0.05,
        "regime": "sideways",
    }
    args.update(overrides)
    return check_budget(**args)


def _config(size=20, weighting="equal", constraints=None):
    """Legacy config shape: weighting and cash reserve are top-level keys."""
    config = {
        "score_model_version": "flip_wide_shadow_v1",
        "horizon": 20,
        "selection": {"mode": "top_n", "portfolio_size": size},
        "cash_reserve_pct": 0,
        "constraints": dict(constraints or {}),
    }
    if weighting is not None:
        config["weighting"] = weighting
    return config


def _predictions(count=20, percentile=0.9):
    return [
        SimpleNamespace(
            stock_code=f"sh60{index:04d}",
            score=float(count - index),
            percentile=percentile if percentile is not None else 0.9,
        )
        for index in range(count)
    ]


# --------------------------------------------------------------------------
# budget contract
# --------------------------------------------------------------------------


def test_budget_validates_and_defaults_regime():
    validated = validate_risk_budget({"cash_floor_pct": 0.05})
    assert validated["regime_policy"] == "none"
    assert validated["max_positions"] is None


def test_within_budget_is_admissible():
    result = _check()
    assert result["admissible"] is True
    assert result["scale"] == 1.0


def test_cash_floor_breach_is_a_stop_not_a_scale():
    result = _check(cash_pct=0.01)
    assert result["admissible"] is False
    assert "cash_floor_pct" in result["stops"]
    assert result["scale"] == 0.0


def test_drawdown_breach_is_a_stop():
    result = _check(drawdown_pct=-0.40)
    assert result["admissible"] is False
    assert "max_drawdown_pct" in result["stops"]


def test_position_count_truncates_instead_of_scaling():
    """Shrinking every weight leaves the count unchanged; names must be dropped."""
    result = _check(position_count=40)
    assert result["admissible"] is True
    assert "max_positions" in result["violations"]
    assert result["truncate_to"] == 20
    assert result["scale"] == 1.0


def test_gross_exposure_scales_the_book():
    result = _check(gross_exposure_pct=1.0, largest_weight_pct=0.2)
    # 1.0 > 0.95 cap -> scale to 0.95; 0.2 > 0.10 single cap -> scale to 0.5
    assert result["scale"] == pytest.approx(0.5)


def test_single_name_cap_scales_the_book():
    result = _check(largest_weight_pct=0.2)
    assert "max_single_stock_pct" in result["violations"]
    assert result["scale"] == pytest.approx(0.5)


def test_turnover_breach_refuses_rather_than_trims():
    result = _check(turnover_pct=0.5)
    assert result["admissible"] is False
    assert "max_turnover_pct" in result["violations"]


def test_bear_regime_reduces_exposure():
    result = _check(regime="bear", risk=dict(FULL, regime_policy="bear_reduce_only"))
    assert result["admissible"] is True
    assert result["scale"] == pytest.approx(BEAR_REDUCTION)


def test_bull_only_policy_stops_in_a_bear_market():
    result = _check(regime="bear", risk=dict(FULL, regime_policy="bull_only"))
    assert result["admissible"] is False
    assert "regime_bull_only" in result["stops"]


# --------------------------------------------------------------------------
# weighting
# --------------------------------------------------------------------------


def test_default_and_explicit_equal_agree():
    default = plan_target_holdings(_predictions(), _config(), weighting="equal")
    historical = plan_target_holdings(_predictions(), _config())
    assert default == historical


def test_score_weighting_is_proportional_to_percentile():
    predictions = [
        SimpleNamespace(stock_code="sh600000", score=2.0, percentile=0.9),
        SimpleNamespace(stock_code="sh600001", score=1.0, percentile=0.3),
    ]
    plan = plan_target_holdings(
        predictions,
        _config(size=2, constraints={"max_single_stock_pct": 0.9}),
        weighting="score",
    )
    weights = {h["stock_code"]: h["weight"] for h in plan["holdings"]}
    assert weights["sh600000"] > weights["sh600001"]
    assert sum(weights.values()) == pytest.approx(1.0, abs=1e-6)


def test_score_weighting_clips_at_the_single_cap():
    predictions = [
        SimpleNamespace(stock_code="sh600000", score=2.0, percentile=0.99),
        SimpleNamespace(stock_code="sh600001", score=1.0, percentile=0.01),
    ]
    plan = plan_target_holdings(
        predictions,
        _config(size=2, constraints={"max_single_stock_pct": 0.3}),
        weighting="score",
    )
    weights = {h["stock_code"]: h["weight"] for h in plan["holdings"]}
    assert weights["sh600000"] == pytest.approx(0.3)
    assert sum(weights.values()) < 1.0  # clipped remainder stays in cash


def test_score_weighting_refuses_an_industry_cap():
    with pytest.raises(ValueError, match="max_industry_pct"):
        plan_target_holdings(
            _predictions(),
            _config(constraints={"max_industry_pct": 0.3}),
            weighting="score",
            industry_by_code={p.stock_code: "J66" for p in _predictions()},
        )


def test_unknown_weighting_is_rejected():
    with pytest.raises(ValueError, match="weighting must be"):
        plan_target_holdings(_predictions(), _config(), weighting="vol_target")
