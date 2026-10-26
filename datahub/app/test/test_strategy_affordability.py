"""Decision-session affordability is opt-in, identity-bearing and pre-selection."""

import copy
import datetime
from types import SimpleNamespace

import pytest

from app.lib.strategy_engine.config import validate_strategy_config
from app.lib.strategy_engine.runner import assemble_daily_plan
from app.lib.strategy_engine.spec import (
    legacy_to_spec,
    spec_hash,
    spec_to_legacy_config,
    validate_strategy_spec,
)
from caifubao_contracts.strategy_spec import (
    OpeningTradeInfeasible,
    preflight_execution_feasibility,
)


def _config(enabled=True):
    constraints = {"max_single_stock_pct": 0.05}
    if enabled is not None:
        constraints["exclude_infeasible_buys"] = enabled
    return validate_strategy_config(
        {
            "score_model_version": "test-v1",
            "horizon": 20,
            "initial_nav": 100000,
            "selection": {"mode": "top_n", "portfolio_size": 3},
            "constraints": constraints,
        }
    )


def _plan(config, prices):
    return assemble_daily_plan(
        config=config,
        date=datetime.datetime(2026, 4, 13),
        predictions=[
            SimpleNamespace(stock_code=code, score=100 - i, percentile=0.9)
            for i, code in enumerate(prices)
        ],
        previous_holdings=None,
        flags={
            code: {
                "is_st": False,
                "is_bse": False,
                "trade_status": 1,
                "reference_close": price,
            }
            for code, price in prices.items()
        },
    )


def test_unaffordable_star_is_replaced_before_truncation():
    plan = _plan(
        _config(), {"sh688213": 108.8, "sh600001": 20, "sh600002": 30, "sh600003": 40}
    )
    assert [h["stock_code"] for h in plan["target_holdings"]] == [
        "sh600001",
        "sh600002",
        "sh600003",
    ]
    assert all(h["weight"] == 0.05 for h in plan["target_holdings"])
    assert plan["eligibility"]["reason_counts"] == {"board_minimum_over_budget": 1}
    assert plan["eligibility"]["price_date"] == "2026-04-13"


@pytest.mark.parametrize("price", [None, 0, -1, True, float("nan"), float("inf")])
def test_invalid_signal_close_is_never_replaced_with_a_future_price(price):
    plan = _plan(_config(), {"sh600001": price, "sh600002": 20})
    assert [h["stock_code"] for h in plan["target_holdings"]] == ["sh600002"]
    assert plan["eligibility"]["reason_counts"] == {"invalid_signal_close": 1}


def test_minimum_commission_ratio_uses_same_preflight():
    plan = _plan(_config(), {"sh600001": 1, "sh600002": 20})
    assert plan["eligibility"]["reason_counts"] == {"minimum_commission_ratio": 1}
    assert plan["target_holdings"] == [{"stock_code": "sh600002", "weight": 0.05}]


def test_disabled_policy_preserves_selection_and_omits_diagnostics():
    plan = _plan(_config(None), {"sh688213": 108.8, "sh600001": 20})
    assert plan["target_holdings"][0]["stock_code"] == "sh688213"
    assert "eligibility" not in plan


def test_enabled_policy_requires_flags_even_with_other_exclusions_disabled():
    config = _config()
    config["constraints"].update(
        exclude_st=False, exclude_bse=False, exclude_suspended=False
    )
    with pytest.raises(ValueError, match="universe flag map"):
        assemble_daily_plan(
            config=config,
            date=datetime.datetime(2026, 4, 13),
            predictions=[SimpleNamespace(stock_code="sh600001")],
            previous_holdings=None,
        )


def test_new_policy_round_trip_and_hash_compatibility():
    old = legacy_to_spec(_config(None))["spec"]
    before = spec_hash(old)
    assert "exclude_infeasible_buys" not in validate_strategy_spec(old)["constraints"]
    assert spec_hash(legacy_to_spec(spec_to_legacy_config(old))["spec"]) == before
    enabled = copy.deepcopy(old)
    enabled["constraints"]["exclude_infeasible_buys"] = True
    assert spec_hash(enabled) != before
    assert spec_hash(
        legacy_to_spec(spec_to_legacy_config(enabled))["spec"]
    ) == spec_hash(enabled)
    assert "exclude_infeasible_buys" not in old["constraints"]


def test_historical_normalized_hash_is_unchanged():
    spec = {
        "spec_version": "1",
        "universe": {"market": "CN_A", "as_of": "PIT"},
        "signal": {"score_model_version": "test-v1", "horizon": 20},
        "selection": {"mode": "top_n", "portfolio_size": 20},
        "sizing": {"weighting": "equal", "cash_reserve_pct": 0},
        "constraints": {"max_single_stock_pct": 0.05},
        "rebalance": {"cadence_days": 5},
        "risk": {},
        "execution": {},
        "evidence": {"timing_version": "paper_causal_v1", "policy": "research"},
        "initial_nav": 100000,
    }
    assert (
        spec_hash(spec)
        == "e6a00032bf5f80a2ffc1e5d54044ff49cbe73cf27fef369fb0de3a1c9160ff20"
    )


@pytest.mark.parametrize("bad", [1, "true", None])
def test_policy_rejects_non_boolean_in_both_contracts(bad):
    config = _config(None)
    spec = legacy_to_spec(config)["spec"]
    spec["constraints"]["exclude_infeasible_buys"] = bad
    with pytest.raises(ValueError, match="boolean"):
        validate_strategy_spec(spec)
    config["constraints"]["exclude_infeasible_buys"] = bad
    with pytest.raises(ValueError, match="bool"):
        validate_strategy_config(config)


def test_final_smaller_target_weight_still_fails_preflight():
    spec = legacy_to_spec(_config())["spec"]
    with pytest.raises(OpeningTradeInfeasible, match="board minimum"):
        preflight_execution_feasibility(
            spec, reference_prices={"sh600001": 20}, target_weights={"sh600001": 0.01}
        )


def test_invalid_configuration_is_not_swallowed_as_an_excluded_stock(monkeypatch):
    import caifubao_contracts.strategy_spec as contract

    def invalid(*args, **kwargs):
        raise ValueError("invalid execution configuration")

    monkeypatch.setattr(contract, "preflight_execution_feasibility", invalid)
    with pytest.raises(ValueError, match="invalid execution configuration"):
        _plan(_config(), {"sh600001": 20})
