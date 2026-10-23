# -*- coding: utf-8 -*-
"""S0 golden/contract tests for a shared StrategySpec and legacy bridge."""

import copy

import pytest

from app.lib.strategy_engine.config import (
    DEFAULT_STRATEGY_CONFIG,
    strategy_config_hash,
    validate_strategy_config,
)
from app.lib.strategy_engine.spec import (
    legacy_to_spec,
    spec_hash,
    spec_to_legacy_config,
    validate_strategy_spec,
)


def test_legacy_migration_preserves_identity_and_behavior_fields():
    legacy = validate_strategy_config(DEFAULT_STRATEGY_CONFIG)
    migrated = legacy_to_spec(legacy)
    assert migrated["legacy_config_hash"] == strategy_config_hash(legacy)
    assert migrated["spec_hash"] == spec_hash(migrated["spec"])
    assert migrated["spec_hash"] != migrated["legacy_config_hash"]
    config = spec_to_legacy_config(migrated["spec"])
    for key in (
        "selection",
        "constraints",
        "rebalance",
        "weighting",
        "cash_reserve_pct",
        "score_model_version",
        "horizon",
        "initial_nav",
        "timing_version",
    ):
        assert config[key] == legacy[key]


def test_spec_hash_order_insensitive_and_no_mutation():
    original = legacy_to_spec(DEFAULT_STRATEGY_CONFIG)["spec"]
    copied = copy.deepcopy(original)
    copied["risk"] = dict(reversed(list(copied["risk"].items())))
    assert spec_hash(original) == spec_hash(copied)
    assert original == legacy_to_spec(DEFAULT_STRATEGY_CONFIG)["spec"]


@pytest.mark.parametrize(
    "selection",
    [
        {"mode": "top_n", "portfolio_size": 2},
        {
            "mode": "top_percentile",
            "lower": 0.2,
            "upper": 1.0,
            "portfolio_size": 3,
        },
    ],
)
def test_migrated_spec_preserves_target_and_rebalance_golden(selection):
    from types import SimpleNamespace

    from app.lib.strategy_engine.selection import (
        compute_rebalance,
        select_target_holdings,
    )

    legacy = validate_strategy_config(
        {"score_model_version": "test-v1", "selection": selection}
    )
    migrated = legacy_to_spec(legacy)
    restored = spec_to_legacy_config(migrated["spec"])
    predictions = [
        SimpleNamespace(stock_code="sh600001", score=3, percentile=0.9),
        SimpleNamespace(stock_code="sh600002", score=2, percentile=0.6),
        SimpleNamespace(stock_code="sh600003", score=1, percentile=0.1),
    ]
    previous = [{"stock_code": "sh600003", "weight": 1.0}]
    target_old = select_target_holdings(predictions, legacy)
    target_new = select_target_holdings(predictions, restored)
    assert target_old == target_new
    assert compute_rebalance(previous, target_old) == compute_rebalance(
        previous, target_new
    )
    assert migrated["legacy_config_hash"] == strategy_config_hash(legacy)


def test_fee_change_changes_hash():
    original = legacy_to_spec(DEFAULT_STRATEGY_CONFIG)["spec"]
    updated = copy.deepcopy(original)
    updated["execution"]["commission_rate"] = 0.00030
    assert spec_hash(original) != spec_hash(updated)


def test_strict_nested_unknown_key_rejected():
    spec = legacy_to_spec(DEFAULT_STRATEGY_CONFIG)["spec"]
    spec["risk"]["turnover_buget_pct"] = 0.1
    with pytest.raises(ValueError, match="unknown risk"):
        validate_strategy_spec(spec)


def test_replay_compat_requires_separate_policy_and_legacy_refuses():
    spec = legacy_to_spec(DEFAULT_STRATEGY_CONFIG)["spec"]
    spec["execution"]["unknown_limit_policy"] = "replay_compat"
    validate_strategy_spec(spec)
    with pytest.raises(ValueError, match="replay compatibility"):
        spec_to_legacy_config(spec)


def test_non_default_risk_cannot_silently_run_on_legacy():
    spec = legacy_to_spec(DEFAULT_STRATEGY_CONFIG)["spec"]
    spec["risk"]["max_positions"] = 10
    with pytest.raises(ValueError, match="non-default risk"):
        spec_to_legacy_config(spec)


def test_spec_rejects_nan_and_unsupported_sizing():
    spec = legacy_to_spec(DEFAULT_STRATEGY_CONFIG)["spec"]
    spec["execution"]["slippage_per_side"] = float("nan")
    with pytest.raises(ValueError, match="finite"):
        validate_strategy_spec(spec)
    spec = legacy_to_spec(DEFAULT_STRATEGY_CONFIG)["spec"]
    spec["sizing"]["weighting"] = "risk_parity"
    with pytest.raises(ValueError, match="equal"):
        validate_strategy_spec(spec)


def test_top_n_migration_does_not_reintroduce_bounds():
    legacy = validate_strategy_config(
        {
            "score_model_version": "v1",
            "selection": {"mode": "top_n", "portfolio_size": 3},
        }
    )
    spec = legacy_to_spec(legacy)["spec"]
    assert "lower" not in spec["selection"]
    assert "upper" not in spec["selection"]
    assert spec_to_legacy_config(spec)["selection"] == legacy["selection"]


def test_cli_accepts_spec_and_legacy_config_exclusively():
    from app.jobs.strategy_runner import build_parser

    parser = build_parser()
    assert (
        parser.parse_args(
            ["run", "--date", "2026-10-23", "--spec-json", "{}"]
        ).spec_json
        == "{}"
    )
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "run",
                "--date",
                "2026-10-23",
                "--config-json",
                "{}",
                "--spec-json",
                "{}",
            ]
        )


def test_forward_hash_kind_mismatch_is_replay_even_if_hash_bytes_match():
    import datetime
    from types import SimpleNamespace

    from app.lib.strategy_engine.runner import classify_evidence_kind

    window = SimpleNamespace(
        status="ACTIVE",
        start_date=datetime.datetime.fromisoformat("2026-10-23"),
        config_hash="same-bytes",
        hash_kind="legacy",
    )
    kind = classify_evidence_kind(
        date=datetime.datetime.fromisoformat("2026-10-23"),
        decision_at=datetime.datetime.fromisoformat("2026-10-23T07:00:00+00:00"),
        execution_date=datetime.datetime.fromisoformat("2026-10-26"),
        window=window,
        config_hash="same-bytes",
        hash_kind="spec",
        existing_status=None,
        replace=False,
    )
    assert kind == "REPLAY"


def test_feasibility_preflight_fails_closed_on_cash_and_board_minimum():
    from caifubao_contracts.strategy_spec import preflight_execution_feasibility

    spec = legacy_to_spec(DEFAULT_STRATEGY_CONFIG)["spec"]
    spec["selection"]["portfolio_size"] = 4
    spec["initial_nav"] = 20_000
    result = preflight_execution_feasibility(
        spec, reference_prices={"sh600000": 10, "sh688001": 10}
    )
    assert result["checked_names"] == 2
    assert result["details"][0]["board_minimum"] == 100
    assert result["details"][1]["board_minimum"] == 200
    assert result["per_name_budget_cny"] == 5000
    with pytest.raises(ValueError, match="board minimum"):
        preflight_execution_feasibility(
            spec,
            reference_prices={"sh600000": 10},
            target_weights={"sh600000": 0.04},
        )
    with pytest.raises(ValueError, match="board minimum"):
        preflight_execution_feasibility(spec, reference_prices={"sh688001": 40})
    with pytest.raises(ValueError, match="uneconomic"):
        preflight_execution_feasibility(
            spec,
            reference_prices={"sh600001": 5},
            max_minimum_commission_pct=0.005,
        )
    with pytest.raises(ValueError, match="decision-time quote map"):
        preflight_execution_feasibility(spec, reference_prices={})


def test_spec_risk_policy_exposure_regime_is_hashed_and_validated():
    spec = legacy_to_spec(DEFAULT_STRATEGY_CONFIG)["spec"]
    first = spec_hash(spec)
    spec["risk"]["max_gross_exposure_pct"] = 0.7
    spec["risk"]["max_single_stock_pct"] = 0.1
    spec["risk"]["regime_policy"] = "bear_reduce_only"
    assert spec_hash(spec) != first
    assert validate_strategy_spec(spec)["risk"]["regime_policy"] == "bear_reduce_only"
    with pytest.raises(ValueError, match="non-default risk"):
        spec_to_legacy_config(spec)


def test_versioned_strategy_spec_has_frozen_hash_golden_vector():
    # Pinned independently of the implementation and of legacy config hashes.
    from caifubao_contracts.strategy_spec import DEFAULT_EXECUTION, DEFAULT_RISK

    example = {
        "spec_version": "1",
        "universe": {"market": "CN_A", "as_of": "PIT"},
        "signal": {"score_model_version": "golden-v1", "horizon": 20},
        "selection": {"mode": "top_n", "portfolio_size": 2},
        "sizing": {"weighting": "equal", "cash_reserve_pct": 0.0},
        "constraints": {},
        "rebalance": {"cadence_days": 5},
        "risk": dict(DEFAULT_RISK),
        "execution": dict(DEFAULT_EXECUTION),
        "evidence": {"timing_version": "paper_causal_v1", "policy": "research"},
        "initial_nav": 10000.0,
    }
    assert spec_hash(example) == (
        "7fb19ffc0ca190cdf3fcd2eaf94df3b0e40c99eb6a578a7a9d1a39d9320ec8ba"
    )


def test_replay_and_paper_decisions_share_exact_spec_semantics():
    import datetime
    from types import SimpleNamespace

    from app.lib.strategy_engine.replay import replay_strategy_spec_decision
    from app.lib.strategy_engine.runner import assemble_daily_plan

    legacy = validate_strategy_config(
        {
            "score_model_version": "frozen-decision-v1",
            "selection": {"mode": "top_n", "portfolio_size": 2},
        }
    )
    spec = legacy_to_spec(legacy)["spec"]
    date = datetime.datetime(2026, 10, 23)
    scores = [
        SimpleNamespace(stock_code=code, score=score, percentile=0.9)
        for code, score in [
            ("sh600000", 0.8),
            ("sh600001", 0.9),
            ("sh600002", 0.7),
        ]
    ]
    flags = {
        code: {"is_st": False, "is_bse": False, "trade_status": 1}
        for code in ("sh600000", "sh600001", "sh600002")
    }
    previous = [{"stock_code": "sh600002", "weight": 0.5}]
    paper = assemble_daily_plan(
        config=legacy,
        date=date,
        predictions=scores,
        previous_holdings=previous,
        flags=flags,
    )
    replay = replay_strategy_spec_decision(
        spec=spec,
        date=date,
        predictions=scores,
        previous_holdings=previous,
        flags=flags,
    )
    assert replay == paper
    assert [x["stock_code"] for x in replay["target_holdings"]] == [
        "sh600001",
        "sh600000",
    ]
    assert replay["rebalance"]["removed"] == ["sh600002"]
