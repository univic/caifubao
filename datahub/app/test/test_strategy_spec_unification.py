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
