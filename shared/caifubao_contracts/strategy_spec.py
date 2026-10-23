# -*- coding: utf-8 -*-
"""Immutable strategy declarations and deterministic legacy migration.

This module is a *contract*: it must not import datahub, Mongo, a broker, or
any runtime-specific strategy implementation.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math

from caifubao_contracts.friction import DEFAULT_COST_PROFILE

SPEC_VERSION = "1"
DEFAULT_RISK = {
    "max_turnover_pct": None,
    "max_positions": None,
    "cash_floor_pct": 0.0,
    "max_drawdown_pct": None,
    "regime_policy": "none",
}
DEFAULT_EXECUTION = {
    "commission_rate": DEFAULT_COST_PROFILE.commission_rate,
    "minimum_commission_cny": DEFAULT_COST_PROFILE.minimum_commission_cny,
    "sell_stamp_duty_rate": DEFAULT_COST_PROFILE.sell_stamp_duty_rate,
    "transfer_fee_rate": DEFAULT_COST_PROFILE.transfer_fee_rate,
    "slippage_per_side": DEFAULT_COST_PROFILE.slippage_per_side,
    "board_lot": DEFAULT_COST_PROFILE.board_lot,
    "unknown_limit_policy": "fail_closed",
}
_ALLOWED = {
    "": {
        "spec_version",
        "universe",
        "signal",
        "selection",
        "sizing",
        "constraints",
        "rebalance",
        "risk",
        "execution",
        "evidence",
        "initial_nav",
    },
    "universe": {"market", "as_of"},
    "signal": {"score_model_version", "horizon"},
    "selection": {"mode", "lower", "upper", "portfolio_size"},
    "sizing": {"weighting", "cash_reserve_pct"},
    "constraints": {
        "exclude_st",
        "exclude_bse",
        "exclude_suspended",
        "max_single_stock_pct",
        "max_industry_pct",
        "min_trade_amount_cny",
    },
    "rebalance": {"cadence_days"},
    "risk": set(DEFAULT_RISK),
    "execution": set(DEFAULT_EXECUTION),
    "evidence": {"timing_version", "policy"},
}


def _finite_number(value, path: str, *, low: float = 0.0, high: float | None = None):
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) < low
        or (high is not None and float(value) > high)
    ):
        raise ValueError(f"{path} must be a finite number in [{low}, {high}]")
    return float(value)


def _object(value, path: str) -> dict:
    if not isinstance(value, dict):
        raise ValueError(f"{path or 'StrategySpec'} must be an object")
    unknown = set(value) - _ALLOWED[path]
    if unknown:
        raise ValueError(f"unknown {path or 'StrategySpec'} fields: {sorted(unknown)}")
    return value


def _required(value: dict, field: str, path: str):
    if field not in value:
        raise ValueError(f"{path}.{field} is required")
    return value[field]


def canonical_json(value) -> str:
    """Stable hash input; refuse NaN/Inf rather than hashing invalid JSON."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def spec_hash(spec: dict) -> str:
    return hashlib.sha256(
        canonical_json(validate_strategy_spec(spec)).encode("utf-8")
    ).hexdigest()


def validate_strategy_spec(spec: dict) -> dict:
    """Return a deep copy with explicit defaults; reject unsupported semantics."""
    data = _object(spec, "")
    normalized = copy.deepcopy(data)
    if normalized.get("spec_version") != SPEC_VERSION:
        raise ValueError(f"spec_version must be {SPEC_VERSION}")
    for part in (
        "universe",
        "signal",
        "selection",
        "sizing",
        "constraints",
        "rebalance",
        "risk",
        "execution",
        "evidence",
    ):
        _object(_required(normalized, part, "spec"), part)

    universe = normalized["universe"]
    if universe != {"market": "CN_A", "as_of": "PIT"}:
        raise ValueError("universe requires market=CN_A and as_of=PIT")
    signal = normalized["signal"]
    name = _required(signal, "score_model_version", "signal")
    if not isinstance(name, str) or not name.strip():
        raise ValueError("signal.score_model_version must be nonempty")
    if type(_required(signal, "horizon", "signal")) is not int or signal[
        "horizon"
    ] not in (5, 20, 60):
        raise ValueError("signal.horizon must be one of 5, 20, 60")

    selection = normalized["selection"]
    if selection.get("mode") not in ("top_n", "top_percentile"):
        raise ValueError("selection.mode is unsupported")
    size = _required(selection, "portfolio_size", "selection")
    if type(size) is not int or size <= 0:
        raise ValueError("selection.portfolio_size must be positive")
    if selection["mode"] == "top_percentile":
        for key in ("lower", "upper"):
            selection[key] = _finite_number(
                _required(selection, key, "selection"), f"selection.{key}", high=1
            )
        if selection["lower"] > selection["upper"]:
            raise ValueError("selection.lower must be <= upper")
    elif "lower" in selection or "upper" in selection:
        raise ValueError("top_n must not declare percentile bounds")

    sizing = normalized["sizing"]
    if sizing.get("weighting") != "equal":
        raise ValueError("sizing.weighting currently supports equal only")
    sizing["cash_reserve_pct"] = _finite_number(
        _required(sizing, "cash_reserve_pct", "sizing"),
        "sizing.cash_reserve_pct",
        high=1,
    )
    if sizing["cash_reserve_pct"] >= 1:
        raise ValueError("sizing.cash_reserve_pct must be < 1")
    for key, value in normalized["constraints"].items():
        if key in ("exclude_st", "exclude_bse", "exclude_suspended"):
            if type(value) is not bool:
                raise ValueError(f"constraints.{key} must be boolean")
        elif value is not None:
            normalized["constraints"][key] = _finite_number(
                value, f"constraints.{key}", high=1 if key.endswith("_pct") else None
            )
    cadence = normalized["rebalance"].get("cadence_days")
    if type(cadence) is not int or cadence <= 0:
        raise ValueError("rebalance.cadence_days must be positive integer")

    risk = {**DEFAULT_RISK, **normalized["risk"]}
    for key in ("max_turnover_pct", "cash_floor_pct", "max_drawdown_pct"):
        if risk[key] is not None:
            risk[key] = _finite_number(risk[key], f"risk.{key}", high=1)
    if risk["max_positions"] is not None and (
        type(risk["max_positions"]) is not int or risk["max_positions"] <= 0
    ):
        raise ValueError("risk.max_positions must be positive integer or null")
    if risk["regime_policy"] != "none":
        raise ValueError("risk.regime_policy currently supports none only")
    normalized["risk"] = risk

    execution = {**DEFAULT_EXECUTION, **normalized["execution"]}
    for key in (
        "commission_rate",
        "minimum_commission_cny",
        "sell_stamp_duty_rate",
        "transfer_fee_rate",
        "slippage_per_side",
    ):
        execution[key] = _finite_number(execution[key], f"execution.{key}")
        if key != "minimum_commission_cny" and execution[key] >= 1:
            raise ValueError(f"execution.{key} must be < 1")
    if type(execution["board_lot"]) is not int or execution["board_lot"] < 1:
        raise ValueError("execution.board_lot must be positive integer")
    if execution["unknown_limit_policy"] not in ("fail_closed", "replay_compat"):
        raise ValueError(
            "execution.unknown_limit_policy must be explicit and supported"
        )
    normalized["execution"] = execution

    evidence = normalized["evidence"]
    if evidence.get("timing_version") != "paper_causal_v1":
        raise ValueError("evidence.timing_version must be paper_causal_v1")
    if evidence.get("policy") != "research":
        raise ValueError("evidence.policy currently supports research only")
    normalized["initial_nav"] = _finite_number(
        _required(normalized, "initial_nav", "spec"), "initial_nav", low=0.000001
    )
    return normalized


def migrate_legacy_config(config: dict) -> dict:
    """Accept *normalized and validated* legacy config; preserve hash separately.

    The caller must use validate_strategy_config before invoking this function.
    The legacy hash deliberately includes exactly the old normalized config,
    including the old rule that an absent execution override is absent.
    """
    legacy = copy.deepcopy(config)
    allowed = {
        "timing_version",
        "score_model_version",
        "horizon",
        "initial_nav",
        "selection",
        "constraints",
        "rebalance",
        "weighting",
        "cash_reserve_pct",
        "execution",
    }
    if not isinstance(legacy, dict) or set(legacy) - allowed:
        raise ValueError("legacy config has unsupported keys")
    if not all(
        key in legacy
        for key in (
            "timing_version",
            "score_model_version",
            "horizon",
            "initial_nav",
            "selection",
            "constraints",
            "rebalance",
            "weighting",
            "cash_reserve_pct",
        )
    ):
        raise ValueError("legacy config must be validated and normalized first")
    raw_execution = dict(legacy.get("execution") or {})
    # The existing paper model nests initial_nav under execution, but the
    # strategy's initial_nav is the strategy state of record.
    raw_execution.pop("initial_nav", None)
    selection = copy.deepcopy(legacy["selection"])
    if selection["mode"] == "top_n":
        selection.pop("lower", None)
        selection.pop("upper", None)
    spec = {
        "spec_version": SPEC_VERSION,
        "universe": {"market": "CN_A", "as_of": "PIT"},
        "signal": {
            "score_model_version": legacy["score_model_version"],
            "horizon": legacy["horizon"],
        },
        "selection": selection,
        "sizing": {
            "weighting": legacy["weighting"],
            "cash_reserve_pct": legacy["cash_reserve_pct"],
        },
        "constraints": copy.deepcopy(legacy["constraints"]),
        "rebalance": copy.deepcopy(legacy["rebalance"]),
        "risk": dict(DEFAULT_RISK),
        "execution": {**DEFAULT_EXECUTION, **raw_execution},
        "evidence": {
            "timing_version": legacy["timing_version"],
            "policy": "research",
        },
        "initial_nav": legacy["initial_nav"],
    }
    spec = validate_strategy_spec(spec)
    legacy_hash = hashlib.sha256(
        json.dumps(legacy, sort_keys=True, separators=(",", ":"), default=str).encode(
            "utf-8"
        )
    ).hexdigest()
    return {
        "spec": spec,
        "spec_hash": spec_hash(spec),
        "legacy_config_hash": legacy_hash,
    }


def to_legacy_config(spec: dict) -> dict:
    """Build a legacy config only when its semantics are representable.

    Use this as a compatibility adapter, never to certify a legacy forward
    window with the new spec hash.
    """
    resolved = validate_strategy_spec(spec)
    if resolved["risk"] != DEFAULT_RISK:
        raise ValueError(
            "legacy strategy runner cannot enforce non-default risk policy"
        )
    if resolved["execution"]["unknown_limit_policy"] != "fail_closed":
        raise ValueError("legacy strategy runner cannot enforce replay compatibility")
    execution = dict(resolved["execution"])
    execution.pop("unknown_limit_policy")
    execution["initial_nav"] = resolved["initial_nav"]
    return {
        "timing_version": resolved["evidence"]["timing_version"],
        "score_model_version": resolved["signal"]["score_model_version"],
        "horizon": resolved["signal"]["horizon"],
        "initial_nav": resolved["initial_nav"],
        "selection": copy.deepcopy(resolved["selection"]),
        "constraints": copy.deepcopy(resolved["constraints"]),
        "rebalance": copy.deepcopy(resolved["rebalance"]),
        "weighting": resolved["sizing"]["weighting"],
        "cash_reserve_pct": resolved["sizing"]["cash_reserve_pct"],
        "execution": execution,
    }
