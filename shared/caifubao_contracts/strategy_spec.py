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
from caifubao_contracts.strategy_accounting import validate_accounting
from caifubao_contracts.strategy_allocation import validate_allocation
from caifubao_contracts.strategy_benchmark import (
    validate_benchmark,
    validate_kill_criteria,
)
from caifubao_contracts.strategy_sleeve import (
    validate_horizon_claim,
    validate_sleeves,
)
from caifubao_contracts.strategy_exit_policy import validate_exit_policy
from caifubao_contracts.strategy_spec_consumers import leaf_paths

SPEC_VERSION = "1"
#: Declaration versions this validator accepts. 1 is frozen for replay; 2 adds
#: the strategy family and an explicit exit contract.
SUPPORTED_SPEC_VERSIONS = ("1", "2")
DEFAULT_RISK = {
    "max_turnover_pct": None,
    "max_positions": None,
    "cash_floor_pct": 0.0,
    "max_drawdown_pct": None,
    "max_single_stock_pct": None,
    "max_gross_exposure_pct": None,
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
        "family",
        "allocation",
        "accounting",
        "benchmark",
        "sleeves",
        "horizon_claim",
    },
    "universe": {"market", "as_of"},
    "signal": {"score_model_version", "horizon"},
    "selection": {"mode", "lower", "upper", "portfolio_size", "exit"},
    "allocation": {"assets", "band_pct"},
    "accounting": {"corporate_actions", "mark_price"},
    "benchmark": {"kind", "assets", "index"},
    "sleeves": {
        "name",
        "capital_share",
        "accounting_id",
        "evidence_id",
        "on_shortfall",
    },
    "horizon_claim": {"hypothesis_horizon", "evidence_horizon", "rationale"},
    "selection.exit": {
        "rank_below",
        "stop_loss_pct",
        "max_hold_sessions",
        "on_ineligible",
        "min_hold_sessions",
    },
    "sizing": {"weighting", "cash_reserve_pct"},
    "constraints": {
        "exclude_st",
        "exclude_bse",
        "exclude_suspended",
        "exclude_infeasible_buys",
        "max_single_stock_pct",
        "max_industry_pct",
        "min_trade_amount_cny",
    },
    "rebalance": {"cadence_days", "exit_buffer", "min_hold_sessions"},
    "risk": set(DEFAULT_RISK),
    "execution": set(DEFAULT_EXECUTION),
    "evidence": {"timing_version", "policy", "kill_criteria"},
    "evidence.kill_criteria": {
        "min_excess_per_period",
        "max_drawdown",
        "review_after_sessions",
        "on_breach",
    },
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


_MANIFEST: dict | None = None


def _consumer_manifest() -> dict:
    """The consumer manifest that ships beside this contract, read once."""
    global _MANIFEST
    if _MANIFEST is None:
        import json
        from pathlib import Path

        path = Path(__file__).with_name("strategy_spec_consumers.json")
        _MANIFEST = json.loads(path.read_text(encoding="utf-8"))
    return _MANIFEST


def present_leaf_paths(spec: dict) -> list[str]:
    """Leaf field paths that this declaration actually carries."""
    present = []
    for path in leaf_paths(_ALLOWED):
        node = spec
        found = True
        for part in path.split("."):
            if not isinstance(node, dict) or part not in node:
                found = False
                break
            node = node[part]
        if found:
            present.append(path)
    return present


def check_manifest_coverage(spec: dict) -> None:
    """Refuse a v2 declaration whose fields the manifest does not account for.

    This is what makes the manifest load-bearing at validation time rather than a
    CI comment: a field that no entry names, or that no v2 consumer reads, cannot
    appear in a v2 declaration at all.
    """
    fields = _consumer_manifest().get("fields") or {}
    for path in present_leaf_paths(spec):
        entry = fields.get(path)
        if entry is None:
            raise ValueError(
                f"spec_version 2 field {path!r} has no consumer manifest entry"
            )
        if "2" not in (entry.get("consumed_by_versions") or []):
            raise ValueError(
                f"spec_version 2 field {path!r} is not read by any v2 consumer"
            )


def _validate_rank_selection(selection: dict) -> None:
    """Validate the rank-selection fields; the allocation family skips these."""
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
    version = normalized.get("spec_version")
    if version not in SUPPORTED_SPEC_VERSIONS:
        raise ValueError(f"spec_version must be one of {list(SUPPORTED_SPEC_VERSIONS)}")
    # `signal` is a scoring source. An allocation declaration has none, so the
    # block becomes optional for that family and required for every other one;
    # there is no placeholder model version in between.
    allocation_family = normalized.get("family") == "asset_allocation"
    required_parts = [
        "universe",
        "rebalance",
        "risk",
        "execution",
        "evidence",
    ]
    if not allocation_family:
        required_parts += ["selection", "sizing", "constraints"]
    for part in required_parts:
        _object(_required(normalized, part, "spec"), part)
    if "signal" in normalized:
        _object(normalized["signal"], "signal")
    elif not allocation_family:
        raise ValueError("signal is required unless the family is asset_allocation")

    v2_only = (
        "family",
        "allocation",
        "accounting",
        "benchmark",
        "sleeves",
        "horizon_claim",
    )
    if version == "1":
        # v1 must behave exactly as before: the v2-only fields are still unknown
        # to it, so a v1 declaration carrying one is rejected rather than
        # silently accepted into a hash that later reads differently.
        present = [key for key in v2_only if key in normalized]
        if "exit" in (normalized.get("selection") or {}):
            present.append("selection.exit")
        if "kill_criteria" in (normalized.get("evidence") or {}):
            present.append("evidence.kill_criteria")
        if present:
            raise ValueError(
                f"spec_version 1 does not support {sorted(present)}; use version 2"
            )
    else:
        # The family is universal; the allocation block is required only by the
        # allocation family and rejected by the rank family below.
        for key in ("family",):
            if key not in normalized:
                raise ValueError(f"spec_version 2 requires {key}")
        exit_block = _object(
            _required(normalized["selection"], "exit", "selection"), "selection.exit"
        )
        validate_exit_policy(normalized["family"], exit_block)
        normalized["selection"]["exit"] = exit_block
        for key in ("accounting", "benchmark"):
            if key not in normalized:
                raise ValueError(
                    f"spec_version 2 requires {key}: a strategy that cannot state "
                    f"its {key} cannot be measured or compared"
                )
        normalized["accounting"] = validate_accounting(normalized["accounting"])
        normalized["benchmark"] = validate_benchmark(normalized["benchmark"])
        evidence_block = normalized["evidence"]
        if "kill_criteria" not in evidence_block:
            raise ValueError(
                "spec_version 2 requires evidence.kill_criteria: a strategy with no "
                "declared failure condition is never retired, only observed"
            )
        evidence_block["kill_criteria"] = validate_kill_criteria(
            evidence_block["kill_criteria"]
        )
        check_manifest_coverage(normalized)
        # Sleeve isolation and horizon discipline are optional blocks, but when
        # present they are enforced rather than recorded.
        if "sleeves" in normalized:
            raw = normalized["sleeves"]
            if not isinstance(raw, list):
                raise ValueError("sleeves must be a list of sleeve objects")
            normalized["sleeves"] = validate_sleeves(raw)
        if "horizon_claim" in normalized:
            normalized["horizon_claim"] = validate_horizon_claim(
                normalized["horizon_claim"]
            )
        if normalized["family"] == "asset_allocation":
            if "allocation" not in normalized:
                raise ValueError(
                    "spec_version 2 requires allocation for asset_allocation: a "
                    "fixed asset set and target weights are its book semantics"
                )
            normalized["allocation"] = validate_allocation(normalized["allocation"])
        elif "allocation" in normalized:
            raise ValueError(
                "allocation is only meaningful for asset_allocation; rank_selection "
                "sizes by rank"
            )

    universe = normalized["universe"]
    if universe != {"market": "CN_A", "as_of": "PIT"}:
        raise ValueError("universe requires market=CN_A and as_of=PIT")
    signal = normalized.get("signal")
    if signal is not None:
        name = _required(signal, "score_model_version", "signal")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("signal.score_model_version must be nonempty")
        if type(_required(signal, "horizon", "signal")) is not int or signal[
            "horizon"
        ] not in (5, 20, 60):
            raise ValueError("signal.horizon must be one of 5, 20, 60")
    elif not allocation_family:
        raise ValueError("signal is required unless the family is asset_allocation")

    if allocation_family and version == "2":
        selection_block = normalized.get("selection") or {}
        # An allocation book sizes by declared weights and ranks nothing, so
        # rank-selection semantics must not be declared where they mean nothing:
        # they would be validated, hashed and never read.
        rank_only = sorted(set(selection_block) - {"exit"})
        if rank_only:
            raise ValueError(
                "asset_allocation must not declare rank-selection semantics: "
                f"{rank_only}; the asset set and its weights are the book"
            )
        for block in ("sizing", "constraints"):
            if normalized.get(block):
                raise ValueError(
                    f"asset_allocation must not declare {block}: "
                    f"{sorted(normalized[block])} are rank-selection limits"
                )
        normalized.pop("sizing", None)
        normalized.pop("constraints", None)
    selection = normalized.get("selection") or {}
    if allocation_family and version == "2":
        # Nothing below applies: the allocation family declares only its exit
        # contract, and sizing/constraints were already removed above.
        selection_checked = selection
    else:
        _validate_rank_selection(selection)
    selection = selection_checked if allocation_family and version == "2" else selection

    # The allocation family declares its sizing as asset weights, so the
    # rank-selection sizing and eligibility blocks below do not apply to it.
    skip_rank_blocks = allocation_family and version == "2"
    if not skip_rank_blocks:
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
    for key, value in (normalized.get("constraints") or {}).items():
        if key in (
            "exclude_st",
            "exclude_bse",
            "exclude_suspended",
            "exclude_infeasible_buys",
        ):
            if type(value) is not bool:
                raise ValueError(f"constraints.{key} must be boolean")
        elif value is not None:
            normalized["constraints"][key] = _finite_number(
                value, f"constraints.{key}", high=1 if key.endswith("_pct") else None
            )
    cadence = normalized["rebalance"].get("cadence_days")
    if type(cadence) is not int or cadence <= 0:
        raise ValueError("rebalance.cadence_days must be positive integer")
    # Optional exit hysteresis. Deliberately not defaulted: the normalized spec
    # is hashed byte-for-byte, so injecting a default here would change the
    # identity of every already-certified window.
    rebalance = normalized["rebalance"]
    if "exit_buffer" in rebalance:
        buffer = _finite_number(
            rebalance["exit_buffer"], "rebalance.exit_buffer", low=1.0
        )
        if buffer < 1.0:
            raise ValueError("rebalance.exit_buffer must be >= 1.0")
        rebalance["exit_buffer"] = buffer
    if "min_hold_sessions" in rebalance:
        value = rebalance["min_hold_sessions"]
        if type(value) is not int or value < 0:
            raise ValueError(
                "rebalance.min_hold_sessions must be a non-negative integer"
            )

    risk = {**DEFAULT_RISK, **normalized["risk"]}
    for key in (
        "max_turnover_pct",
        "cash_floor_pct",
        "max_drawdown_pct",
        "max_single_stock_pct",
        "max_gross_exposure_pct",
    ):
        if risk[key] is not None:
            risk[key] = _finite_number(risk[key], f"risk.{key}", high=1)
    if risk["max_positions"] is not None and (
        type(risk["max_positions"]) is not int or risk["max_positions"] <= 0
    ):
        raise ValueError("risk.max_positions must be positive integer or null")
    if risk["regime_policy"] not in ("none", "bull_only", "bear_reduce_only"):
        raise ValueError(
            "risk.regime_policy must be none, bull_only or bear_reduce_only"
        )
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
    # Even without decision-time prices we can reject capital budgets that
    # cannot afford the *minimum commission itself*. Price/lot affordability
    # requires the separate snapshot-aware preflight before execution.
    if skip_rank_blocks:
        # The per-name budget check is a rank-selection concern: an allocation
        # book's per-name budget comes from its declared weights.
        names = max(1, len(normalized.get("allocation", {}).get("assets") or {}))
        reserve = 0.0
        cap = None
    else:
        names = selection["portfolio_size"]
        reserve = sizing["cash_reserve_pct"]
        cap = (normalized.get("constraints") or {}).get("max_single_stock_pct")
    budget = normalized["initial_nav"] * (1 - reserve) / names
    if cap is not None:
        budget = min(budget, normalized["initial_nav"] * cap)
    if execution["minimum_commission_cny"] >= budget:
        raise ValueError(
            "initial_nav/selection.portfolio_size leaves too little "
            "per-name budget for execution.minimum_commission_cny"
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
    # Exit hysteresis is a spec-level execution semantic with no legacy
    # counterpart: dropping it keeps the legacy config (and therefore
    # legacy_config_hash) byte-identical for specs that declare it.
    rebalance = {
        key: value
        for key, value in resolved["rebalance"].items()
        if key == "cadence_days"
    }
    return {
        "timing_version": resolved["evidence"]["timing_version"],
        "score_model_version": resolved["signal"]["score_model_version"],
        "horizon": resolved["signal"]["horizon"],
        "initial_nav": resolved["initial_nav"],
        "selection": copy.deepcopy(resolved["selection"]),
        "constraints": copy.deepcopy(resolved["constraints"]),
        "rebalance": rebalance,
        "weighting": resolved["sizing"]["weighting"],
        "cash_reserve_pct": resolved["sizing"]["cash_reserve_pct"],
        "execution": execution,
    }


class OpeningTradeInfeasible(ValueError):
    """A valid decision-time quote cannot meet an opening execution constraint."""

    def __init__(self, message: str, *, reason_code: str):
        super().__init__(message)
        self.reason_code = reason_code


def preflight_execution_feasibility(
    spec: dict,
    *,
    reference_prices: dict[str, float],
    target_weights: dict[str, float] | None = None,
    max_minimum_commission_pct: float = 0.01,
) -> dict:
    """Fail closed on an explicit target-price snapshot before generating orders.

    Static spec validation cannot know stock prices or board composition; the
    caller MUST supply decision-time prices of prospective target names. This
    validation uses the same board lot and fee arithmetic as the simulator.
    A per-name budget represents equal weighting after the cash reserve and
    any per-name cap. A 1% minimum-commission cap is explicit and configurable.
    No target is silently assumed affordable from cash alone.
    """
    from caifubao_contracts.strategy_execution import (
        adjusted_price,
        board_lot_rule,
        execution_fees,
    )
    from caifubao_contracts.friction import CostProfile

    normalized = validate_strategy_spec(spec)
    if (
        isinstance(max_minimum_commission_pct, bool)
        or not isinstance(max_minimum_commission_pct, (int, float))
        or not math.isfinite(max_minimum_commission_pct)
        or not 0 < max_minimum_commission_pct <= 1
    ):
        raise ValueError("max_minimum_commission_pct must be finite in (0, 1]")
    if not isinstance(reference_prices, dict) or not reference_prices:
        raise ValueError("reference_prices must be a nonempty decision-time quote map")
    width = normalized["selection"]["portfolio_size"]
    if len(reference_prices) > width:
        raise ValueError("reference_prices exceeds selection.portfolio_size")
    if target_weights is not None and set(target_weights) != set(reference_prices):
        raise ValueError(
            "target_weights and reference_prices must name the same stocks"
        )
    initial_nav = normalized["initial_nav"]
    investable = initial_nav * (1 - normalized["sizing"]["cash_reserve_pct"])
    budget = investable / width
    cap = normalized["constraints"].get("max_single_stock_pct")
    if cap is not None:
        budget = min(budget, initial_nav * cap)
    execution = normalized["execution"]
    if execution["minimum_commission_cny"] >= budget:
        raise ValueError("initial_nav/portfolio_size cannot cover minimum commission")
    profile = CostProfile(
        commission_rate=execution["commission_rate"],
        minimum_commission_cny=execution["minimum_commission_cny"],
        sell_stamp_duty_rate=execution["sell_stamp_duty_rate"],
        transfer_fee_rate=execution["transfer_fee_rate"],
        slippage_per_side=execution["slippage_per_side"],
        board_lot=execution["board_lot"],
    )
    details = []
    for code in sorted(reference_prices):
        raw = _finite_number(
            reference_prices[code], f"reference_prices.{code}", low=0.000001
        )
        minimum, _ = board_lot_rule(code, execution["board_lot"])
        name_budget = budget
        if target_weights is not None:
            weight = _finite_number(
                target_weights[code], f"target_weights.{code}", low=0.00000001, high=1
            )
            name_budget = min(budget, initial_nav * weight)
        price = adjusted_price(raw, "BUY", execution["slippage_per_side"])
        fees = execution_fees(price, minimum, "BUY", profile)
        minimum_spend = price * minimum + fees["commission"] + fees["transfer_fee"]
        minimum_share = fees["commission"] / (price * minimum)
        if minimum_spend > name_budget:
            raise OpeningTradeInfeasible(
                f"initial_nav/portfolio_size cannot buy board minimum for {code}: "
                f"budget={name_budget:.2f}, needed={minimum_spend:.2f}",
                reason_code="board_minimum_over_budget",
            )
        if minimum_share > max_minimum_commission_pct:
            raise OpeningTradeInfeasible(
                f"minimum_commission_cny makes trade uneconomic for {code}: "
                f"share={minimum_share:.6f}, maximum={max_minimum_commission_pct:.6f}",
                reason_code="minimum_commission_ratio",
            )
        details.append(
            {
                "stock_code": code,
                "board_minimum": minimum,
                "target_budget_cny": round(name_budget, 2),
                "minimum_spend_cny": round(minimum_spend, 2),
                "minimum_commission_pct": round(minimum_share, 8),
            }
        )
    return {
        "spec_hash": spec_hash(normalized),
        "per_name_budget_cny": round(budget, 2),
        "checked_names": len(details),
        "details": details,
    }
