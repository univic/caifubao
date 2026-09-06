# -*- coding: utf-8 -*-
"""Versioned strategy configuration.

The strategy layer never re-implements direction logic: it reads scores from a
named model_version (direction fixed at the scoring construction layer) and
always "buys high". A strategy config pins the score source and the selection /
constraint / rebalance semantics so a paper run is reproducible (config_hash
pinned in the persisted artifact, like the scoring model registry).
"""

from __future__ import annotations

import hashlib
import json

# Score-source default: the flip_wide shadow version registered by task 3.3
# tooling (construction-layer reversal at horizon 20). Configurable to any
# registered model version.
DEFAULT_SCORE_MODEL_VERSION = "flip_wide_shadow_v1"

DEFAULT_HORIZON = 20
DEFAULT_REBALANCE_CADENCE_DAYS = 5  # weekly (5 trading days)

# Execution cost parameters aligned with the autoresearch profile
# (autoresearch/profile.yaml -> execution). Paper NAV must use the same
# cost semantics as research so the paper track is comparable.
PAPER_EXECUTION = {
    "commission_rate": 0.00025,
    "minimum_commission_cny": 5.0,
    "sell_stamp_duty_rate": 0.001,
    "slippage_per_side": 0.001,
    "board_lot": 100,
    "initial_nav": 1_000_000.0,
}

# flip_wide research selection (flip_wide.yaml): top_percentile wide book.
DEFAULT_STRATEGY_CONFIG = {
    "score_model_version": DEFAULT_SCORE_MODEL_VERSION,
    "horizon": DEFAULT_HORIZON,
    "selection": {
        "mode": "top_percentile",
        "lower": 0.20,
        "upper": 1.00,
        "portfolio_size": 800,
    },
    "constraints": {
        "exclude_st": True,
        "exclude_bse": True,
        "exclude_suspended": True,
        "max_single_stock_pct": 0.05,  # ignored while equal-weight wide book
        "min_trade_amount_cny": 0.0,  # liquidity floor; 0 = unenforced
    },
    "rebalance": {"cadence_days": DEFAULT_REBALANCE_CADENCE_DAYS},
    "weighting": "equal",
    "cash_reserve_pct": 0.0,
}


def strategy_config_hash(config: dict) -> str:
    """Canonical order-insensitive sha256 of a strategy config (mirrors the
    scoring registry's model_config_hash semantics)."""
    canonical = json.dumps(config, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


_KNOWN_TOP_LEVEL = {
    "score_model_version",
    "horizon",
    "selection",
    "constraints",
    "rebalance",
    "weighting",
    "cash_reserve_pct",
}
_KNOWN_SELECTION_KEYS = {"mode", "lower", "upper", "portfolio_size"}
_KNOWN_CONSTRAINT_KEYS = {
    "exclude_st",
    "exclude_bse",
    "exclude_suspended",
    "max_single_stock_pct",
    "min_trade_amount_cny",
}
_KNOWN_REBALANCE_KEYS = {"cadence_days"}
_SELECTION_MODES = {"top_percentile", "top_n"}
_WEIGHTINGS = {"equal"}


def _reject_unknown_nested(parent: str, block, known: set) -> None:
    """Reject unknown keys inside a nested block (mirrors the scoring registry
    so a typo cannot be silently absorbed into the default and hashed into a
    reproducible but wrong configuration)."""
    if not isinstance(block, dict):
        return
    unknown = set(block) - known
    if unknown:
        raise ValueError(
            f"unknown {parent} keys: {sorted(unknown)}; known: {sorted(known)}"
        )


def _deep_merge(base: dict, overlay: dict) -> dict:
    """Deep-merge overlay onto base (overlay wins); returns a new dict."""
    result = json.loads(json.dumps(base, default=str))
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = json.loads(json.dumps(value, default=str))
    return result


def validate_strategy_config(config: dict) -> dict:
    """Validate a strategy config, returning a normalized deep copy.

    Missing optional blocks (constraints/rebalance/cash_reserve_pct) are filled
    from DEFAULT_STRATEGY_CONFIG so minimal configs stay valid. Unknown keys at
    every nesting level are rejected (typos fail loudly, never silently adopt a
    default). Raises ValueError with a specific message on the first problem.
    """
    if not isinstance(config, dict):
        raise ValueError("strategy config must be a dict")
    unknown = set(config) - _KNOWN_TOP_LEVEL
    if unknown:
        raise ValueError(
            f"unknown strategy config keys: {sorted(unknown)}; "
            f"known: {sorted(_KNOWN_TOP_LEVEL)}"
        )
    _reject_unknown_nested("selection", config.get("selection"), _KNOWN_SELECTION_KEYS)
    _reject_unknown_nested(
        "constraints", config.get("constraints"), _KNOWN_CONSTRAINT_KEYS
    )
    _reject_unknown_nested("rebalance", config.get("rebalance"), _KNOWN_REBALANCE_KEYS)

    normalized = _deep_merge(DEFAULT_STRATEGY_CONFIG, config)
    if "score_model_version" not in config:
        # The score source must always be explicit — never silently defaulted,
        # so a typo cannot run the wrong (e.g. flipped vs default) source.
        normalized.pop("score_model_version", None)
    version = str(normalized.get("score_model_version") or "").strip()
    if not version:
        raise ValueError("score_model_version is required")
    normalized["score_model_version"] = version

    horizon = normalized.get("horizon", DEFAULT_HORIZON)
    if horizon not in (5, 20, 60):
        raise ValueError(f"horizon must be one of 5/20/60, got {horizon!r}")

    selection = normalized.get("selection") or {}
    if not isinstance(selection, dict):
        raise ValueError("selection must be a dict")
    mode = selection.get("mode")
    if mode not in _SELECTION_MODES:
        raise ValueError(f"selection.mode must be in {sorted(_SELECTION_MODES)}")
    size = selection.get("portfolio_size")
    if not isinstance(size, int) or isinstance(size, bool) or size < 1:
        raise ValueError("selection.portfolio_size must be a positive integer")
    if mode == "top_percentile":
        lower = selection.get("lower", 0.0)
        upper = selection.get("upper", 1.0)
        for label, value in (("lower", lower), ("upper", upper)):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not 0 <= float(value) <= 1
            ):
                raise ValueError(f"selection.{label} must be in [0, 1]")
        if not float(lower) <= float(upper):
            raise ValueError("selection.lower must be <= selection.upper")
    elif mode == "top_n":
        # Inherited default bounds are irrelevant for top_n; only reject when
        # the caller explicitly passed them alongside top_n.
        user_selection = config.get("selection") or {}
        if isinstance(user_selection, dict):
            user_keys = set(user_selection)
            if user_keys & {"lower", "upper"}:
                raise ValueError("selection.top_n must not carry lower/upper bounds")
        selection.pop("lower", None)
        selection.pop("upper", None)
        normalized["selection"] = selection

    constraints = normalized.get("constraints") or {}
    if not isinstance(constraints, dict):
        raise ValueError("constraints must be a dict")
    for flag in ("exclude_st", "exclude_bse", "exclude_suspended"):
        if flag in constraints and not isinstance(constraints[flag], bool):
            raise ValueError(f"constraints.{flag} must be a bool")
    max_pct = constraints.get("max_single_stock_pct")
    if max_pct is not None and (
        isinstance(max_pct, bool)
        or not isinstance(max_pct, (int, float))
        or not 0 < float(max_pct) <= 1
    ):
        raise ValueError("constraints.max_single_stock_pct must be in (0, 1]")
    min_amt = constraints.get("min_trade_amount_cny")
    if min_amt is not None and (
        isinstance(min_amt, bool)
        or not isinstance(min_amt, (int, float))
        or float(min_amt) < 0
    ):
        raise ValueError("constraints.min_trade_amount_cny must be >= 0")

    rebalance = normalized.get("rebalance") or {}
    if not isinstance(rebalance, dict):
        raise ValueError("rebalance must be a dict")
    cadence = rebalance.get("cadence_days", DEFAULT_REBALANCE_CADENCE_DAYS)
    if isinstance(cadence, bool) or not isinstance(cadence, int) or cadence < 1:
        raise ValueError("rebalance.cadence_days must be a positive integer")

    weighting = normalized.get("weighting", "equal")
    if weighting not in _WEIGHTINGS:
        raise ValueError(f"weighting must be in {sorted(_WEIGHTINGS)}")

    cash_reserve = normalized.get("cash_reserve_pct", 0.0)
    if (
        isinstance(cash_reserve, bool)
        or not isinstance(cash_reserve, (int, float))
        or not 0 <= float(cash_reserve) < 1
    ):
        raise ValueError("cash_reserve_pct must be in [0, 1)")

    return normalized
