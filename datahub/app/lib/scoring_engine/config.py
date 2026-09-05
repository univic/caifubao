# -*- coding: utf-8 -*-

from copy import deepcopy

SUPPORTED_HORIZONS = (5, 20, 60)
DEFAULT_MODEL_VERSION = "score_v2_202605b"

SCORING_CONFIG = {
    5: {
        "effective_threshold": 0.02,
        "stop_loss_threshold": -0.05,
        "buy_threshold": 70.0,
        "watch_threshold": 50.0,
        "avoid_threshold": 20.0,
        "buy_percentile": 0.95,
        "watch_percentile": 0.80,
        "avoid_percentile": 0.20,
        "minimum_quote_count": 20,
        "momentum_lookback": 3,
        "breakout_lookback": 20,
        "risk_lookback": 10,
        "signal_decay_factor": 0.7,
        "signal_decay_max_days": 5,
        "weights": {
            "signal_strength": 25.0,
            "momentum": 25.0,
            "trend_alignment": 20.0,
            "breakout_or_position": 10.0,
            "industry_momentum": 5.0,
            "relative_strength": 0.0,
            "real_relative_strength": 10.0,
            "risk_penalty": 10.0,
        },
    },
    20: {
        "effective_threshold": 0.05,
        "stop_loss_threshold": -0.08,
        "buy_threshold": 70.0,
        "watch_threshold": 50.0,
        "avoid_threshold": 20.0,
        "buy_percentile": 0.95,
        "watch_percentile": 0.80,
        "avoid_percentile": 0.20,
        "minimum_quote_count": 60,
        "momentum_lookback": 10,
        "breakout_lookback": 60,
        "risk_lookback": 20,
        "signal_decay_factor": 0.7,
        "signal_decay_max_days": 10,
        "weights": {
            "signal_strength": 15.0,
            "momentum": 15.0,
            "trend_alignment": 30.0,
            "breakout_or_position": 5.0,
            "industry_momentum": 5.0,
            "relative_strength": 15.0,
            "real_relative_strength": 10.0,
            "risk_penalty": 15.0,
        },
    },
    60: {
        "effective_threshold": 0.08,
        "stop_loss_threshold": -0.12,
        "buy_threshold": 70.0,
        "watch_threshold": 50.0,
        "avoid_threshold": 20.0,
        "buy_percentile": 0.95,
        "watch_percentile": 0.80,
        "avoid_percentile": 0.20,
        "minimum_quote_count": 120,
        "momentum_lookback": 20,
        "breakout_lookback": 120,
        "risk_lookback": 60,
        "signal_decay_factor": 0.5,
        "signal_decay_max_days": 20,
        "weights": {
            "signal_strength": 5.0,
            "momentum": 10.0,
            "trend_alignment": 35.0,
            "breakout_or_position": 5.0,
            "industry_momentum": 5.0,
            "relative_strength": 25.0,
            "real_relative_strength": 8.0,
            "risk_penalty": 15.0,
        },
    },
}


def get_horizon_config(horizon: int) -> dict:
    if horizon not in SCORING_CONFIG:
        raise ValueError(f"Unsupported scoring horizon: {horizon}")
    return SCORING_CONFIG[horizon]


def get_effective_horizon_config(
    horizon: int, override_config: dict | None = None
) -> dict:
    config = deepcopy(get_horizon_config(horizon))
    if not override_config:
        return config

    horizon_override = (
        override_config.get(str(horizon)) or override_config.get(horizon) or {}
    )
    if not isinstance(horizon_override, dict):
        return config

    weights_override = horizon_override.get("weights")
    if weights_override is None:
        known_weight_keys = set(config.get("weights", {}).keys())
        weights_override = {
            key: value
            for key, value in horizon_override.items()
            if key in known_weight_keys
        }

    for key, value in horizon_override.items():
        if key == "weights" or key in config.get("weights", {}):
            continue
        config[key] = value

    if isinstance(weights_override, dict):
        config["weights"].update(weights_override)

    # Component direction override. Each horizon's components default to
    # positive (higher raw value -> higher score) and penalties to negative
    # (higher raw penalty -> lower score). An override can flip any component
    # or penalty to -1/0/+1 (e.g. construction-layer reversal for candidates
    # whose cross-sectional IC is negative, like the research flip_wide).
    # The resolved per-component direction map is stored under "directions":
    # {"component_id": +1|-1|0, ...}. Values missing from the override keep
    # their default polarity (component +1, penalty -1).
    directions_override = horizon_override.get("directions")
    if directions_override is not None and not isinstance(directions_override, dict):
        raise ValueError(
            f"directions override must be a dict, got {type(directions_override).__name__}"
        )
    if isinstance(directions_override, dict):
        allowed = set(config.get("weights", {}).keys())
        unknown = set(map(str, directions_override)) - allowed
        if unknown:
            raise ValueError(
                "direction override keys must be scored components/penalties; "
                f"got {sorted(unknown)}"
            )
        # Validate raw values BEFORE coercing so e.g. 0.5 does not silently
        # truncate to 0 and bools (subclass of int) do not pass as ints.
        for value in directions_override.values():
            if isinstance(value, bool) or value not in (-1, 0, 1):
                raise ValueError("component directions must be -1, 0, or 1")
        resolved = {}
        for key in allowed:
            default = -1 if key == "risk_penalty" else 1
            raw = directions_override.get(key)
            resolved[key] = default if raw is None else int(raw)
        config["directions"] = resolved

    return config
