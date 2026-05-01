# -*- coding: utf-8 -*-

SUPPORTED_HORIZONS = (5, 20, 60)
DEFAULT_MODEL_VERSION = "score_v2_202604"

SCORING_CONFIG = {
    5: {
        "effective_threshold": 0.02,
        "stop_loss_threshold": -0.05,
        "buy_threshold": 70.0,
        "watch_threshold": 50.0,
        "minimum_quote_count": 20,
        "momentum_lookback": 3,
        "breakout_lookback": 20,
        "risk_lookback": 10,
        "weights": {
            "signal_strength": 30.0,
            "momentum": 25.0,
            "trend_alignment": 20.0,
            "breakout_or_position": 15.0,
            "relative_strength": 0.0,
            "risk_penalty": 10.0,
        },
    },
    20: {
        "effective_threshold": 0.05,
        "stop_loss_threshold": -0.08,
        "buy_threshold": 70.0,
        "watch_threshold": 50.0,
        "minimum_quote_count": 60,
        "momentum_lookback": 10,
        "breakout_lookback": 60,
        "risk_lookback": 20,
        "weights": {
            "signal_strength": 15.0,
            "momentum": 15.0,
            "trend_alignment": 30.0,
            "breakout_or_position": 10.0,
            "relative_strength": 15.0,
            "risk_penalty": 15.0,
        },
    },
    60: {
        "effective_threshold": 0.08,
        "stop_loss_threshold": -0.12,
        "buy_threshold": 70.0,
        "watch_threshold": 50.0,
        "minimum_quote_count": 120,
        "momentum_lookback": 20,
        "breakout_lookback": 120,
        "risk_lookback": 60,
        "weights": {
            "signal_strength": 5.0,
            "momentum": 10.0,
            "trend_alignment": 35.0,
            "breakout_or_position": 10.0,
            "relative_strength": 25.0,
            "risk_penalty": 15.0,
        },
    },
}


def get_horizon_config(horizon: int) -> dict:
    if horizon not in SCORING_CONFIG:
        raise ValueError(f"Unsupported scoring horizon: {horizon}")
    return SCORING_CONFIG[horizon]
