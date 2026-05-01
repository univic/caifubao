# -*- coding: utf-8 -*-

import math
from statistics import pstdev


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def quote_price(quote, field: str = "close") -> float | None:
    value = getattr(quote, f"{field}_hfq", None)
    if value is None:
        value = getattr(quote, field, None)
    return float(value) if value is not None else None


def build_component(
    component_id: str,
    group: str,
    label: str,
    raw_value,
    normalized_value: float,
    weight: float,
    evidence: dict | None = None,
) -> dict:
    normalized = clamp(float(normalized_value))
    contribution = round(normalized * float(weight), 4)
    return {
        "id": component_id,
        "group": group,
        "label": label,
        "raw_value": raw_value,
        "normalized_value": round(normalized, 4),
        "weight": float(weight),
        "contribution": contribution,
        "direction": "positive",
        "evidence": evidence or {},
    }


def build_penalty(
    penalty_id: str,
    label: str,
    raw_value,
    normalized_value: float,
    weight: float,
    evidence: dict | None = None,
) -> dict:
    normalized = clamp(float(normalized_value))
    contribution = round(-normalized * float(weight), 4)
    return {
        "id": penalty_id,
        "group": "risk",
        "label": label,
        "raw_value": raw_value,
        "normalized_value": round(normalized, 4),
        "weight": float(weight),
        "contribution": contribution,
        "direction": "negative",
        "evidence": evidence or {},
    }


def signal_strength_component(signals: list, weight: float) -> dict:
    bullish_signals = [
        signal
        for signal in signals
        if getattr(signal, "direction", None) == "BULLISH"
        and getattr(signal, "signal_name", None)
    ]
    if not bullish_signals:
        normalized = 0.0
    else:
        strengths = [
            float(getattr(signal, "strength", 1.0) or 1.0) for signal in bullish_signals
        ]
        normalized = clamp(sum(strengths) / max(len(strengths), 1))

    return build_component(
        "signal_strength",
        "signal",
        "Bullish signal strength",
        [getattr(signal, "signal_name", None) for signal in bullish_signals],
        normalized,
        weight,
        {
            "signals": [
                {
                    "name": getattr(signal, "signal_name", None),
                    "strength": getattr(signal, "strength", None),
                    "reason": getattr(signal, "reason", None),
                }
                for signal in bullish_signals
            ]
        },
    )


def trend_alignment_component(quote, factors, horizon: int, weight: float) -> dict:
    close = quote_price(quote)
    if close is None or factors is None:
        return build_component(
            "trend_alignment",
            "trend",
            "Moving-average trend alignment",
            None,
            0.0,
            weight,
        )

    checks = []
    ma_20 = getattr(factors, "ma_20", None)
    ma_60 = getattr(factors, "ma_60", None)
    ma_120 = getattr(factors, "ma_120", None)

    if ma_20:
        checks.append(close > ma_20)
    if ma_60:
        checks.append(close > ma_60)
        if ma_20:
            checks.append(ma_20 > ma_60)
    if horizon >= 60 and ma_120:
        checks.append(close > ma_120)
        if ma_60:
            checks.append(ma_60 > ma_120)

    normalized = sum(1 for item in checks if item) / len(checks) if checks else 0.0
    return build_component(
        "trend_alignment",
        "trend",
        "Moving-average trend alignment",
        normalized,
        normalized,
        weight,
        {
            "close": close,
            "ma_20": ma_20,
            "ma_60": ma_60,
            "ma_120": ma_120,
        },
    )


def momentum_component(
    quote, previous_quotes: list, lookback: int, weight: float
) -> dict:
    close = quote_price(quote)
    if close is None or len(previous_quotes) < 1:
        return build_component(
            "momentum", "momentum", "Recent momentum", None, 0.0, weight
        )

    oldest = previous_quotes[-1]
    old_close = quote_price(oldest)
    if not old_close:
        return build_component(
            "momentum", "momentum", "Recent momentum", None, 0.0, weight
        )

    change = (close - old_close) / old_close
    normalized = clamp(change / 0.1)
    return build_component(
        "momentum",
        "momentum",
        "Recent momentum",
        round(change, 6),
        normalized,
        weight,
        {"lookback": lookback, "close": close, "old_close": old_close},
    )


def breakout_or_position_component(quote, history_quotes: list, weight: float) -> dict:
    close = quote_price(quote)
    if close is None or not history_quotes:
        return build_component(
            "breakout_or_position",
            "position",
            "Recent range position",
            None,
            0.0,
            weight,
        )

    highs = [quote_price(item, "high") for item in history_quotes]
    lows = [quote_price(item, "low") for item in history_quotes]
    highs = [value for value in highs if value is not None]
    lows = [value for value in lows if value is not None]
    if not highs or not lows:
        return build_component(
            "breakout_or_position",
            "position",
            "Recent range position",
            None,
            0.0,
            weight,
        )

    high = max(highs)
    low = min(lows)
    if close >= high:
        normalized = 1.0
    elif high > low:
        normalized = clamp((close - low) / (high - low))
    else:
        normalized = 0.0

    return build_component(
        "breakout_or_position",
        "position",
        "Recent range position",
        {"close": close, "range_high": high, "range_low": low},
        normalized,
        weight,
        {"history_count": len(history_quotes)},
    )


def relative_strength_component(quote, previous_quotes: list, weight: float) -> dict:
    close = quote_price(quote)
    if close is None or not previous_quotes:
        return build_component(
            "relative_strength",
            "relative_strength",
            "Relative strength placeholder",
            None,
            0.0,
            weight,
        )

    old_close = quote_price(previous_quotes[-1])
    if not old_close:
        normalized = 0.0
        change = None
    else:
        change = (close - old_close) / old_close
        normalized = clamp((change + 0.05) / 0.15)

    return build_component(
        "relative_strength",
        "relative_strength",
        "Relative strength proxy",
        round(change, 6) if change is not None else None,
        normalized,
        weight,
        {"note": "Universe/index relative strength can replace this proxy later."},
    )


def risk_penalty(quote, history_quotes: list, weight: float) -> dict:
    closes = [quote_price(item) for item in reversed(history_quotes)]
    current_close = quote_price(quote)
    if current_close is not None:
        closes.append(current_close)
    closes = [value for value in closes if value is not None and value > 0]

    returns = []
    for idx in range(1, len(closes)):
        returns.append((closes[idx] - closes[idx - 1]) / closes[idx - 1])

    volatility = pstdev(returns) if len(returns) > 1 else 0.0
    trade_status = getattr(quote, "trade_status", 1)
    is_st = getattr(quote, "isST", 0)
    abnormal = trade_status == 0 or is_st == 1
    raw_penalty = volatility / 0.06
    if abnormal:
        raw_penalty += 1.0
    if math.isnan(raw_penalty):
        raw_penalty = 0.0

    return build_penalty(
        "risk_penalty",
        "Volatility and tradeability risk",
        round(volatility, 6),
        raw_penalty,
        weight,
        {"trade_status": trade_status, "is_st": is_st, "return_count": len(returns)},
    )
