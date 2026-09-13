# -*- coding: utf-8 -*-
"""Score reconstruction for the real 8-component model on historical dates.

The frozen factor-lab panel carries only price/volume features, so it cannot
measure ``flip_wide`` (whose identity is the *eight-component* construction).
This module rebuilds the components the way ``scoring_engine.components``
defines them, from the tables the engine itself reads (``stock_daily_quote``,
``stock_factor_daily``, ``stock_industry``), so a small-book test can run on the
real signal instead of a single-factor proxy.

Scope and honesty
-----------------
* **Verified against the engine**: the formulas and thresholds mirror
  ``components.py`` (momentum 10-session change / 0.1; relative strength
  (change+0.05)/0.15; trend alignment = share of MA checks true; breakout =
  (close-low)/(high-low) over the breakout lookback; real relative strength =
  CSI300 alpha; risk penalty = stdev(returns)/0.06 (+1 when ST/suspended)).
* **Not reconstructed**: ``signal_strength`` (needs the signal decay chain) and
  ``industry_momentum`` (needs the industry daily-metrics aggregate). Callers
  must **renormalise over the components they have** and report which are
  missing; the two omitted components are 20 of 110 weight in H20.
* Direction is applied per call: the ``flip`` side negates every alpha
  component and keeps the penalty's standard sign, matching the registry entry
  for ``flip_wide_shadow_v1``.
"""

from __future__ import annotations

import math
from statistics import pstdev

import pandas as pd

#: H20 config values from `scoring_engine/config.py`.
LOOKBACKS = {
    "momentum_lookback": 10,
    "breakout_lookback": 60,
    "risk_lookback": 20,
}
WEIGHTS = {
    "momentum": 15.0,
    "trend_alignment": 30.0,
    "breakout_or_position": 5.0,
    "relative_strength": 15.0,
    "real_relative_strength": 10.0,
    "risk_penalty": 15.0,
}
#: Components this module does not rebuild, with their H20 weights, so a report
#: can state exactly what fraction of the model is missing.
UNAVAILABLE = {"signal_strength": 15.0, "industry_momentum": 5.0}


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return 0.0
    return max(low, min(high, float(value)))


def _series_close(frame: pd.DataFrame) -> pd.Series:
    """Tradable close: HFQ when present, else the raw close column."""
    hfq = pd.to_numeric(frame.get("close_hfq"), errors="coerce")
    if hfq is None:
        return pd.to_numeric(frame["close"], errors="coerce")
    if "close" in frame.columns:
        raw = pd.to_numeric(frame["close"], errors="coerce")
        return hfq.where(hfq > 0, raw)
    return hfq


def build_features(quotes: pd.DataFrame, factors: pd.DataFrame) -> pd.DataFrame:
    """Per-(code, date) component inputs: MA ratios, range, volatility.

    ``quotes`` needs ``code, date, close_hfq, high_hfq, low_hfq, trade_amount``;
    ``factors`` needs ``stock_code, date, ma_20, ma_60, ma_120``.
    """
    frame = quotes.rename(columns={"code": "stock_code"}).copy()
    if "close" not in frame.columns and "close_hfq" in frame.columns:
        frame["close"] = frame["close_hfq"]
    frame["date"] = pd.to_datetime(frame["date"])
    frame["close"] = _series_close(frame)
    frame["high"] = pd.to_numeric(frame["high_hfq"], errors="coerce")
    frame["low"] = pd.to_numeric(frame["low_hfq"], errors="coerce")
    factors = factors.copy()
    factors["date"] = pd.to_datetime(factors["date"])
    frame = frame.merge(
        factors[["stock_code", "date", "ma_20", "ma_60", "ma_120"]],
        on=["stock_code", "date"],
        how="left",
    )
    frame = frame.sort_values(["stock_code", "date"], kind="stable")
    grouped = frame.groupby("stock_code", sort=False, observed=True)
    frame["ret_1"] = grouped["close"].pct_change(fill_method=None)
    for name, window in LOOKBACKS.items():
        if name == "momentum_lookback":
            frame["close_lag_mom"] = grouped["close"].shift(window)
        elif name == "risk_lookback":
            frame["vol"] = (
                grouped["ret_1"].rolling(window).std().reset_index(level=0, drop=True)
            )
    # `min_periods=1` mirrors the engine, which slices whatever history exists
    # up to the lookback (`history_quotes[:breakout_lookback]`) rather than
    # requiring a full window.
    frame["range_high"] = (
        grouped["high"]
        .rolling(LOOKBACKS["breakout_lookback"], min_periods=1)
        .max()
        .reset_index(level=0, drop=True)
    )
    frame["range_low"] = (
        grouped["low"]
        .rolling(LOOKBACKS["breakout_lookback"], min_periods=1)
        .min()
        .reset_index(level=0, drop=True)
    )
    return frame


def components_at(
    features: pd.DataFrame, index_alpha: pd.Series | None = None
) -> pd.DataFrame:
    """Component **normalised** values (0..1) per row; penalty stays raw.

    ``index_alpha`` is a per-(date) CSI300 return series used for the real
    relative strength component; when absent that component falls back to the
    engine's self-proxy (own 10-session change).
    """
    frame = features.copy()
    close = frame["close"]

    # momentum: clamp(change / 0.1)
    change = (close / frame["close_lag_mom"]) - 1.0
    frame["momentum"] = (change / 0.1).clip(0.0, 1.0)

    # trend alignment: share of MA checks true
    checks = pd.DataFrame(index=frame.index)
    checks["c1"] = close > frame["ma_20"]
    checks["c2"] = close > frame["ma_60"]
    checks["c3"] = frame["ma_20"] > frame["ma_60"]
    frame["trend_alignment"] = checks.mean(axis=1)

    # breakout / position
    span = frame["range_high"] - frame["range_low"]
    position = ((close - frame["range_low"]) / span.where(span > 0)).clip(0.0, 1.0)
    # engine parity: `close >= range_high` is a new high and pins position to 1.0
    frame["breakout_or_position"] = position.where(close < frame["range_high"], 1.0)

    # relative strength: clamp((change + 0.05) / 0.15)
    frame["relative_strength"] = ((change + 0.05) / 0.15).clip(0.0, 1.0)

    # real relative strength: CSI300 alpha if available, else self-proxy
    if index_alpha is not None:
        alpha = change - frame["date"].map(index_alpha)
        frame["real_relative_strength"] = ((alpha + 0.05) / 0.15).clip(0.0, 1.0)
    else:
        frame["real_relative_strength"] = frame["relative_strength"]

    # risk penalty stays a raw penalty: stdev / 0.06, +1 when untradeable
    raw = frame["vol"].fillna(0.0) / 0.06

    def _flag(column: str, default) -> pd.Series:
        value = (
            frame[column]
            if column in frame.columns
            else pd.Series(default, index=frame.index)
        )
        return pd.to_numeric(value, errors="coerce").fillna(default)

    abnormal = (_flag("trade_status", 1) != 1) | (_flag("is_st", 0) == 1)
    frame["risk_penalty"] = raw + abnormal.astype(float)
    return frame


def flip_score(components: pd.DataFrame) -> pd.Series:
    """Weighted, direction-applied score for the flip side.

    Every available alpha component is negated (construction-layer reversal)
    and the penalty keeps its subtractive sign, mirroring the
    ``flip_wide_shadow_v1`` registry entry. Weights are renormalised over the
    components actually present, so dropping `signal_strength`/`industry_momentum`
    changes the scale but not the ordering within a date.
    """
    total = sum(WEIGHTS[name] for name in WEIGHTS if name in components.columns)
    if total <= 0:
        raise ValueError("no component columns available")
    score = pd.Series(0.0, index=components.index)
    for name, weight in WEIGHTS.items():
        if name not in components.columns:
            continue
        scaled = components[name] * weight / total
        # Alpha components flip (construction-layer reversal); the risk penalty
        # keeps its standard subtractive sign.
        score += -scaled if name != "risk_penalty" else -scaled
    return score


def cross_sectional_rank(frame: pd.DataFrame, score: pd.Series) -> pd.Series:
    """Per-session percentile of ``score`` (1.0 = best for the flip side)."""
    return score.groupby(frame["date"], sort=False).rank(method="average", pct=True)


def volatility_of(returns) -> float:
    """Kept for parity checks against the engine's penalty definition."""
    clean = [value for value in returns if value is not None and value == value]
    return pstdev(clean) if len(clean) > 1 else 0.0
