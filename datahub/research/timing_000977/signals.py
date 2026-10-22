# -*- coding: utf-8 -*-
"""Causal feature set and strategy builders for single-asset timing.

Every builder returns a target-exposure ``Series`` indexed like ``frame`` and
valued in ``[0, 1]``.  The value at date ``t`` may only use bars up to and
including ``t``; execution is shifted to the next open by ``engine.simulate``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import numpy as np
import pandas as pd

TRADING_DAYS = 244.0


# --------------------------------------------------------------------------- #
# Features
# --------------------------------------------------------------------------- #
def add_features(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.copy()
    close = df["close"]
    df["ret1"] = close.pct_change()
    for n in (5, 10, 20, 40, 60, 120, 200, 250):
        df[f"ma{n}"] = close.rolling(n, min_periods=n).mean()
        df[f"ret{n}"] = close.pct_change(n)
        df[f"vol{n}"] = df["ret1"].rolling(n, min_periods=n).std() * np.sqrt(
            TRADING_DAYS
        )
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - close.shift()).abs(),
            (df["low"] - close.shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr14"] = tr.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    df["atr_pct"] = df["atr14"] / close
    return df


def _as_exposure(values: np.ndarray, index: pd.Index) -> pd.Series:
    return pd.Series(values, index=index, dtype=float).fillna(0.0)


# --------------------------------------------------------------------------- #
# Families
# --------------------------------------------------------------------------- #
def ma_trend(df: pd.DataFrame, n: int = 60) -> pd.Series:
    """Long while close is above an n-day moving average."""
    ma = df[f"ma{n}"] if f"ma{n}" in df else df["close"].rolling(n).mean()
    return _as_exposure(
        (df["close"].to_numpy() > ma.to_numpy()).astype(float), df.index
    )


def dual_ma(df: pd.DataFrame, fast: int = 20, slow: int = 60) -> pd.Series:
    f = df["close"].rolling(fast).mean().to_numpy()
    s = df["close"].rolling(slow).mean().to_numpy()
    return _as_exposure((f > s).astype(float), df.index)


def momentum(df: pd.DataFrame, n: int = 60) -> pd.Series:
    r = df["close"].pct_change(n).to_numpy()
    return _as_exposure((r > 0).astype(float), df.index)


def donchian(df: pd.DataFrame, entry_n: int = 55, exit_n: int = 20) -> pd.Series:
    """Breakout above the prior entry_n-day high, exit below the exit_n-day low."""
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    close = df["close"].to_numpy(dtype=float)
    n = len(close)
    out = np.zeros(n)
    state = 0
    for i in range(n):
        if i >= entry_n + 1:
            entry_hi = high[i - entry_n : i].max()  # excludes today (causal)
        else:
            entry_hi = np.inf
        if i >= exit_n + 1:
            exit_lo = low[i - exit_n : i].min()
        else:
            exit_lo = -np.inf
        if state == 0 and close[i] > entry_hi:
            state = 1
        elif state == 1 and close[i] < exit_lo:
            state = 0
        out[i] = state
    return _as_exposure(out, df.index)


def trend_stop(
    df: pd.DataFrame,
    n: int = 60,
    stop: float = 0.15,
    use_atr: bool = False,
    atr_mult: float = 3.0,
) -> pd.Series:
    """Trend gate plus a trailing stop measured from the highest close since entry."""
    close = df["close"].to_numpy(dtype=float)
    ma = (
        df[f"ma{n}"].to_numpy(dtype=float)
        if f"ma{n}" in df
        else df["close"].rolling(n).mean().to_numpy()
    )
    atr = df["atr14"].to_numpy(dtype=float)
    n_bars = len(close)
    out = np.zeros(n_bars)
    state = 0
    peak = -np.inf
    for i in range(n_bars):
        if not np.isfinite(ma[i]):
            out[i] = 0.0
            continue
        above = close[i] > ma[i]
        if state == 1:
            peak = max(peak, close[i])
            trail = atr_mult * atr[i] if use_atr else peak * stop
            floor = peak - trail if use_atr else peak * (1.0 - stop)
            if close[i] < floor:
                state = 0
        if state == 0 and above:
            state = 1
            peak = close[i]
        out[i] = state
    return _as_exposure(out, df.index)


def vol_target(df: pd.DataFrame, target: float = 0.25, n: int = 20) -> pd.Series:
    """Constant-risk sizing: exposure = target vol / realised vol, capped at 1."""
    vol = df[f"vol{n}"].to_numpy(dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        expo = np.where(
            np.isfinite(vol) & (vol > 0), np.minimum(1.0, target / vol), 0.0
        )
    return _as_exposure(expo, df.index)


def trend_vol(
    df: pd.DataFrame,
    n: int = 60,
    target: float = 0.25,
    vol_n: int = 20,
    floor: float = 0.0,
) -> pd.Series:
    """Trend gate scaled by inverse volatility; ``floor`` keeps a minimum sleeve."""
    trend = ma_trend(df, n).to_numpy()
    scaled = vol_target(df, target, vol_n).to_numpy()
    expo = np.where(trend > 0, np.maximum(scaled, floor), 0.0)
    return _as_exposure(np.minimum(expo, 1.0), df.index)


def rsi(df: pd.DataFrame, n: int = 14, low: float = 30, high: float = 70) -> pd.Series:
    """Mean reversion: buy oversold, hold until overbought."""
    close = df["close"]
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi_val = (100 - 100 / (1 + rs)).to_numpy()
    out = np.zeros(len(rsi_val))
    state = 0
    for i, val in enumerate(rsi_val):
        if not np.isfinite(val):
            continue
        if state == 0 and val < low:
            state = 1
        elif state == 1 and val > high:
            state = 0
        out[i] = state
    return _as_exposure(out, df.index)


def market_gate(
    df: pd.DataFrame, index_frame: pd.DataFrame, n: int = 200, mode: str = "ma"
) -> pd.Series:
    """Risk-on only while the broad index is above its n-day MA (or positive n-day return)."""
    px = index_frame["close"].reindex(df.index).ffill()
    if mode == "ma":
        ref = px.rolling(n, min_periods=n).mean()
        gate = (px > ref).to_numpy()
    else:
        gate = (px.pct_change(n) > 0).to_numpy()
    return _as_exposure(gate.astype(float), df.index)


# --------------------------------------------------------------------------- #
# Registry used by the scanner
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class StrategySpec:
    name: str
    build: Callable[..., pd.Series]
    params: dict


def vol_regime(
    df: pd.DataFrame, n: int = 20, lookback: int = 750, q: float = 0.6
) -> pd.Series:
    """Hold only while realised volatility sits in the calm part of its own history."""
    vol = df[f"vol{n}"]
    thresh = vol.rolling(lookback, min_periods=250).quantile(q)
    gate = (vol <= thresh).to_numpy()
    return _as_exposure(gate.astype(float), df.index)


def dist_reversal(
    df: pd.DataFrame, n: int = 200, entry: float = 0.15, exit_: float = 0.05
) -> pd.Series:
    """Contrarian: buy stretched-below-trend, sell when the gap has closed."""
    dist = (df["close"] / df[f"ma{n}"] - 1).to_numpy()
    out = np.zeros(len(dist))
    state = 0
    for i, d in enumerate(dist):
        if not np.isfinite(d):
            continue
        if state == 0 and d < -entry:
            state = 1
        elif state == 1 and d > exit_:
            state = 0
        out[i] = state
    return _as_exposure(out, df.index)


def ret_reversal(
    df: pd.DataFrame, n: int = 250, entry: float = 0.30, exit_: float = 0.05
) -> pd.Series:
    """Contrarian on trailing return: buy after a long slide, sell after recovery."""
    r = df["close"].pct_change(n).to_numpy()
    out = np.zeros(len(r))
    state = 0
    for i, v in enumerate(r):
        if not np.isfinite(v):
            continue
        if state == 0 and v < -entry:
            state = 1
        elif state == 1 and v > exit_:
            state = 0
        out[i] = state
    return _as_exposure(out, df.index)


def rsi_trend(
    df: pd.DataFrame, ma_n: int = 200, low: float = 35, high: float = 65
) -> pd.Series:
    """Oversold entry, but only inside a long-term uptrend."""
    base = rsi(df, 14, low, high).to_numpy()
    trend = (df["close"] > df[f"ma{ma_n}"]).to_numpy()
    return _as_exposure((base * trend), df.index)


def crash_rebound(
    df: pd.DataFrame, drop: float = 0.25, n: int = 20, hold: int = 20
) -> pd.Series:
    """Buy a sharp n-day drawdown and time-stop out after ``hold`` bars."""
    r = df["close"].pct_change(n).to_numpy()
    out = np.zeros(len(r))
    left = 0
    for i, v in enumerate(r):
        if left > 0:
            out[i] = 1.0
            left -= 1
            continue
        if np.isfinite(v) and v < -drop:
            out[i] = 1.0
            left = hold
    return _as_exposure(out, df.index)


def combine(*series: pd.Series, mode: str = "min") -> pd.Series:
    frame = pd.concat(series, axis=1).fillna(0.0)
    arr = frame.to_numpy()
    if mode == "min":
        out = arr.min(axis=1)
    elif mode == "max":
        out = arr.max(axis=1)
    else:
        out = arr.mean(axis=1)
    return pd.Series(out, index=series[0].index)


def registry() -> list[StrategySpec]:
    specs: list[StrategySpec] = []
    for n in (10, 20, 30, 40, 50, 60, 80, 100, 120, 150, 200, 250):
        specs.append(StrategySpec(f"ma{n}", ma_trend, {"n": n}))
    for fast, slow in (
        (5, 20),
        (10, 30),
        (10, 60),
        (20, 60),
        (20, 120),
        (50, 120),
        (60, 200),
    ):
        specs.append(
            StrategySpec(f"dual{fast}_{slow}", dual_ma, {"fast": fast, "slow": slow})
        )
    for n in (10, 20, 40, 60, 120, 250):
        specs.append(StrategySpec(f"mom{n}", momentum, {"n": n}))
    for entry, exit_ in ((20, 10), (20, 20), (55, 20), (55, 55), (120, 60)):
        specs.append(
            StrategySpec(
                f"donch{entry}_{exit_}", donchian, {"entry_n": entry, "exit_n": exit_}
            )
        )
    for n in (20, 40, 60, 120, 200):
        for stop in (0.10, 0.15, 0.20, 0.25):
            specs.append(
                StrategySpec(
                    f"stop{n}_{int(stop * 100)}", trend_stop, {"n": n, "stop": stop}
                )
            )
    for n in (40, 60, 120):
        for mult in (2.0, 3.0, 4.0):
            specs.append(
                StrategySpec(
                    f"atrstop{n}_{int(mult * 10)}",
                    trend_stop,
                    {"n": n, "use_atr": True, "atr_mult": mult},
                )
            )
    for target in (0.15, 0.20, 0.25, 0.30, 0.40):
        specs.append(
            StrategySpec(f"vol{int(target * 100)}", vol_target, {"target": target})
        )
    for n in (40, 60, 120):
        for target in (0.20, 0.25, 0.30):
            specs.append(
                StrategySpec(
                    f"tv{n}_{int(target * 100)}", trend_vol, {"n": n, "target": target}
                )
            )
    specs.append(StrategySpec("rsi14", rsi, {}))
    return specs
