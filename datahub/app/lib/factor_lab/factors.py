# -*- coding: utf-8 -*-
"""Vectorised factor registry for the factor lab.

Every factor is expressed with pandas on the panel's own columns, using only
backward-looking windows (`shift(k)` with k > 0, trailing `rolling`), so no
factor can see a future bar. That is the whole point of keeping them here rather
than reusing `scoring_engine/technical_factors.py`, whose functions are
per-stock Python loops over hydrated quotes — fine for the scoring path,
hopeless for sweeping hundreds of variants over eight million rows.

Adding a factor = adding one function here; `REGISTRY` is the CLI's menu.
"""

import pandas as pd


def _price(panel: pd.DataFrame) -> pd.Series:
    """Evaluation price series: HFQ close, falling back to raw close."""
    hfq = pd.to_numeric(panel["close_hfq"], errors="coerce")
    raw = pd.to_numeric(panel["close"], errors="coerce")
    return hfq.where(hfq > 0, raw)


def _by_stock(panel: pd.DataFrame, series: pd.Series) -> pd.Series:
    """``series`` grouped per stock, in panel row order (panel is date-sorted).

    ``observed=True`` because ``stock_code`` is a categorical once the panel is
    loaded for evaluation — without it pandas warns and keeps empty categories,
    which is slower and would inflate the group count.
    """
    return series.groupby(panel["stock_code"], sort=False, observed=True)


def _returns(panel: pd.DataFrame) -> pd.Series:
    return _by_stock(panel, _price(panel)).pct_change(fill_method=None)


def momentum(panel: pd.DataFrame, window: int) -> pd.Series:
    price = _price(panel)
    return price / _by_stock(panel, price).shift(window) - 1.0


def reversal(panel: pd.DataFrame, window: int) -> pd.Series:
    return -momentum(panel, window)


def volatility(panel: pd.DataFrame, window: int) -> pd.Series:
    returns = _returns(panel)
    return (
        _by_stock(panel, returns).rolling(window).std().reset_index(level=0, drop=True)
    )


def amihud(panel: pd.DataFrame, window: int) -> pd.Series:
    """Mean |return| per unit of turnover — a standard illiquidity proxy."""
    amount = pd.to_numeric(panel["trade_amount"], errors="coerce")
    returns = _returns(panel).abs()
    scaled = returns / amount.where(amount > 0) * 1e9
    return (
        _by_stock(panel, scaled).rolling(window).mean().reset_index(level=0, drop=True)
    )


def volume_ratio(panel: pd.DataFrame, window: int) -> pd.Series:
    volume = pd.to_numeric(panel["volume"], errors="coerce")
    grouped = _by_stock(panel, volume)
    average = grouped.rolling(window).mean().reset_index(level=0, drop=True)
    return volume / average.where(average > 0)


def volume_zscore(panel: pd.DataFrame, window: int) -> pd.Series:
    """How unusual today's volume is versus its own trailing distribution."""
    volume = pd.to_numeric(panel["volume"], errors="coerce")
    grouped = _by_stock(panel, volume)
    mean = grouped.rolling(window).mean().reset_index(level=0, drop=True)
    std = grouped.rolling(window).std().reset_index(level=0, drop=True)
    return (volume - mean) / std.where(std > 0)


def gap(panel: pd.DataFrame) -> pd.Series:
    """Overnight gap: today's open versus the previous session's close."""
    price = _price(panel)
    open_hfq = pd.to_numeric(panel["open_hfq"], errors="coerce")
    open_raw = pd.to_numeric(panel["open"], errors="coerce")
    open_price = open_hfq.where(open_hfq > 0, open_raw)
    previous = _by_stock(panel, price).shift(1)
    return open_price / previous.where(previous > 0) - 1.0


def range_position(panel: pd.DataFrame, window: int) -> pd.Series:
    """Where today's close sits in the trailing high-low range (0..1)."""
    price = _price(panel)
    high = pd.to_numeric(panel["high_hfq"], errors="coerce")
    low = pd.to_numeric(panel["low_hfq"], errors="coerce")
    highest = (
        _by_stock(panel, high).rolling(window).max().reset_index(level=0, drop=True)
    )
    lowest = _by_stock(panel, low).rolling(window).min().reset_index(level=0, drop=True)
    span = highest - lowest
    return (price - lowest) / span.where(span > 0)


def trend(panel: pd.DataFrame, window: int) -> pd.Series:
    """Distance above the trailing moving average."""
    price = _price(panel)
    average = (
        _by_stock(panel, price).rolling(window).mean().reset_index(level=0, drop=True)
    )
    return price / average.where(average > 0) - 1.0


def rsi(panel: pd.DataFrame, window: int = 14) -> pd.Series:
    """Wilder RSI, trailing only."""
    delta = _returns(panel)
    gains = delta.clip(lower=0)
    losses = (-delta).clip(lower=0)
    average_gain = (
        _by_stock(panel, gains)
        .ewm(alpha=1.0 / window, adjust=False, min_periods=window)
        .mean()
        .reset_index(level=0, drop=True)
    )
    average_loss = (
        _by_stock(panel, losses)
        .ewm(alpha=1.0 / window, adjust=False, min_periods=window)
        .mean()
        .reset_index(level=0, drop=True)
    )
    relative = average_gain / average_loss.where(average_loss > 0)
    return 100 - 100 / (1 + relative)


#: name -> (callable(panel) -> Series, description)
REGISTRY = {
    "momentum_3": (lambda p: momentum(p, 3), "3-session price momentum"),
    "momentum_10": (lambda p: momentum(p, 10), "10-session price momentum"),
    "momentum_20": (lambda p: momentum(p, 20), "20-session price momentum"),
    "momentum_60": (lambda p: momentum(p, 60), "60-session price momentum"),
    "reversal_1": (lambda p: reversal(p, 1), "1-session reversal"),
    "reversal_5": (lambda p: reversal(p, 5), "5-session reversal"),
    "reversal_10": (lambda p: reversal(p, 10), "10-session reversal"),
    "volatility_20": (lambda p: volatility(p, 20), "20-session return volatility"),
    "volatility_60": (lambda p: volatility(p, 60), "60-session return volatility"),
    "amihud_20": (lambda p: amihud(p, 20), "20-session Amihud illiquidity"),
    "volume_ratio_20": (lambda p: volume_ratio(p, 20), "volume / 20-session average"),
    "volume_zscore_60": (
        lambda p: volume_zscore(p, 60),
        "volume z-score vs 60 sessions",
    ),
    "gap_1": (gap, "overnight gap (open vs previous close)"),
    "range_position_20": (
        lambda p: range_position(p, 20),
        "close position in the 20-session range",
    ),
    "trend_20": (lambda p: trend(p, 20), "distance above the 20-session average"),
    "trend_60": (lambda p: trend(p, 60), "distance above the 60-session average"),
    "rsi_14": (rsi, "14-session RSI (Wilder)"),
}


def compute(panel: pd.DataFrame, name: str) -> pd.Series:
    """Factor values for ``name``, aligned to the panel's row order."""
    if name not in REGISTRY:
        raise KeyError(f"unknown factor {name!r}; known: {', '.join(sorted(REGISTRY))}")
    function, _ = REGISTRY[name]
    values = function(panel)
    if not isinstance(values, pd.Series):
        values = pd.Series(values, index=panel.index)
    return values.reindex(panel.index)


def with_factor(panel: pd.DataFrame, name: str) -> pd.DataFrame:
    """Panel copy carrying ``name`` as a column (kept for reuse across horizons)."""
    frame = panel.copy()
    frame[name] = compute(panel, name)
    return frame
