# -*- coding: utf-8 -*-
"""Technical factor computation functions.

Each function accepts a list of StockDailyQuote objects sorted by date ascending
and returns a dict mapping date.isoformat() -> float factor value.

Look-ahead-bias free: every function only uses data at or before the evaluation date.
"""

from statistics import mean


def _closing_price(quote) -> float | None:
    """Return HFQ-adjusted close, falling back to raw close."""
    price = quote.close_hfq or quote.close
    return float(price) if price else None


def volume_ratio(quotes: list, ma_window: int = 20) -> dict:
    """volume / MA20(volume) — measures trading interest intensity.

    Values > 1 indicate above-average volume (possible breakout/interest).
    Values < 1 indicate below-average volume (dull/sideways).
    """
    result = {}
    volumes = [getattr(q, "volume", 0) or 0 for q in quotes]

    for i, quote in enumerate(quotes):
        if i < ma_window - 1:
            continue
        window = volumes[max(0, i - ma_window + 1) : i + 1]
        ma = mean(window) if window else 0
        if ma > 0 and volumes[i] > 0:
            result[quote.date.isoformat()] = round(volumes[i] / ma, 6)
    return result


def bb_position(quotes: list, window: int = 20, num_std: float = 2.0) -> dict:
    """(close - BB_lower) / (BB_upper - BB_lower) — Bollinger Band position.

    Values: 0 = at lower band, 0.5 = at middle, 1.0 = at upper band, >1 = breakout.
    """
    from statistics import pstdev

    result = {}
    closes = [_closing_price(q) for q in quotes]

    for i, quote in enumerate(quotes):
        if i < window - 1:
            continue
        window_closes = [
            c for c in closes[max(0, i - window + 1) : i + 1] if c is not None
        ]
        if len(window_closes) < window // 2:
            continue

        ma = mean(window_closes)
        std = pstdev(window_closes) if len(window_closes) > 1 else 0
        bb_upper = ma + num_std * std
        bb_lower = ma - num_std * std

        if bb_upper > bb_lower and closes[i] is not None:
            position = (closes[i] - bb_lower) / (bb_upper - bb_lower)
            result[quote.date.isoformat()] = round(position, 6)
    return result


def atr_ratio(quotes: list, window: int = 14) -> dict:
    """ATR(14) / close — normalized volatility.

    Uses Wilder's smoothing: first ATR = mean of first N TR values,
    then ATR_t = (ATR_{t-1} * (N-1) + TR_t) / N.
    """
    result = {}
    highs = [getattr(q, "high", 0) or 0 for q in quotes]
    lows = [getattr(q, "low", 0) or 0 for q in quotes]
    closes = [_closing_price(q) for q in quotes]

    # True Range
    tr_values = []
    for i, _q in enumerate(quotes):
        if i == 0:
            tr = highs[i] - lows[i]
        else:
            high, low = highs[i], lows[i]
            prev_close = closes[i - 1] if closes[i - 1] else 0
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        tr_values.append(tr)

    # Wilder's ATR
    atr_values = []
    for i in range(len(quotes)):
        if i < window:
            atr_values.append(0)
        elif i == window:
            atr_values.append(mean(tr_values[1 : window + 1]))
        else:
            atr_values.append((atr_values[-1] * (window - 1) + tr_values[i]) / window)

    for i, quote in enumerate(quotes):
        if atr_values[i] > 0 and closes[i] and closes[i] > 0:
            result[quote.date.isoformat()] = round(atr_values[i] / closes[i], 6)
    return result


def consecutive_up(quotes: list) -> dict:
    """Number of consecutive days where close > open — trend persistence.

    Positive = uptrend streak, 0 = first down day.
    """
    result = {}
    streak = 0
    for i, quote in enumerate(quotes):
        close = _closing_price(quote)
        open_price = quote.open_hfq or quote.open
        if close is not None and open_price is not None and close > open_price:
            streak += 1
        else:
            streak = 0
        result[quote.date.isoformat()] = streak
    return result


def turnover_accel(quotes: list, ma_window: int = 5) -> dict:
    """turnover_rate / MA5(turnover_rate) — volume acceleration.

    Values > 1: turnover accelerating (increasing interest).
    Values < 1: turnover decelerating.
    """
    result = {}
    turnover_rates = [getattr(q, "turnover_rate", 0) or 0 for q in quotes]

    for i, quote in enumerate(quotes):
        if i < ma_window - 1:
            continue
        window = [r for r in turnover_rates[max(0, i - ma_window + 1) : i + 1] if r > 0]
        if len(window) < 2 or turnover_rates[i] <= 0:
            continue
        ma = mean(window)
        if ma > 0:
            result[quote.date.isoformat()] = round(turnover_rates[i] / ma, 6)
    return result


def gap_ratio(quotes: list) -> dict:
    """(open - prev_close) / prev_close — overnight gap strength.

    Positive: gap up (bullish). Negative: gap down (bearish).
    """
    result = {}
    for i, quote in enumerate(quotes):
        if i == 0:
            continue
        open_price = quote.open_hfq or quote.open
        prev_close = _closing_price(quotes[i - 1])
        if open_price is not None and prev_close and prev_close > 0:
            result[quote.date.isoformat()] = round(
                (open_price - prev_close) / prev_close, 6
            )
    return result


def yearly_position(quotes: list) -> dict:
    """(close - 52w_low) / (52w_high - 52w_low) — position within 52-week range.

    Approximates 52 weeks = ~250 trading days.
    0 = at 52-week low, 1 = at 52-week high.
    """
    result = {}
    window = 250
    closes = [_closing_price(q) for q in quotes]

    for i, quote in enumerate(quotes):
        if i < 20:  # minimum data
            continue
        start_idx = max(0, i - window)
        window_closes = [c for c in closes[start_idx : i + 1] if c is not None]
        if len(window_closes) < 20:
            continue

        high = max(window_closes)
        low = min(window_closes)
        if high > low and closes[i] is not None:
            result[quote.date.isoformat()] = round((closes[i] - low) / (high - low), 6)
    return result


def rsi_14(quotes: list, window: int = 14) -> dict:
    """Standard RSI using Wilder's smoothing.

    Values: >70 overbought, <30 oversold.
    """
    result = {}
    closes = [_closing_price(q) for q in quotes]

    # Compute price changes
    gains = []
    losses = []
    for i in range(1, len(closes)):
        if closes[i] is None or closes[i - 1] is None:
            gains.append(0)
            losses.append(0)
        else:
            change = closes[i] - closes[i - 1]
            gains.append(max(change, 0))
            losses.append(abs(min(change, 0)))

    # Wilder's smoothing
    rsi_values = [0] * len(quotes)
    if len(gains) >= window:
        avg_gain = mean(gains[:window])
        avg_loss = mean(losses[:window])
        if avg_loss > 0:
            rsi_values[window] = 100 - (100 / (1 + avg_gain / avg_loss))
        elif avg_gain > 0:
            rsi_values[window] = 100

        for i in range(window, len(gains)):
            avg_gain = (avg_gain * (window - 1) + gains[i]) / window
            avg_loss = (avg_loss * (window - 1) + losses[i]) / window
            if avg_loss > 0:
                rsi_values[i + 1] = 100 - (100 / (1 + avg_gain / avg_loss))
            elif avg_gain > 0:
                rsi_values[i + 1] = 100

    for i, quote in enumerate(quotes):
        if rsi_values[i] > 0:
            result[quote.date.isoformat()] = round(rsi_values[i], 4)
    return result


# Registry for easy iteration over all technical factor functions
ALL_TECHNICAL_FACTORS = {
    "volume_ratio": volume_ratio,
    "bb_position": bb_position,
    "atr_ratio": atr_ratio,
    "consecutive_up": consecutive_up,
    "turnover_accel": turnover_accel,
    "gap_ratio": gap_ratio,
    "yearly_position": yearly_position,
    "rsi_14": rsi_14,
}
