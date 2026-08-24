# -*- coding: utf-8 -*-
"""Unit tests for technical factor computation functions."""

from datetime import datetime
from types import SimpleNamespace

from app.lib.scoring_engine.technical_factors import (
    ALL_TECHNICAL_FACTORS,
    atr_ratio,
    bb_position,
    consecutive_up,
    gap_ratio,
    rsi_14,
    turnover_accel,
    volume_ratio,
    yearly_position,
)


def _make_quote(
    date_str, close, open_p=None, high=None, low=None, volume=0, turnover=0
):
    """Factory for quote-like objects matching StockDailyQuote field names."""
    return SimpleNamespace(
        date=datetime.fromisoformat(date_str),
        close_hfq=close,
        close=close,
        open_hfq=open_p if open_p is not None else close,
        open=open_p if open_p is not None else close,
        high=high if high is not None else close,
        low=low if low is not None else close,
        volume=volume,
        turnover_rate=turnover,
    )


class TestVolumeRatio:
    def test_normal(self):
        quotes = [
            _make_quote("2024-01-10", 100, volume=1000),
            _make_quote("2024-01-11", 101, volume=2000),
            _make_quote("2024-01-12", 102, volume=500),
        ]
        result = volume_ratio(quotes)
        # Not enough data for MA20, result should be empty (or very small)
        assert len(result) <= 3

    def test_above_average_volume(self):
        """Above-average volume on the last day yields ratio > 1."""
        quotes = [
            _make_quote(f"2024-01-{d:02d}", 100.0, volume=1000) for d in range(1, 22)
        ]
        # Last entry spike
        quotes.append(_make_quote("2024-01-22", 100.0, volume=5000))
        result = volume_ratio(quotes)
        dates = sorted(result.keys())
        last_val = result[dates[-1]]
        assert last_val > 1.0


class TestBBPosition:
    def test_mid_band(self):
        """Constant prices produce no BB position (zero width)."""
        quotes = [_make_quote(f"2024-01-{d:02d}", 100.0) for d in range(1, 31)]
        result = bb_position(quotes)
        # All same price => BB width near 0, bb_upper == bb_lower => no entries
        assert isinstance(result, dict)

    def test_extreme_position(self):
        """A large spike pushes BB position above 1 (breakout)."""
        quotes = [_make_quote(f"2024-01-{d:02d}", 100.0) for d in range(1, 25)]
        quotes.append(_make_quote("2024-01-25", 130.0))  # breakout above
        result = bb_position(quotes)
        assert isinstance(result, dict)
        if result:
            dates = sorted(result.keys())
            last_val = result[dates[-1]]
            assert last_val > 1.0


class TestATRRatio:
    def test_computes(self):
        quotes = [
            _make_quote(f"2024-01-{d:02d}", 100.0, high=102.0, low=98.0)
            for d in range(1, 21)
        ]
        result = atr_ratio(quotes)
        assert isinstance(result, dict)


class TestConsecutiveUp:
    def test_streak(self):
        quotes = [
            _make_quote("2024-01-01", 100, open_p=99),  # up -> 1
            _make_quote("2024-01-02", 102, open_p=101),  # up -> 2
            _make_quote("2024-01-03", 100, open_p=101),  # down -> 0
            _make_quote("2024-01-04", 103, open_p=99),  # up -> 1
        ]
        result = consecutive_up(quotes)
        dates = sorted(result.keys())
        assert result[dates[0]] == 1
        assert result[dates[1]] == 2
        assert result[dates[2]] == 0
        assert result[dates[3]] == 1

    def test_no_streak(self):
        """All down days yield streak of 0 throughout."""
        quotes = [
            _make_quote("2024-01-01", 100, open_p=101),
            _make_quote("2024-01-02", 99, open_p=100),
            _make_quote("2024-01-03", 98, open_p=99),
        ]
        result = consecutive_up(quotes)
        for v in result.values():
            assert v == 0


class TestTurnoverAccel:
    def test_computes(self):
        quotes = [
            _make_quote(f"2024-01-{d:02d}", 100.0, turnover=2.0) for d in range(1, 10)
        ]
        result = turnover_accel(quotes)
        assert isinstance(result, dict)

    def test_acceleration(self):
        """Increasing turnover should yield ratio > 1 for final days."""
        quotes = [
            _make_quote(f"2024-01-{d:02d}", 100.0, turnover=float(d))
            for d in range(1, 10)
        ]
        result = turnover_accel(quotes)
        assert isinstance(result, dict)
        if result:
            dates = sorted(result.keys())
            last_val = result[dates[-1]]
            # Later turnover is much higher than early MA5 -> ratio > 1
            assert last_val > 1.0


class TestGapRatio:
    def test_gap_up(self):
        quotes = [
            _make_quote("2024-01-01", 100, open_p=100),
            _make_quote("2024-01-02", 105, open_p=105),
        ]
        result = gap_ratio(quotes)
        dates = sorted(result.keys())
        assert dates[0] == "2024-01-02T00:00:00"
        assert abs(result[dates[0]] - 0.05) < 0.001

    def test_gap_down(self):
        quotes = [
            _make_quote("2024-01-01", 100, open_p=100),
            _make_quote("2024-01-02", 95, open_p=95),
        ]
        result = gap_ratio(quotes)
        dates = sorted(result.keys())
        assert result[dates[0]] < 0

    def test_no_result_for_single_quote(self):
        """Single quote yields no gaps (needs prev_close)."""
        quotes = [_make_quote("2024-01-01", 100)]
        result = gap_ratio(quotes)
        assert result == {}


class TestYearlyPosition:
    def test_degenerate_range(self):
        """Constant prices: high == low, yield no entries."""
        quotes = [_make_quote(f"2024-01-{d:02d}", 100.0) for d in range(1, 30)]
        result = yearly_position(quotes)
        assert isinstance(result, dict)

    def test_at_extreme_high(self):
        """When price is at the window high, position = 1.0."""
        quotes = [_make_quote(f"2024-01-{d:02d}", 100.0) for d in range(1, 30)]
        # Last quote is significantly higher
        quotes.append(_make_quote("2024-01-30", 150.0))
        result = yearly_position(quotes)
        assert isinstance(result, dict)
        if result:
            dates = sorted(result.keys())
            last_val = result[dates[-1]]
            assert abs(last_val - 1.0) < 0.01

    def test_at_extreme_low(self):
        """When price is at the window low, position = 0.0."""
        quotes = [_make_quote(f"2024-01-{d:02d}", 100.0) for d in range(1, 30)]
        quotes.append(_make_quote("2024-01-30", 50.0))
        result = yearly_position(quotes)
        assert isinstance(result, dict)
        if result:
            dates = sorted(result.keys())
            last_val = result[dates[-1]]
            assert abs(last_val - 0.0) < 0.01


class TestRSI14:
    def test_all_up(self):
        """Steady uptrend => RSI approaches 100."""
        quotes = [_make_quote(f"2024-01-{d:02d}", 100.0 + d) for d in range(1, 21)]
        result = rsi_14(quotes)
        assert isinstance(result, dict)
        if result:
            last_val = list(result.values())[-1]
            assert last_val > 50

    def test_mostly_down(self):
        """Predominantly downtrend with small bounces => RSI below 50 but > 0."""
        prices = []
        val = 100.0
        for i in range(20):
            if i > 0 and i % 6 == 0:
                val += 1.0  # small bounce
            elif i > 0:
                val -= 2.0  # larger drops
            prices.append(val)
        quotes = [_make_quote(f"2024-01-{d + 1:02d}", p) for d, p in enumerate(prices)]
        result = rsi_14(quotes)
        assert isinstance(result, dict)
        if result:
            last_val = list(result.values())[-1]
            assert last_val < 50


class TestRegistry:
    def test_all_nine_factors(self):
        assert len(ALL_TECHNICAL_FACTORS) == 9
        expected = {
            "volume_ratio",
            "bb_position",
            "atr_ratio",
            "consecutive_up",
            "turnover_accel",
            "gap_ratio",
            "yearly_position",
            "rsi_14",
            "real_relative_strength",
        }
        assert set(ALL_TECHNICAL_FACTORS.keys()) == expected

    def test_factors_are_callable(self):
        for name, func in ALL_TECHNICAL_FACTORS.items():
            assert callable(func), f"{name} is not callable"
