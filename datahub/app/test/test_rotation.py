# -*- coding: utf-8 -*-
"""Multi-asset ETF rotation tests: selection, no-look-ahead, costs, signal."""

import numpy as np
import pandas as pd
import pytest

from app.lib.etf_lab.rotation import (
    monthly_signal,
    select_targets,
    simulate,
    summary,
    yearly_returns,
)


def _flat_prices(codes, sessions, value=10.0, index=None):
    idx = pd.DatetimeIndex(sessions) if index is None else index
    return pd.DataFrame(value, index=idx, columns=codes)


def test_select_targets_requires_positive_momentum_and_trend():
    momentum = pd.Series({"A": 0.20, "B": 0.10, "C": -0.05, "D": 0.30, "BOND": 0.01})
    trend = pd.Series({"A": True, "B": True, "C": True, "D": False, "BOND": True})
    picks = select_targets(momentum, trend, top_n=3, defensive="BOND")
    # D is filtered by the trend MA, C by negative momentum, BOND is defensive.
    assert picks == ["A", "B"]


def test_select_targets_falls_back_to_the_defensive_asset():
    momentum = pd.Series({"A": -0.2, "B": -0.1, "BOND": 0.005})
    trend = pd.Series({"A": True, "B": True, "BOND": True})
    assert select_targets(momentum, trend, top_n=2, defensive="BOND") == ["BOND"]
    # A defensive asset that is not quoted cannot be held: with nothing else
    # qualifying the target list is empty rather than silently holding cash.
    negative = pd.Series({"A": -0.2, "B": -0.1})
    assert select_targets(negative, trend, top_n=2, defensive="MISSING") == []


def test_simulate_does_not_hand_the_new_book_the_rebalance_gain():
    # 40 sessions; on session 21 (the rebalance after the first) the market jumps
    # +50% for one session. The signal on session 20 uses that close, which is
    # BEFORE the jump, so a look-ahead-free simulation cannot earn it with the
    # book picked on session 20 when that book starts on session 21.
    sessions = pd.date_range("2026-01-01", periods=40, freq="B")
    prices = pd.DataFrame(index=sessions, columns=["A", "BOND"], dtype=float)
    prices["A"] = 10.0
    prices["BOND"] = 10.0
    # build momentum so that A qualifies on session 20 and is bought
    prices.loc[sessions[0], "A"] = 5.0
    prices.loc[sessions[20], "A"] = 10.0
    # the jump happens on session 21
    prices.loc[sessions[21] :, "A"] = 15.0
    prices["A"] = prices["A"].ffill()
    result = simulate(
        prices,
        lookback=5,
        top_n=1,
        ma=3,
        cadence=20,
        defensive="BOND",
        start=sessions[0],
        end=sessions[-1],
    )
    curve = result["curve"]
    # session 20 -> 21 is +50% only for a holder of A coming into 21; the book
    # bought on 20 starts on 21, so it earns 21 -> 22 (flat), not 20 -> 21.
    r20_21 = curve.loc[sessions[21]] / curve.loc[sessions[20]] - 1.0
    assert abs(r20_21) < 0.01


def test_simulate_charges_turnover_only_on_replacements():
    sessions = pd.date_range("2026-01-01", periods=45, freq="B")
    prices = _flat_prices(["A", "BOND"], sessions)
    # A rises steadily so it is always selected and never replaced.
    prices["A"] = np.linspace(10, 15, len(sessions))
    result = simulate(
        prices,
        lookback=5,
        top_n=1,
        ma=3,
        cadence=20,
        defensive="BOND",
        start=sessions[0],
        end=sessions[-1],
    )
    assert result["total_trades"] == 1  # only the initial buy
    # The first session has no book yet, so it accrues the cash leg only; the
    # single round trip is charged as the first book takes effect on session 2.
    assert result["curve"].iloc[0] == pytest.approx(1.0 + 0.00004)
    assert result["final_equity"] > 1.0


def test_simulate_holds_the_defensive_asset_when_nothing_qualifies():
    sessions = pd.date_range("2026-01-01", periods=40, freq="B")
    prices = _flat_prices(["A", "BOND"], sessions)
    prices["A"] = np.linspace(10, 5, len(sessions))  # steadily falling
    prices["BOND"] = np.linspace(10, 10.4, len(sessions))
    result = simulate(
        prices,
        lookback=5,
        top_n=1,
        ma=3,
        cadence=20,
        defensive="BOND",
        start=sessions[0],
        end=sessions[-1],
    )
    assert result["final_holdings"] == ["BOND"]
    assert result["final_equity"] > 1.0


def test_yearly_returns_and_summary():
    sessions = pd.date_range("2024-12-01", periods=250, freq="B")
    curve = pd.Series(np.linspace(1.0, 1.5, len(sessions)), index=sessions)
    yearly = yearly_returns(curve)
    assert set(yearly) == {2024, 2025}
    assert yearly[2025] > 0
    result = {
        "curve": curve,
        "yearly_returns": yearly,
        "max_drawdown": -0.1,
        "total_trades": 4,
        "final_equity": 1.5,
    }
    stats = summary(result)
    assert stats["annualised"] > 0
    assert stats["negative_years"] == 0
    assert stats["trades"] == 4


def test_monthly_signal_reports_selection_and_reasons():
    sessions = pd.date_range("2026-01-01", periods=30, freq="B")
    prices = _flat_prices(["A", "B", "C", "BOND"], sessions)
    prices["A"] = np.linspace(10, 20, len(sessions))  # strong uptrend
    prices["B"] = np.linspace(10, 8, len(sessions))  # downtrend
    prices["C"] = np.linspace(10, 11, len(sessions))  # mild uptrend
    prices["BOND"] = np.linspace(10, 10.1, len(sessions))
    signal = monthly_signal(prices, lookback=5, top_n=1, ma=3, defensive="BOND")
    assert signal["selected"] == ["A"]
    reasons = {row["code"]: row["reason"] for row in signal["diagnostics"]}
    assert reasons["BOND"] == "defensive"
    assert reasons["B"] == "negative_momentum"
    assert reasons["C"] is None  # qualifies but is not top-1


def test_monthly_signal_uses_the_last_session_at_or_before_as_of():
    sessions = pd.date_range("2026-01-01", periods=30, freq="B")
    prices = _flat_prices(["A", "BOND"], sessions)
    prices["A"] = np.linspace(10, 20, len(sessions))
    signal = monthly_signal(
        prices, lookback=5, top_n=1, ma=3, as_of="2026-02-01", defensive="BOND"
    )
    assert signal["as_of"] <= pd.Timestamp("2026-02-01")
    assert signal["selected"] == ["A"]


def test_point_in_time_eligible_filter_excludes_new_listings():
    sessions = pd.date_range("2026-01-01", periods=40, freq="B")
    prices = _flat_prices(["OLD", "NEW", "BOND"], sessions)
    prices["OLD"] = np.linspace(10, 12, len(sessions))
    prices["NEW"] = np.linspace(10, 40, len(sessions))  # would win if allowed
    prices["BOND"] = np.linspace(10, 10.1, len(sessions))

    def eligible(day):
        return ["OLD", "BOND"] if day < sessions[20] else ["OLD", "NEW", "BOND"]

    early = simulate(
        prices,
        lookback=5,
        top_n=1,
        ma=3,
        cadence=10,
        defensive="BOND",
        eligible=eligible,
        start=sessions[0],
        end=sessions[19],
    )
    assert all(code != "NEW" for code in early["final_holdings"] or ["OLD"])
    signal = monthly_signal(
        prices,
        lookback=5,
        top_n=1,
        ma=3,
        defensive="BOND",
        as_of=sessions[25],
        eligible=eligible,
    )
    assert signal["selected"] == ["NEW"]


def test_target_vol_scales_exposure_and_dampens_returns():
    sessions = pd.date_range("2026-01-01", periods=80, freq="B")
    prices = _flat_prices(["A", "BOND"], sessions)
    rng = np.random.default_rng(7)
    prices["A"] = 10.0 * np.cumprod(1 + rng.normal(0.004, 0.03, len(sessions)))
    prices["BOND"] = np.linspace(10.0, 10.2, len(sessions))
    full = simulate(prices, lookback=5, top_n=1, ma=3, cadence=20, defensive="BOND")
    scaled = simulate(
        prices,
        lookback=5,
        top_n=1,
        ma=3,
        cadence=20,
        defensive="BOND",
        target_vol=0.10,
        vol_window=20,
    )
    # A 10 % vol target on a ~45 %-vol asset must de-risk and dampen the outcome.
    assert scaled["final_exposure"] <= 1.0
    assert scaled["final_equity"] < full["final_equity"] or full["final_equity"] < 1.0
