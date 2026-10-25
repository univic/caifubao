"""Opt-in daily marks never execute phantom sell orders on non-rebalance days."""

import pytest

from app.lib.strategy_engine.nav import QuoteView, simulate_paper_nav

D1, D2, D3 = "2026-10-22", "2026-10-23", "2026-10-26"
ZERO_COSTS = {
    "commission_rate": 0,
    "minimum_commission_cny": 0,
    "sell_stamp_duty_rate": 0,
    "transfer_fee_rate": 0,
    "slippage_per_side": 0,
}


def _project(prices, valuation_dates=None):
    return simulate_paper_nav(
        prices=prices,
        schedule=[{"date": D1, "holdings": {"sh600000": 0.5}}],
        initial_nav=10000,
        execution=ZERO_COSTS,
        valuation_dates=valuation_dates,
    )


def _prices():
    return {
        "sh600000": {
            D1: QuoteView(10, 10, previous_close=10),
            D2: QuoteView(10.5, 11, previous_close=10),
            D3: QuoteView(11, 12, previous_close=11),
        }
    }


def test_daily_valuation_only_marks_stock_without_phantom_rebalance():
    result = _project(_prices(), [D1, D2, D3])
    assert [point["date"] for point in result["curve"]] == [D1, D2, D3]
    assert [point["valuation_only"] for point in result["curve"]] == [
        False,
        True,
        True,
    ]
    assert [point["turnover"] for point in result["curve"][1:]] == [0.0, 0.0]
    assert len(result["trades"]) == 1
    assert result["trades"][0]["side"] == "BUY"
    assert result["curve"][0]["nav"] == pytest.approx(10000)
    assert result["curve"][1]["nav"] == pytest.approx(10500)
    assert result["curve"][2]["nav"] == pytest.approx(11000)
    assert result["nav_parity"]["max_abs_delta_cny"] == 0.0


def test_missing_intervening_mark_is_explicitly_stale():
    prices = _prices()
    del prices["sh600000"][D2]
    result = _project(prices, [D1, D2, D3])
    assert result["curve"][1]["nav"] == pytest.approx(10000)
    assert result["curve"][1]["stale_positions_count"] == 1
    assert result["curve"][2]["stale_positions_count"] == 0
    assert result["curve"][1]["daily_return"] == 0.0


def test_without_opt_in_preserves_legacy_single_rebalance_curve_shape():
    result = _project(_prices())
    assert len(result["curve"]) == 1
    assert "valuation_only" not in result["curve"][0]
    assert "stale_positions_count" not in result["curve"][0]


@pytest.mark.parametrize(
    "sessions",
    [[D1, D1], [D2, D3], [D1, "2026-10-21"], []],
)
def test_incomplete_or_inconsistent_session_inputs_fail_closed(sessions):
    with pytest.raises(ValueError):
        _project(_prices(), sessions)
