"""Paper opening fills must not depend on prices learned after execution."""

import pytest

from app.lib.strategy_engine.nav import QuoteView, simulate_paper_nav


ZERO_COSTS = {
    "commission_rate": 0,
    "minimum_commission_cny": 0,
    "sell_stamp_duty_rate": 0,
    "slippage_per_side": 0,
}
D1, D2, D3 = "2026-01-02", "2026-01-05", "2026-01-06"


def _opening_case(close):
    return simulate_paper_nav(
        prices={
            "a": {D1: QuoteView(10, 10), D2: QuoteView(20, close)},
            "b": {D2: QuoteView(10, 10)},
        },
        schedule=[
            {"date": D1, "holdings": {"a": 0.5}},
            {"date": D2, "holdings": {"a": 0.5, "b": 0.25}},
        ],
        initial_nav=100_000,
        execution=ZERO_COSTS,
    )


def test_same_day_close_cannot_change_opening_sizing_or_turnover():
    flat, rallied = _opening_case(20), _opening_case(40)
    assert flat["curve"][1]["turnover"] == rallied["curve"][1]["turnover"]
    assert flat["trades"] == rallied["trades"]
    # Opening gap from 10 to 20 is already observable: 50k cash + 100k A.
    assert flat["trades"][1]["quantity"] == 3700
    assert flat["curve"][1]["turnover"] == round(37_000 / 150_000, 6)
    assert rallied["terminal_nav"] - flat["terminal_nav"] == 100_000


@pytest.mark.parametrize("quote", [None, QuoteView(100, 100, trade_status=0)])
def test_untradable_holding_carries_prior_close_for_opening_and_nav(quote):
    result = simulate_paper_nav(
        prices={
            "a": {D1: QuoteView(10, 12), **({D2: quote} if quote else {})},
            "b": {D2: QuoteView(10, 10)},
        },
        schedule=[
            {"date": D1, "holdings": {"a": 0.5}},
            {"date": D2, "holdings": {"b": 0.25}},
        ],
        initial_nav=100_000,
        execution=ZERO_COSTS,
    )
    assert result["terminal_nav"] == 110_000
    assert result["curve"][1]["positions_count"] == 2
    assert [(t["stock_code"], t["side"], t["quantity"]) for t in result["trades"]] == [
        ("a", "BUY", 5000),
        ("b", "BUY", 2700),
    ]


def test_fill_ledger_and_nav_account_for_actual_execution_costs():
    result = simulate_paper_nav(
        prices={"a": {D1: QuoteView(10, 10), D2: QuoteView(10, 10)}},
        schedule=[
            {"date": D1, "holdings": {"a": 0.5}},
            {"date": D2, "holdings": {}},
        ],
        initial_nav=100_000,
    )
    buy, sell = result["trades"]
    assert (buy["date"], sell["date"]) == (D1, D2)
    # Fee-aware sizing (roadmap 1.3a): budget 50,000 -> 4900 shares, because
    # 5000 shares would spend 50,062.51 > target budget at the slippage
    # adjusted open (10.01).
    assert buy["quantity"] == sell["quantity"] == 4900
    assert buy["price"] == pytest.approx(10.01)
    assert sell["price"] == pytest.approx(9.99)
    assert buy["costs"] == pytest.approx({"commission": 12.26225, "stamp_duty": 0})
    assert sell["costs"] == pytest.approx(
        {"commission": 12.23775, "stamp_duty": 48.951}
    )
    assert result["terminal_nav"] == pytest.approx(99_828.55)


@pytest.mark.parametrize("bad_price", [None, 0, -10, float("nan"), float("inf"), "bad"])
def test_invalid_open_never_produces_an_order(bad_price):
    result = simulate_paper_nav(
        prices={"a": {D1: QuoteView(bad_price, 10)}},
        schedule=[{"date": D1, "holdings": {"a": 0.5}}],
        initial_nav=100_000,
    )
    assert result["trades"] == []
    assert result["terminal_nav"] == 100_000


@pytest.mark.parametrize("bad_close", [None, 0, -10, float("nan"), float("inf"), "bad"])
def test_invalid_close_cannot_corrupt_carried_nav(bad_close):
    result = simulate_paper_nav(
        prices={"a": {D1: QuoteView(10, 12), D2: QuoteView(15, bad_close)}},
        schedule=[
            {"date": D1, "holdings": {"a": 0.5}},
            {"date": D2, "holdings": {"a": 0.5}},
        ],
        initial_nav=100_000,
        execution=ZERO_COSTS,
    )
    assert result["terminal_nav"] == 110_000


def test_quotes_after_schedule_are_never_read():
    class UnreadableQuote:
        def __getattribute__(self, name):
            raise AssertionError("future quote accessed")

    result = simulate_paper_nav(
        prices={"a": {D1: QuoteView(10, 10), D3: UnreadableQuote()}},
        schedule=[{"date": D1, "holdings": {"a": 0.5}}],
        initial_nav=100_000,
        execution=ZERO_COSTS,
    )
    assert result["terminal_nav"] == 100_000
