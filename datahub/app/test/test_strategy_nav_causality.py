"""Paper opening fills: causality, limit gating and reporting.

The opening fill must not depend on prices learned after execution, and it must
be refused (and reported) when the session is limit-up for a BUY, limit-down for
a SELL, or when the limit verdict cannot be derived at all (fail closed).
"""

import pytest

from app.lib.strategy_engine.nav import QuoteView, simulate_paper_nav

ZERO_COSTS = {
    "commission_rate": 0,
    "minimum_commission_cny": 0,
    "sell_stamp_duty_rate": 0,
    "transfer_fee_rate": 0,
    "slippage_per_side": 0,
}
D1, D2, D3 = "2026-01-02", "2026-01-05", "2026-01-06"


def _opening_case(close):
    return simulate_paper_nav(
        prices={
            "a": {
                D1: QuoteView(10, 10, previous_close=10),
                D2: QuoteView(20, close, previous_close=10),
            },
            "b": {D2: QuoteView(10, 10, previous_close=10)},
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
            "a": {
                D1: QuoteView(10, 12, previous_close=10),
                **({D2: quote} if quote else {}),
            },
            "b": {D2: QuoteView(10, 10, previous_close=10)},
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
        prices={"a": {D1: QuoteView(10, 10, previous_close=10)}},
        schedule=[{"date": D1, "holdings": {"a": 0.5}}],
        initial_nav=100_000,
    )
    buy = result["trades"][0]
    assert buy["quantity"] == 4900
    assert buy["price"] == pytest.approx(10.01)
    assert buy["costs"] == pytest.approx(
        {"commission": 12.26225, "stamp_duty": 0, "transfer_fee": 0.49049}
    )
    # Round trip: sell the same position on the next session.
    round_trip = simulate_paper_nav(
        prices={
            "a": {
                D1: QuoteView(10, 10, previous_close=10),
                D2: QuoteView(10, 10, previous_close=10),
            }
        },
        schedule=[
            {"date": D1, "holdings": {"a": 0.5}},
            {"date": D2, "holdings": {}},
        ],
        initial_nav=100_000,
    )
    sell = round_trip["trades"][1]
    assert sell["side"] == "SELL"
    assert sell["price"] == pytest.approx(9.99)
    assert sell["costs"] == pytest.approx(
        {"commission": 12.23775, "stamp_duty": 24.4755, "transfer_fee": 0.48951}
    )
    assert round_trip["terminal_nav"] == pytest.approx(99_852.04)


def test_limit_up_open_is_refused_for_a_buy_and_reported():
    """A planned BUY must not fill on a limit-up open (+10 % board)."""
    result = simulate_paper_nav(
        prices={"a": {D1: QuoteView(11.0, 11.0, previous_close=10.0)}},
        schedule=[{"date": D1, "holdings": {"a": 0.5}}],
        initial_nav=100_000,
    )
    assert result["trades"] == []
    assert result["terminal_nav"] == 100_000
    assert result["blocked"] == [
        {"date": D1, "stock_code": "a", "side": "BUY", "reason": "at_limit_up"}
    ]
    assert result["blocked_count"] == 1


def test_limit_down_open_is_refused_for_a_sell_and_reported():
    """A planned SELL must not fill on a limit-down open (-10 % board)."""
    result = simulate_paper_nav(
        prices={
            "a": {
                D1: QuoteView(10.0, 10.0, previous_close=10.0),
                D2: QuoteView(9.0, 9.0, previous_close=10.0),
            }
        },
        schedule=[
            {"date": D1, "holdings": {"a": 0.5}},
            {"date": D2, "holdings": {}},
        ],
        initial_nav=100_000,
    )
    assert [t["side"] for t in result["trades"]] == ["BUY"]
    assert result["blocked"] == [
        {"date": D2, "stock_code": "a", "side": "SELL", "reason": "at_limit_down"}
    ]
    # The blocked exit rolls forward: the position is still held.
    assert result["curve"][1]["positions_count"] == 1


def test_missing_limit_flags_fail_closed_and_are_reported():
    """No explicit flag and no previous_close -> the verdict is unknown; the BUY
    is refused (fail loud) rather than filled (fail open)."""
    result = simulate_paper_nav(
        prices={"a": {D1: QuoteView(10.0, 10.0)}},
        schedule=[{"date": D1, "holdings": {"a": 0.5}}],
        initial_nav=100_000,
    )
    assert result["trades"] == []
    assert result["blocked_count"] == 1
    assert result["blocked"][0]["reason"] == "limit_unknown"


def test_unknown_limit_verdict_does_not_stop_other_names():
    """A mixed batch: the name with a derivable in-band verdict fills and only
    the name with an unknowable verdict is refused (fail loud per name, never a
    whole-batch abort)."""
    result = simulate_paper_nav(
        prices={
            "a": {D1: QuoteView(10.0, 10.0, previous_close=10.0)},  # in band
            "b": {D1: QuoteView(20.0, 20.0)},  # no previous_close -> unknown
        },
        schedule=[{"date": D1, "holdings": {"a": 0.4, "b": 0.4}}],
        initial_nav=100_000,
        execution=ZERO_COSTS,
    )
    assert [(t["stock_code"], t["side"]) for t in result["trades"]] == [("a", "BUY")]
    assert result["blocked"] == [
        {"date": D1, "stock_code": "b", "side": "BUY", "reason": "limit_unknown"}
    ]
    assert result["blocked_count"] == 1
    assert result["curve"][0]["positions_count"] == 1


def test_explicit_limit_flags_win_over_the_derived_verdict():
    blocked = simulate_paper_nav(
        prices={"a": {D1: QuoteView(11.0, 11.0, previous_close=10.0, limit_up=False)}},
        schedule=[{"date": D1, "holdings": {"a": 0.5}}],
        initial_nav=100_000,
        execution=ZERO_COSTS,
    )
    assert len(blocked["trades"]) == 1  # explicit "not at the limit" wins
    assert blocked["blocked_count"] == 0


@pytest.mark.parametrize("bad_price", [None, 0, -10, float("nan"), float("inf"), "bad"])
def test_invalid_open_never_produces_an_order(bad_price):
    result = simulate_paper_nav(
        prices={"a": {D1: QuoteView(bad_price, 10, previous_close=10)}},
        schedule=[{"date": D1, "holdings": {"a": 0.5}}],
        initial_nav=100_000,
    )
    assert result["trades"] == []
    assert result["terminal_nav"] == 100_000


@pytest.mark.parametrize("bad_close", [None, 0, -10, float("nan"), float("inf"), "bad"])
def test_invalid_close_cannot_corrupt_carried_nav(bad_close):
    result = simulate_paper_nav(
        prices={
            "a": {
                D1: QuoteView(10, 12, previous_close=10),
                D2: QuoteView(15, bad_close, previous_close=15),
            }
        },
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
        prices={"a": {D1: QuoteView(10, 10, previous_close=10), D3: UnreadableQuote()}},
        schedule=[{"date": D1, "holdings": {"a": 0.5}}],
        initial_nav=100_000,
        execution=ZERO_COSTS,
    )
    assert result["terminal_nav"] == 100_000
