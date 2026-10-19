# -*- coding: utf-8 -*-
"""Fee-aware opening BUY budget (roadmap 1.3a) + board minimums (2.2).

Regression suite for the buy-sizing fix in nav.py: the largest board-legal
quantity whose spend (value at the slippage-adjusted open + commission +
two-way transfer fee, commission = max(value*rate, minimum)) fits
min(cash, target budget) must be selected; an order is skipped only when even
the board minimum cannot fit — never just because the fee-unadjusted initial lot
count overshoots.
"""

import pytest

from app.lib.strategy_engine.nav import QuoteView, board_lot_rule, simulate_paper_nav

D = "2026-01-02"

ZERO_COSTS = {
    "commission_rate": 0,
    "minimum_commission_cny": 0,
    "sell_stamp_duty_rate": 0,
    "transfer_fee_rate": 0,
    "slippage_per_side": 0,
}


def _single(open_price, weight, initial_nav, execution=None, code="a"):
    return simulate_paper_nav(
        prices={
            code: {D: QuoteView(open_price, open_price, previous_close=open_price)}
        },
        schedule=[{"date": D, "holdings": {code: weight}}],
        initial_nav=initial_nav,
        execution=execution,
    )


def _buys(result):
    return [t for t in result["trades"] if t["side"] == "BUY"]


def test_full_target_reserves_slippage_and_commission():
    """100% target: fee-unadjusted max lots overshoot cash once slippage +
    commission + transfer fee apply; the engine must buy one lot fewer, not skip
    the order."""
    result = _single(open_price=10.0, weight=1.0, initial_nav=100_000.0)
    buys = _buys(result)
    assert len(buys) == 1, "100% target must still produce a BUY (fee-reserved)"
    trade = buys[0]
    # exec price = 10 * 1.001 = 10.01; 9900 shares fit 100,000 once the transfer
    # fee is charged on the buy leg too, 10000 does not.
    assert trade["quantity"] == 9900
    spend = (
        trade["price"] * trade["quantity"]
        + trade["costs"]["commission"]
        + trade["costs"]["transfer_fee"]
    )
    assert spend <= 100_000.0 + 1e-6


def test_minimum_commission_boundary_reduces_to_budget():
    """value*rate < minimum_commission: commission = 5 CNY; spend must still
    respect the target budget (15000) instead of overshooting to 15020."""
    result = _single(open_price=50.0, weight=0.15, initial_nav=100_000.0)
    buys = _buys(result)
    assert len(buys) == 1
    trade = buys[0]
    assert trade["quantity"] == 200  # 300 shares would overshoot 15,000
    assert trade["costs"]["commission"] == 5.0  # minimum commission applies
    spend = (
        trade["price"] * trade["quantity"]
        + trade["costs"]["commission"]
        + trade["costs"]["transfer_fee"]
    )
    assert spend <= 15_000.0 + 1e-6


def test_proportional_commission_boundary_reduces_to_budget():
    """value*rate >= minimum_commission: proportional commission; spend must
    fit the 50000 target budget (5000 shares would overshoot once the transfer
    fee is reserved)."""
    result = _single(open_price=10.0, weight=0.5, initial_nav=100_000.0)
    buys = _buys(result)
    assert len(buys) == 1
    trade = buys[0]
    assert trade["quantity"] == 4900
    assert trade["costs"]["commission"] > 5.0  # proportional branch
    spend = (
        trade["price"] * trade["quantity"]
        + trade["costs"]["commission"]
        + trade["costs"]["transfer_fee"]
    )
    assert spend <= 50_000.0 + 1e-6


def test_cash_below_one_fee_inclusive_lot_skips():
    """Cash below the fee-inclusive cost of one lot: no BUY for the name."""
    result = _single(open_price=210.0, weight=1.0, initial_nav=20_000.0)
    assert _buys(result) == []


def test_sequential_buys_never_overdraw_cash():
    """Two new targets bought in order: each spend stays within its own
    min(remaining cash, target budget); final cash is never negative."""
    prices = {
        "a": {D: QuoteView(10.0, 10.0, previous_close=10.0)},
        "b": {D: QuoteView(20.0, 20.0, previous_close=20.0)},
    }
    result = simulate_paper_nav(
        prices=prices,
        schedule=[{"date": D, "holdings": {"a": 0.6, "b": 0.4}}],
        initial_nav=100_000.0,
    )
    buys = {t["stock_code"]: t for t in _buys(result)}
    assert set(buys) == {"a", "b"}
    # a: budget 60000 -> 5900 shares (6000 would overshoot once commission +
    # the transfer fee are reserved); b: min(remaining cash, 40000) -> 1900.
    assert buys["a"]["quantity"] == 5900
    assert buys["b"]["quantity"] == 1900
    total_spend = sum(
        t["price"] * t["quantity"]
        + t["costs"]["commission"]
        + t["costs"]["transfer_fee"]
        for t in buys.values()
    )
    assert total_spend <= 100_000.0 + 1e-6


def test_zero_cost_execution_is_unchanged():
    """Zero slippage/commission/transfer fee: quantity equals the plain
    budget/price board-lot quantity (pre-change behavior preserved)."""
    result = _single(
        open_price=10.0, weight=1.0, initial_nav=100_000.0, execution=ZERO_COSTS
    )
    buys = _buys(result)
    assert len(buys) == 1
    assert buys[0]["quantity"] == 10_000


def test_star_board_minimum_is_200_with_one_share_increments():
    """科创板: a BUY is at least 200 shares and then steps by 1 share."""
    assert board_lot_rule("sh688001") == (200, 1)
    assert board_lot_rule("688001") == (200, 1)
    # A budget that would buy 300 main-board shares buys 300 STAR shares too
    # (200 + 100 one-share steps).
    result = _single(
        open_price=10.0,
        weight=0.03,
        initial_nav=100_000.0,
        execution=ZERO_COSTS,
        code="sh688001",
    )
    assert _buys(result)[0]["quantity"] == 300
    # Below the 200-share minimum: no order at all, not a 100-share lot.
    thin = _single(
        open_price=10.0,
        weight=0.01,
        initial_nav=100_000.0,
        execution=ZERO_COSTS,
        code="sh688001",
    )
    assert _buys(thin) == []


def test_main_board_and_chinext_keep_100_share_lots():
    assert board_lot_rule("sh600000") == (100, 100)
    assert board_lot_rule("sz000001") == (100, 100)
    assert board_lot_rule("sz300750") == (100, 100)
    for code in ("sh600000", "sz300750"):
        result = _single(
            open_price=10.0,
            weight=0.015,
            initial_nav=100_000.0,
            execution=ZERO_COSTS,
            code=code,
        )
        # 150 shares is not a legal 100-share lot: size down to 100.
        assert _buys(result)[0]["quantity"] == 100


def test_sell_is_exempt_from_the_buy_minimum_so_odd_lots_close():
    """A genuine odd lot must be closable: STAR size 201 shares (200 + a 1-share
    increment, so NOT a 100-share lot) buys, then the whole 201-share position is
    sold. SELL sizing never applies the buy board minimum/increment."""
    result = simulate_paper_nav(
        prices={
            "sh688001": {
                D: QuoteView(10.0, 10.0, previous_close=10.0),
                "2026-01-05": QuoteView(10.0, 10.0, previous_close=10.0),
            }
        },
        schedule=[
            # budget 2013 at open 10 -> 201 shares: 202 would exceed the budget.
            {"date": D, "holdings": {"sh688001": 1.0}},
            {"date": "2026-01-05", "holdings": {}},
        ],
        initial_nav=2013.0,
        execution=ZERO_COSTS,
    )
    buy, sell = result["trades"]
    assert buy["side"] == "BUY"
    assert buy["quantity"] == 201  # 200 minimum + 1 one-share increment
    assert buy["quantity"] % 100 != 0  # not a legal 100-share lot
    assert sell["side"] == "SELL"
    assert sell["quantity"] == 201  # the odd lot closes out in full
    assert result["blocked_count"] == 0


def test_transfer_fee_is_charged_on_both_sides():
    result = _single(open_price=10.0, weight=0.5, initial_nav=100_000.0)
    buy = _buys(result)[0]
    expected = buy["price"] * buy["quantity"] * 0.00001
    assert buy["costs"]["transfer_fee"] == pytest.approx(expected)
    assert buy["costs"]["stamp_duty"] == 0.0


def test_transfer_fee_changes_the_config_hash():
    from app.lib.strategy_engine.config import (
        strategy_config_hash,
        validate_strategy_config,
    )

    base = validate_strategy_config({"score_model_version": "v1"})
    priced = validate_strategy_config(
        {"score_model_version": "v1", "execution": {"transfer_fee_rate": 0.0001}}
    )
    assert strategy_config_hash(base) != strategy_config_hash(priced)
