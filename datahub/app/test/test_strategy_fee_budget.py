# -*- coding: utf-8 -*-
"""Fee-aware opening BUY budget (roadmap 1.3a).

Regression suite for the buy-sizing fix in nav.py: the largest board-lot
quantity whose spend (value at the slippage-adjusted open + commission,
commission = max(value*rate, minimum)) fits min(cash, target budget) must be
selected; an order is skipped only when even one lot cannot fit — never just
because the fee-unadjusted initial lot count overshoots.
"""

from app.lib.strategy_engine.nav import QuoteView, simulate_paper_nav

D = "2026-01-02"

ZERO_COSTS = {
    "commission_rate": 0,
    "minimum_commission_cny": 0,
    "sell_stamp_duty_rate": 0,
    "slippage_per_side": 0,
}


def _single(open_price, weight, initial_nav, execution=None):
    return simulate_paper_nav(
        prices={"a": {D: QuoteView(open_price, open_price)}},
        schedule=[{"date": D, "holdings": {"a": weight}}],
        initial_nav=initial_nav,
        execution=execution,
    )


def _buys(result):
    return [t for t in result["trades"] if t["side"] == "BUY"]


def test_full_target_reserves_slippage_and_commission():
    """100% target: fee-unadjusted max lots overshoot cash once slippage +
    commission apply; the engine must buy one lot fewer, not skip the order."""
    result = _single(open_price=10.0, weight=1.0, initial_nav=100_000.0)
    buys = _buys(result)
    assert len(buys) == 1, "100% target must still produce a BUY (fee-reserved)"
    trade = buys[0]
    # exec price = 10 * 1.001 = 10.01; 9900 shares fit cash, 10000 does not.
    assert trade["quantity"] == 9900
    spend = trade["price"] * trade["quantity"] + trade["costs"]["commission"]
    assert spend <= 100_000.0 + 1e-6


def test_minimum_commission_boundary_reduces_to_budget():
    """value*rate < minimum_commission: commission = 5 CNY; spend must still
    respect the target budget (15000) instead of overshooting to 15020."""
    result = _single(open_price=50.0, weight=0.15, initial_nav=100_000.0)
    buys = _buys(result)
    assert len(buys) == 1
    trade = buys[0]
    assert trade["quantity"] == 200  # 300 shares would spend 15020 > 15000
    assert trade["costs"]["commission"] == 5.0  # minimum commission applies
    spend = trade["price"] * trade["quantity"] + trade["costs"]["commission"]
    assert spend <= 15_000.0 + 1e-6


def test_proportional_commission_boundary_reduces_to_budget():
    """value*rate >= minimum_commission: proportional commission; spend must
    fit the 50000 target budget (5000 shares would spend 50062.51)."""
    result = _single(open_price=10.0, weight=0.5, initial_nav=100_000.0)
    buys = _buys(result)
    assert len(buys) == 1
    trade = buys[0]
    assert trade["quantity"] == 4900
    assert trade["costs"]["commission"] > 5.0  # proportional branch
    spend = trade["price"] * trade["quantity"] + trade["costs"]["commission"]
    assert spend <= 50_000.0 + 1e-6


def test_cash_below_one_fee_inclusive_lot_skips():
    """Cash below the fee-inclusive cost of one lot: no BUY for the name."""
    result = _single(open_price=210.0, weight=1.0, initial_nav=20_000.0)
    assert _buys(result) == []


def test_sequential_buys_never_overdraw_cash():
    """Two new targets bought in order: each spend stays within its own
    min(remaining cash, target budget); final cash is never negative."""
    prices = {
        "a": {D: QuoteView(10.0, 10.0)},
        "b": {D: QuoteView(20.0, 20.0)},
    }
    result = simulate_paper_nav(
        prices=prices,
        schedule=[{"date": D, "holdings": {"a": 0.6, "b": 0.4}}],
        initial_nav=100_000.0,
    )
    buys = {t["stock_code"]: t for t in _buys(result)}
    assert set(buys) == {"a", "b"}
    # a: budget 60000 -> 5900 shares (6000 would spend 60075.03); b: budget
    # min(remaining cash, 40000) -> 1900 shares (2000 would spend 40050.01).
    assert buys["a"]["quantity"] == 5900
    assert buys["b"]["quantity"] == 1900
    total_spend = sum(
        t["price"] * t["quantity"] + t["costs"]["commission"] for t in buys.values()
    )
    assert total_spend <= 100_000.0 + 1e-6


def test_zero_cost_execution_is_unchanged():
    """Zero slippage/commission: quantity equals the plain budget/price
    board-lot quantity (pre-change behavior preserved)."""
    result = _single(
        open_price=10.0, weight=1.0, initial_nav=100_000.0, execution=ZERO_COSTS
    )
    buys = _buys(result)
    assert len(buys) == 1
    assert buys[0]["quantity"] == 10_000
