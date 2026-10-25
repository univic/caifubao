"""Daily research simulation corporate actions: true ex/pay cash causality."""

import pytest

from app.lib.strategy_engine.nav import QuoteView, simulate_paper_nav
from app.lib.strategy_engine.simulation_ledger import ResearchSimulationLedger

D1 = "2026-10-22"
D2 = "2026-10-23"
D3 = "2026-10-26"

NO_FEE = {
    "commission_rate": 0,
    "minimum_commission_cny": 0,
    "sell_stamp_duty_rate": 0,
    "transfer_fee_rate": 0,
    "slippage_per_side": 0,
}


def _action(**changes):
    event = {
        "stock_code": "sh600000",
        "record_date": D1,
        "ex_date": D2,
        "pay_date": D3,
        "cash_per_share": 0.5,
        "share_multiplier": 1.0,
    }
    event.update(changes)
    return event


def _run(action, *, prices=None, days=None):
    market = prices or {
        "sh600000": {
            D1: QuoteView(10, 10, previous_close=10),
            D2: QuoteView(9.5, 9.5, previous_close=10),
            D3: QuoteView(9.5, 9.5, previous_close=9.5),
        }
    }
    return simulate_paper_nav(
        prices=market,
        schedule=[{"date": D1, "holdings": {"sh600000": 0.5}}],
        valuation_dates=days if days is not None else [D1, D2, D3],
        corporate_actions=[action],
        initial_nav=10_000,
        execution=NO_FEE,
    )


def test_cash_receivable_offsets_ex_date_raw_price_drop_until_payment():
    result = _run(_action())
    assert len(result["trades"]) == 1
    assert [p["nav"] for p in result["curve"]] == [10000, 10000, 10000]
    assert result["nav_parity"]["max_abs_delta_cny"] == 0
    events = result["ledger_events"]
    kinds = [event["type"] for event in events]
    assert kinds.count("DIVIDEND_ENTITLEMENT") == 1
    assert kinds.count("DIVIDEND_SETTLEMENT") == 1
    assert all(event["source_domain"] == "research" for event in events if event["type"] == "FILL")
    replay = ResearchSimulationLedger.rebuild(events, initial_cash=10_000)
    assert replay.cash == pytest.approx(5250)
    assert replay.receivables == {}
    assert replay.nav == pytest.approx(10_000)


def test_ex_date_receivable_not_spendable_before_later_pay_date():
    future_pay = _action(pay_date="2026-10-30")
    result = _run(future_pay)
    assert len(result["trades"]) == 1
    assert "DIVIDEND_SETTLEMENT" not in [
        event["type"] for event in result["ledger_events"]
    ]
    account = ResearchSimulationLedger.rebuild(result["ledger_events"], initial_cash=10000)
    assert account.cash == pytest.approx(5000)
    assert account.nav == pytest.approx(10000)
    assert sum(x["amount"] for x in account.receivables.values()) == pytest.approx(250)


def test_split_uses_record_date_quantity_before_ex_open_and_marks_raw_close():
    market = {
        "sh600000": {
            D1: QuoteView(10, 10, previous_close=10),
            D2: QuoteView(5, 5, previous_close=10),
            D3: QuoteView(5, 5, previous_close=5),
        }
    }
    result = _run(_action(cash_per_share=0, share_multiplier=2), prices=market)
    assert [p["nav"] for p in result["curve"]] == [10000, 10000, 10000]
    ledger = ResearchSimulationLedger.rebuild(result["ledger_events"], initial_cash=10000)
    assert ledger.positions["sh600000"]["quantity"] == 1000
    assert ledger.positions["sh600000"]["cost_basis"] == pytest.approx(5000)


@pytest.mark.parametrize(
    "action",
    [
        _action(cash_per_share=-1),
        _action(share_multiplier=0),
        _action(record_date=D2),
        _action(ex_date="not-date"),
    ],
)
def test_invalid_corporate_actions_fail_before_any_plan_simulation(action):
    with pytest.raises(ValueError):
        _run(action)


def test_missing_record_or_ex_session_fails_closed():
    with pytest.raises(ValueError, match="record/ex"):
        _run(_action(), days=[D1, D3])


def test_no_implicit_corporate_feed_assumption():
    with pytest.raises(ValueError, match="daily valuation"):
        simulate_paper_nav(
            prices={},
            schedule=[{"date": D1, "holdings": {}}],
            corporate_actions=[],
        )


def test_duplicate_ex_date_event_not_double_counted():
    with pytest.raises(ValueError, match="duplicate"):
        simulate_paper_nav(
            prices={},
            schedule=[{"date": D1, "holdings": {}}],
            valuation_dates=[D1, D2, D3],
            corporate_actions=[_action(), _action(cash_per_share=0.2)],
        )
