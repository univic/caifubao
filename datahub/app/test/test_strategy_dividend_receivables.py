"""Research cash distributions: ex-date income is NOT immediately spendable."""

import pytest

from app.lib.strategy_engine.simulation_ledger import ResearchSimulationLedger


def _buy(eid="buy"):
    return {
        "type": "FILL",
        "event_id": eid,
        "fill_id": eid,
        "order_id": "buy-1",
        "stock_code": "sh600000",
        "side": "BUY",
        "decision_date": "2026-10-21",
        "execution_date": "2026-10-22",
        "quantity": 100,
        "price": 10,
        "fees": {},
    }


def _entitlement():
    return {
        "type": "DIVIDEND_ENTITLEMENT",
        "event_id": "sh600000:2026-10-26:cash",
        "stock_code": "sh600000",
        "quantity": 100,
        "cash_per_share": 0.5,
        "record_date": "2026-10-23",
        "ex_date": "2026-10-26",
        "pay_date": "2026-10-28",
    }


def _settlement():
    return {
        "type": "DIVIDEND_SETTLEMENT",
        "event_id": "sh600000:2026-10-28:payment",
        "entitlement_id": _entitlement()["event_id"],
        "pay_date": "2026-10-28",
    }


def test_ex_date_books_receivable_without_early_cash_or_fake_loss():
    ledger = ResearchSimulationLedger(initial_cash=5000)
    ledger.apply(_buy())
    ledger.mark({"sh600000": 10.0}, as_of="2026-10-23")
    before = ledger.snapshot()
    assert before["nav"] == pytest.approx(5000)
    ledger.apply(_entitlement())
    ledger.mark({"sh600000": 9.5}, as_of="2026-10-26")
    after = ledger.snapshot()
    assert after["cash"] == before["cash"]
    assert after["receivable_balance"] == pytest.approx(50)
    assert after["nav"] == pytest.approx(before["nav"])
    assert ledger.apply(_entitlement()) is False
    assert ledger.snapshot() == after


def test_settlement_moves_receivable_to_cash_even_if_stock_sold():
    ledger = ResearchSimulationLedger(initial_cash=5000)
    ledger.apply(_buy())
    ledger.apply(_entitlement())
    ledger.apply(
        {
            "type": "FILL",
            "event_id": "sale",
            "fill_id": "sale",
            "order_id": "sale-1",
            "stock_code": "sh600000",
            "side": "SELL",
            "decision_date": "2026-10-26",
            "execution_date": "2026-10-27",
            "quantity": 100,
            "price": 9.5,
            "fees": {},
        }
    )
    before = ledger.snapshot()
    assert before["positions"] == {}
    ledger.apply(_settlement())
    after = ledger.snapshot()
    assert after["cash"] - before["cash"] == pytest.approx(50)
    assert after["receivable_balance"] == 0
    assert after["nav"] == pytest.approx(before["nav"])
    rebuilt = ResearchSimulationLedger.rebuild(ledger.events, initial_cash=5000)
    assert rebuilt.snapshot() == ledger.snapshot()


@pytest.mark.parametrize(
    "override",
    [
        {"record_date": "2026-10-26"},
        {"pay_date": "2026-10-22"},
        {"pay_date": "not-a-date"},
        {"quantity": 101},
        {"cash_per_share": -0.1},
        {"cash_per_share": float("nan")},
    ],
)
def test_invalid_entitlement_fails_atomically(override):
    ledger = ResearchSimulationLedger(initial_cash=5000)
    ledger.apply(_buy())
    original = ledger.snapshot()
    with pytest.raises(ValueError):
        ledger.apply({**_entitlement(), **override})
    assert ledger.snapshot() == original
    assert len(ledger.events) == 1


def test_early_or_orphaned_payment_rejected():
    ledger = ResearchSimulationLedger(initial_cash=5000)
    ledger.apply(_buy())
    original = ledger.snapshot()
    with pytest.raises(ValueError, match="outstanding"):
        ledger.apply(_settlement())
    assert ledger.snapshot() == original
    ledger.apply(_entitlement())
    before = ledger.snapshot()
    with pytest.raises(ValueError, match="before payable"):
        ledger.apply({**_settlement(), "pay_date": "2026-10-27"})
    assert ledger.snapshot() == before


def test_repeated_payment_idempotent_conflict_rejected():
    ledger = ResearchSimulationLedger(initial_cash=5000)
    ledger.apply(_buy())
    ledger.apply(_entitlement())
    payment = _settlement()
    assert ledger.apply(payment)
    after = ledger.snapshot()
    assert ledger.apply(payment) is False
    assert ledger.snapshot() == after
    with pytest.raises(ValueError, match="conflicting"):
        ledger.apply({**payment, "pay_date": "2026-10-30"})


def test_entitlement_is_not_reinvested_at_ex_date():
    ledger = ResearchSimulationLedger(initial_cash=1050)
    ledger.apply(_buy())
    ledger.apply(_entitlement())
    assert ledger.cash == 50
    assert ledger.nav == pytest.approx(1100)
    with pytest.raises(ValueError, match="insufficient cash"):
        ledger.apply(
            {
                **_buy("second-buy"),
                "order_id": "second-buy",
                "stock_code": "sh600001",
                "quantity": 10,
            }
        )
    assert ledger.cash == 50
    assert ledger.receivables
