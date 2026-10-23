# -*- coding: utf-8 -*-
"""S1/S2/S3 unit tests: immutable events, fills and conservative risk."""

import pytest

from caifubao_contracts.strategy_execution import (
    board_lot_rule,
    deterministic_fill_id,
    execution_fees,
    fit_buy_quantity,
    tradable_open,
)
from app.lib.strategy_engine.risk_gate import evaluate_risk
from app.lib.strategy_engine.simulation_ledger import ResearchSimulationLedger


def _fill(event_id, side, quantity, date="2026-10-23", price=10):
    return {
        "type": "FILL", "event_id": event_id, "fill_id": event_id,
        "order_id": "order-1", "stock_code": "sh600000", "side": side,
        "execution_date": date, "decision_date": "2026-10-22",
        "quantity": quantity, "price": price,
        "fees": {"commission": 5.0, "transfer_fee": 0.01},
    }


def test_partial_fills_and_idempotent_rebuild():
    account = ResearchSimulationLedger(initial_cash=10000)
    first = _fill("f1", "BUY", 100)
    second = _fill("f2", "BUY", 50)
    assert account.apply(first)
    assert account.apply(second)
    assert not account.apply(first)
    assert account.positions["sh600000"]["quantity"] == 150
    expected = account.snapshot()
    restored = ResearchSimulationLedger.rebuild(account.events, initial_cash=10000)
    assert restored.snapshot() == expected
    with pytest.raises(ValueError, match="conflicting"):
        account.apply({**first, "quantity": 99})


def test_sell_cost_and_cash_conservation():
    ledger = ResearchSimulationLedger(initial_cash=10000)
    ledger.apply(_fill("f1", "BUY", 100))
    ledger.apply(_fill("f2", "SELL", 100, price=11))
    assert ledger.positions == {}
    assert ledger.cash == pytest.approx(10000 - (1000 + 5.01) + (1100 - 5.01))
    assert ledger.realized_pnl == pytest.approx(89.98)


def test_t_plus_one_and_cash_fail_closed():
    ledger = ResearchSimulationLedger(initial_cash=100)
    with pytest.raises(ValueError, match="T\\+1"):
        ledger.apply(_fill("bad", "BUY", 100, date="2026-10-22"))
    with pytest.raises(ValueError, match="insufficient cash"):
        ledger.apply(_fill("bad2", "BUY", 100))
    assert ledger.events == []


def test_blocked_exit_retains_position_and_rejects_over_cap():
    ledger = ResearchSimulationLedger(initial_cash=10000, max_positions=1)
    ledger.apply(_fill("f1", "BUY", 100))
    ledger.apply({
        "type": "REJECTED", "event_id": "reject-1",
        "order_id": "order-2", "reason_code": "at_limit_down",
    })
    assert ledger.positions["sh600000"]["quantity"] == 100
    with pytest.raises(ValueError, match="max_positions"):
        ledger.apply({**_fill("f3", "BUY", 100), "stock_code": "sh600001",
                      "order_id": "order-3"})


def test_stale_mark_does_not_zero_position():
    ledger = ResearchSimulationLedger(initial_cash=10000)
    ledger.apply(_fill("f1", "BUY", 100))
    assert ledger.mark({"sh600000": 12.0}) > ledger.initial_cash
    nav = ledger.mark({})
    assert nav > ledger.initial_cash
    assert ledger.positions["sh600000"]["stale"] is True


def test_adjustments_are_explicit_not_fake_fills():
    ledger = ResearchSimulationLedger(initial_cash=10000)
    ledger.apply(_fill("f1", "BUY", 100))
    ledger.apply({"type": "SPLIT", "event_id": "split1",
                  "stock_code": "sh600000", "share_multiplier": 2})
    ledger.apply({"type": "DIVIDEND", "event_id": "div1",
                  "stock_code": "sh600000", "cash_per_share": 0.2})
    assert ledger.positions["sh600000"]["quantity"] == 200
    assert ledger.cash == pytest.approx(10000 - 1005.01 + 40)


def test_execution_rules_board_and_unknown_fail_closed():
    assert board_lot_rule("sh688001") == (200, 1)
    assert board_lot_rule("sz300001") == (100, 100)
    assert fit_buy_quantity(1510, 10, 200, 1) == 0
    assert fit_buy_quantity(1010, 10, 100, 100) == 100
    price, reason = tradable_open(
        {"trade_status": 1, "open": 10, "limit_up": None}, "BUY"
    )
    assert price is None and reason == "limit_unknown"
    assert tradable_open(
        {"trade_status": 1, "open": 10, "limit_up": None},
        "BUY", unknown_limit_policy="replay_compat"
    ) == (10, "")
    assert execution_fees(10, 100, "SELL")["stamp_duty"] > 0
    assert deterministic_fill_id(order_id="a", attempt_id="1", execution_id="1") != (
        deterministic_fill_id(order_id="a", attempt_id="1", execution_id="2")
    )


def test_risk_does_not_assume_approved_sell_filled():
    account = {
        "cash": 1000, "nav": 2000,
        "positions": {"sh600000": {"quantity": 100}},
    }
    intents = [
        {"intent_id": "sell", "side": "SELL", "stock_code": "sh600000",
         "quantity": 100, "price": 10},
        {"intent_id": "buy", "side": "BUY", "stock_code": "sh600001",
         "quantity": 100, "price": 10},
    ]
    outcome = evaluate_risk(
        intents=intents, account=account,
        policy={"max_positions": 1},
    )
    assert [x["intent_id"] for x in outcome["approved"]] == ["sell"]
    assert outcome["rejected"][0]["reason_code"] == "MAX_POSITIONS"
    assert "sh600000" in account["positions"]


def test_risk_halt_and_drawdown_are_explainable():
    account = {"cash": 1000, "nav": 1000, "positions": {}, "drawdown": -0.2}
    order = [{"intent_id": "buy", "stock_code": "sh600000", "side": "BUY",
              "quantity": 100, "price": 2}]
    policy = {"max_drawdown_pct": 0.1}
    assert evaluate_risk(intents=order, account=account, policy=policy)[
        "rejected"][0]["reason_code"] == "DRAWDOWN_BREAKER"
    assert evaluate_risk(intents=order, account=account, policy=policy,
                         halted=True)["status"] == "HALTED"
