"""Fail-closed reconciliation of a Research-only event-sourced simulation."""

from copy import deepcopy

import pytest

from app.lib.strategy_engine.research_account_audit import (
    audit_simulation_account,
)
from app.lib.strategy_engine.simulation_ledger import ResearchSimulationLedger


def _fill(*, event_id="fill1", side="BUY", price=10.0, date="2026-10-27"):
    return {
        "type": "FILL",
        "event_id": event_id,
        "fill_id": event_id,
        "order_id": f"order:{event_id}",
        "stock_code": "sh600000",
        "side": side,
        "decision_date": "2026-10-26",
        "execution_date": date,
        "quantity": 100,
        "price": price,
        "fees": {"commission": 5.0, "transfer_fee": 0.01},
    }


def _fixture(*, stale=False):
    account = ResearchSimulationLedger(initial_cash=100_000)
    account.apply(_fill())
    account.mark({"sh600000": 11.0}, as_of="2026-10-27")
    points = [{"date": "2026-10-27", "nav": round(account.nav, 2)}]
    parity = [
        {
            "date": "2026-10-27",
            "legacy_nav": round(account.nav, 2),
            "ledger_nav": round(account.nav, 2),
            "delta": 0.0,
        }
    ]
    if stale:
        account.mark({}, as_of="2026-10-28")
        points.append({"date": "2026-10-28", "nav": round(account.nav, 2)})
        parity.append(
            {
                "date": "2026-10-28",
                "legacy_nav": round(account.nav, 2),
                "ledger_nav": round(account.nav, 2),
                "delta": 0.0,
            }
        )
    return {
        "initial_nav": 100_000.0,
        "terminal_nav": round(account.nav, 2),
        "curve": points,
        "ledger_events": deepcopy(account.events),
        "ledger_snapshot": account.snapshot(),
        "nav_parity": {
            "points": parity,
            "tolerance_cny": 0.01,
            "max_abs_delta_cny": 0,
        },
    }


def test_replays_fills_marks_cash_and_nav_without_claiming_forward():
    result = audit_simulation_account(_fixture(), expected_initial_nav=100_000)
    assert result["status"] == "ACCOUNTING_RECONCILED_RESEARCH_ONLY"
    assert result["cash_cny"] == 98994.99
    assert result["terminal_nav_cny"] == 100094.99
    assert result["positions_count"] == 1
    assert result["fills_count"] == 1
    assert result["valuation_sessions"] == 1
    assert result["source_certification"] == "UNVERIFIED"
    assert result["forward_certification"] == "NOT_EVALUATED"
    assert result["broker_execution"] is False


def test_stale_price_is_not_promoted_to_fresh_market_certification():
    result = audit_simulation_account(
        _fixture(stale=True), expected_initial_nav=100_000
    )
    assert result["valuation_sessions"] == 2
    assert result["stale_positions_count"] == 1
    assert result["fresh_price_certification"] == "UNVERIFIED"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda x: x["ledger_snapshot"].update(cash=99_999),
        lambda x: x["ledger_snapshot"]["positions"]["sh600000"].update(quantity=99),
        lambda x: x.update(terminal_nav=99_999),
        lambda x: x["curve"][0].update(nav=99_999),
        lambda x: x["nav_parity"]["points"][0].update(legacy_nav=99_999),
        lambda x: x["nav_parity"]["points"][0].update(delta=0.005),
        lambda x: x["ledger_events"].pop(),
        lambda x: x["ledger_events"].append(deepcopy(x["ledger_events"][-1])),
        lambda x: x["ledger_snapshot"].update(event_count=999),
        lambda x: x.update(initial_nav=1_000_000),
    ],
)
def test_tampered_balances_journal_or_parity_fail_loudly(mutate):
    payload = _fixture()
    mutate(payload)
    with pytest.raises(ValueError):
        audit_simulation_account(payload, expected_initial_nav=100_000)


def test_missing_valuation_session_mark_refused():
    payload = _fixture()
    payload["curve"].append({"date": "2026-10-28", "nav": 100094.99})
    payload["nav_parity"]["points"].append(
        {
            "date": "2026-10-28",
            "legacy_nav": 100094.99,
            "ledger_nav": 100094.99,
            "delta": 0,
        }
    )
    with pytest.raises(ValueError, match="missing daily MARK"):
        audit_simulation_account(payload, expected_initial_nav=100_000)


def test_ledger_can_reconcile_negative_realized_pnl():
    account = ResearchSimulationLedger(initial_cash=100_000)
    account.apply(_fill())
    account.mark({"sh600000": 10}, as_of="2026-10-27")
    nav1 = account.nav
    account.apply(_fill(event_id="sell1", side="SELL", price=9, date="2026-10-28"))
    account.mark({}, as_of="2026-10-28")
    nav2 = account.nav
    payload = {
        "initial_nav": 100000,
        "terminal_nav": round(nav2, 2),
        "curve": [
            {"date": "2026-10-27", "nav": round(nav1, 2)},
            {"date": "2026-10-28", "nav": round(nav2, 2)},
        ],
        "ledger_events": account.events,
        "ledger_snapshot": account.snapshot(),
        "nav_parity": {
            "points": [
                {
                    "date": "2026-10-27",
                    "legacy_nav": round(nav1, 2),
                    "ledger_nav": round(nav1, 2),
                    "delta": 0,
                },
                {
                    "date": "2026-10-28",
                    "legacy_nav": round(nav2, 2),
                    "ledger_nav": round(nav2, 2),
                    "delta": 0,
                },
            ],
            "max_abs_delta_cny": 0,
        },
    }
    report = audit_simulation_account(payload, expected_initial_nav=100_000)
    assert report["positions_count"] == 0
    assert report["terminal_nav_cny"] < 100_000
