# -*- coding: utf-8 -*-
"""S1/S2/S3 unit tests: immutable events, fills and conservative risk."""

import pytest

from caifubao_contracts.strategy_execution import (
    board_lot_rule,
    deterministic_fill_id,
    execution_fees,
    fit_buy_quantity,
    require_t_plus_one,
    session_limit_flags,
    tradable_open,
)
from app.lib.strategy_engine.risk_gate import evaluate_risk
from app.lib.strategy_engine.simulation_ledger import ResearchSimulationLedger


def _fill(event_id, side, quantity, date="2026-10-23", price=10):
    return {
        "type": "FILL",
        "event_id": event_id,
        "fill_id": event_id,
        "order_id": "order-1",
        "stock_code": "sh600000",
        "side": side,
        "execution_date": date,
        "decision_date": "2026-10-22",
        "quantity": quantity,
        "price": price,
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
    ledger.apply(
        {
            "type": "REJECTED",
            "event_id": "reject-1",
            "order_id": "order-2",
            "stock_code": "sh600000",
            "side": "SELL",
            "execution_date": "2026-10-23",
            "reason_code": "at_limit_down",
        }
    )
    assert ledger.positions["sh600000"]["quantity"] == 100
    with pytest.raises(ValueError, match="max_positions"):
        ledger.apply(
            {**_fill("f3", "BUY", 100), "stock_code": "sh600001", "order_id": "order-3"}
        )


def test_stale_mark_does_not_zero_position():
    ledger = ResearchSimulationLedger(initial_cash=10000)
    ledger.apply(_fill("f1", "BUY", 100))
    assert ledger.mark({"sh600000": 12.0}) > ledger.initial_cash
    nav = ledger.mark({})
    assert nav > ledger.initial_cash
    assert ledger.positions["sh600000"]["stale"] is True
    rebuilt = ResearchSimulationLedger.rebuild(ledger.events, initial_cash=10000)
    assert rebuilt.snapshot() == ledger.snapshot()


def test_adjustments_are_explicit_not_fake_fills():
    ledger = ResearchSimulationLedger(initial_cash=10000)
    ledger.apply(_fill("f1", "BUY", 100))
    ledger.apply(
        {
            "type": "SPLIT",
            "event_id": "split1",
            "stock_code": "sh600000",
            "share_multiplier": 2,
        }
    )
    ledger.apply(
        {
            "type": "DIVIDEND",
            "event_id": "div1",
            "stock_code": "sh600000",
            "cash_per_share": 0.2,
        }
    )
    assert ledger.positions["sh600000"]["quantity"] == 200
    assert ledger.cash == pytest.approx(10000 - 1005.01 + 40)


def test_invalid_ledger_events_are_atomic():
    ledger = ResearchSimulationLedger(initial_cash=3000)
    ledger.apply(_fill("f1", "BUY", 100))
    ledger.apply(
        {
            **_fill("f2", "BUY", 100),
            "stock_code": "sh600001",
            "order_id": "order-2",
        }
    )
    original = ledger.snapshot()
    original_events = len(ledger.events)
    with pytest.raises(ValueError, match="mark sh600001"):
        ledger.apply(
            {
                "type": "MARK",
                "event_id": "invalid-mark",
                "marks": {"sh600000": 12, "sh600001": -10},
            }
        )
    with pytest.raises(ValueError, match="reference_price"):
        ledger.apply(
            {
                **_fill("f3", "BUY", 10),
                "order_id": "order-3",
                "reference_price": 0,
            }
        )
    with pytest.raises(ValueError, match="exit_price"):
        ledger.apply(
            {
                "type": "WRITE_OFF",
                "event_id": "bad-write-off",
                "stock_code": "sh600000",
                "exit_price": -1,
                "reason_code": "DELISTED",
            }
        )
    assert ledger.snapshot() == original
    assert len(ledger.events) == original_events


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
        "BUY",
        unknown_limit_policy="replay_compat",
    ) == (10, "")
    assert execution_fees(10, 100, "SELL")["stamp_duty"] > 0
    assert deterministic_fill_id(order_id="a", attempt_id="1", execution_id="1") != (
        deterministic_fill_id(order_id="a", attempt_id="1", execution_id="2")
    )


def test_risk_does_not_assume_approved_sell_filled():
    account = {
        "cash": 1000,
        "nav": 2000,
        "positions": {"sh600000": {"quantity": 100}},
    }
    intents = [
        {
            "intent_id": "sell",
            "side": "SELL",
            "stock_code": "sh600000",
            "quantity": 100,
            "price": 10,
        },
        {
            "intent_id": "buy",
            "side": "BUY",
            "stock_code": "sh600001",
            "quantity": 100,
            "price": 10,
        },
    ]
    outcome = evaluate_risk(
        intents=intents,
        account=account,
        policy={"max_positions": 1},
    )
    assert [x["intent_id"] for x in outcome["approved"]] == ["sell"]
    assert outcome["rejected"][0]["reason_code"] == "MAX_POSITIONS"
    assert "sh600000" in account["positions"]


def test_sell_approval_does_not_spend_cash_or_double_reserve_shares():
    account = {
        "cash": 1200,
        "nav": 2200,
        "positions": {"sh600000": {"quantity": 100}},
    }
    intents = [
        {
            "intent_id": "sell-1",
            "side": "SELL",
            "stock_code": "sh600000",
            "quantity": 60,
            "price": 10,
        },
        {
            "intent_id": "sell-2",
            "side": "SELL",
            "stock_code": "sh600000",
            "quantity": 60,
            "price": 10,
        },
        {
            "intent_id": "buy",
            "side": "BUY",
            "stock_code": "sh600001",
            "quantity": 100,
            "price": 10,
        },
    ]
    first = evaluate_risk(intents=intents, account=account, policy={})
    assert [x["intent_id"] for x in first["approved"]] == ["sell-1", "buy"]
    assert first["rejected"][0]["intent"]["intent_id"] == "sell-2"
    assert first["rejected"][0]["reason_code"] == "INSUFFICIENT_HOLDINGS"
    assert evaluate_risk(intents=intents, account=account, policy={}) == first
    assert account["cash"] == 1200
    assert account["positions"]["sh600000"]["quantity"] == 100


def test_risk_halt_and_drawdown_are_explainable():
    account = {"cash": 1000, "nav": 1000, "positions": {}, "drawdown": -0.2}
    order = [
        {
            "intent_id": "buy",
            "stock_code": "sh600000",
            "side": "BUY",
            "quantity": 100,
            "price": 2,
        }
    ]
    policy = {"max_drawdown_pct": 0.1}
    assert (
        evaluate_risk(intents=order, account=account, policy=policy)["rejected"][0][
            "reason_code"
        ]
        == "DRAWDOWN_BREAKER"
    )
    assert (
        evaluate_risk(intents=order, account=account, policy=policy, halted=True)[
            "status"
        ]
        == "HALTED"
    )


def test_shared_board_limit_flags_match_research_tradability_adapter():
    from app.lib.strategy_engine.tradability import limit_verdict

    quotes = [
        ("sh600000", 10, 10.99, False),
        ("sz300750", 10, 11.99, False),
        ("sh688001", 10, 8.01, False),
        ("sh600001", 10, 10.49, True),
    ]
    limits = [10, 20, 20, 5]
    for (code, previous, opened, is_st), limit in zip(quotes, limits):
        shared = session_limit_flags(opened, previous, limit)
        research = limit_verdict(
            stock_code=code,
            open_price=opened,
            previous_close=previous,
            is_st=is_st,
        )
        assert shared == research
    assert session_limit_flags(0, 10, 10) == (None, None)
    assert session_limit_flags(10, None, 10) == (None, None)


def test_shared_risk_priority_budget_regime_and_exposure():
    from caifubao_contracts.strategy_risk import evaluate_risk as shared_risk

    assert shared_risk is evaluate_risk
    account = {
        "cash": 1600,
        "nav": 2000,
        "positions": {"sh600000": {"quantity": 100, "market_value": 400}},
    }
    intents = [
        {
            "intent_id": "low",
            "side": "BUY",
            "stock_code": "sh600001",
            "quantity": 100,
            "price": 4,
            "priority": 1,
        },
        {
            "intent_id": "high",
            "side": "BUY",
            "stock_code": "sh600002",
            "quantity": 100,
            "price": 5,
            "priority": 10,
        },
        {
            "intent_id": "sell",
            "side": "SELL",
            "stock_code": "sh600000",
            "quantity": 100,
            "price": 4,
        },
    ]
    policy = {
        "max_turnover_pct": 0.5,
        "max_single_stock_pct": 0.3,
        "max_gross_exposure_pct": 0.8,
        "regime_policy": "bull_only",
    }
    passed = shared_risk(
        account=account,
        intents=intents,
        policy=policy,
        market={"regime": "bull"},
    )
    assert [x["intent_id"] for x in passed["approved"]] == ["sell", "high"]
    assert [x["reason_code"] for x in passed["rejected"]] == ["TURNOVER_BUDGET"]
    assert (
        shared_risk(
            account=account,
            intents=intents,
            policy=policy,
            market={"regime": "bull"},
        )
        == passed
    )
    blocked = shared_risk(
        account=account,
        intents=intents,
        policy=policy,
        market={"regime": "bear"},
    )
    assert [x["intent_id"] for x in blocked["approved"]] == ["sell"]
    assert all(x["reason_code"] == "REGIME_BLOCKED" for x in blocked["rejected"])
    assert account["cash"] == 1600


def test_shared_risk_missing_exposure_snapshot_fails_closed():
    account = {
        "cash": 1000,
        "nav": 2000,
        "positions": {"sh600000": {"quantity": 100}},
    }
    with pytest.raises(ValueError, match="market_value"):
        evaluate_risk(
            account=account,
            intents=[],
            policy={"max_single_stock_pct": 0.2},
        )
    with pytest.raises(ValueError, match="market.regime"):
        evaluate_risk(
            account={"cash": 1000, "nav": 1000, "positions": {}},
            intents=[],
            policy={"regime_policy": "bull_only"},
        )


def test_factor_lab_replay_shared_cost_mode_matches_shared_friction():
    from caifubao_contracts.friction import side_cost as shared_side_cost
    from scripts.factor_lab_account_replay import side_cost as factor_side_cost

    for side in ("BUY", "SELL"):
        for notional in (1000, 100000):
            assert factor_side_cost(notional, side, "shared_v1") == pytest.approx(
                shared_side_cost(notional, side).total
            )
    assert factor_side_cost(1000) != factor_side_cost(1000, "BUY", "shared_v1")
    with pytest.raises(ValueError, match="cost_model"):
        factor_side_cost(1000, "BUY", "unsupported")


def test_frozen_spec_risk_replay_matches_shared_gate_and_never_promotes():
    from app.jobs.strategy_risk_replay import replay_from_snapshots
    from app.lib.strategy_engine.config import DEFAULT_STRATEGY_CONFIG
    from app.lib.strategy_engine.spec import legacy_to_spec

    spec = legacy_to_spec(DEFAULT_STRATEGY_CONFIG)["spec"]
    spec["risk"]["regime_policy"] = "bull_only"
    account = {"cash": 1000, "nav": 1000, "positions": {}}
    intents = [
        {
            "intent_id": "buy",
            "stock_code": "sh600001",
            "side": "BUY",
            "quantity": 100,
            "price": 2,
        }
    ]
    bear = replay_from_snapshots(
        spec=spec,
        account=account,
        intents=intents,
        market={"regime": "bear"},
    )
    assert bear["approved"] == []
    assert bear["rejected"][0]["reason_code"] == "REGIME_BLOCKED"
    assert bear["evidence_kind"] == "REPLAY"
    assert bear["operational_approval"] is False
    assert (
        replay_from_snapshots(
            spec=spec,
            account=account,
            intents=intents,
            market={"regime": "bear"},
        )
        == bear
    )
    bull = replay_from_snapshots(
        spec=spec,
        account=account,
        intents=intents,
        market={"regime": "bull"},
    )
    assert len(bull["approved"]) == 1
    assert bull["spec_hash"] == bear["spec_hash"]
    assert account["cash"] == 1000


def test_simulated_fills_carry_decision_date_and_spec_identity():
    from app.lib.strategy_engine.nav import QuoteView, simulate_paper_nav

    result = simulate_paper_nav(
        prices={
            "sh600000": {
                "2026-10-26": QuoteView(
                    open_price=10,
                    close_price=10,
                    previous_close=10,
                    stock_code="sh600000",
                )
            }
        },
        schedule=[
            {
                "date": "2026-10-26",
                "decision_date": "2026-10-23",
                "holdings": {"sh600000": 0.5},
            }
        ],
        initial_nav=10000,
        spec_hash="frozen-spec-sha",
    )
    fills = [x for x in result["ledger_events"] if x["type"] == "FILL"]
    assert len(fills) == 1
    fill = fills[0]
    assert fill["decision_date"] == "2026-10-23"
    assert fill["execution_date"] == "2026-10-26"
    assert fill["spec_hash"] == "frozen-spec-sha"
    assert fill["source_domain"] == "research"
    assert fill["intent_id"] == fill["order_id"]
    assert fill["currency"] == "CNY"
    rebuilt = ResearchSimulationLedger.rebuild(
        result["ledger_events"], initial_cash=10000
    )
    assert rebuilt.snapshot() == result["ledger_snapshot"]


def test_shared_t_plus_one_rejects_same_day_and_accepts_next_session():
    assert require_t_plus_one("2026-10-23", "2026-10-26") is None
    with pytest.raises(ValueError, match="T\\+1"):
        require_t_plus_one("2026-10-23", "2026-10-23")
    with pytest.raises(ValueError, match="T\\+1"):
        require_t_plus_one("2026-10-24", "2026-10-23")


def test_shared_ledger_event_schema_fail_closed_and_not_mutated():
    from caifubao_contracts.strategy_ledger_events import validate_ledger_event

    event = _fill("id-1", "BUY", 100)
    validated = validate_ledger_event(event)
    assert validated == event
    validated["quantity"] = 200
    assert event["quantity"] == 100
    with pytest.raises(ValueError, match="currency=CNY"):
        validate_ledger_event(
            {**event, "source_domain": "research", "account_id": "a", "currency": "USD"}
        )
    with pytest.raises(ValueError, match="order event"):
        validate_ledger_event(
            {"type": "REJECTED", "event_id": "r1", "reason_code": "suspended"}
        )


def test_frozen_market_regime_snapshot_is_causal_and_fails_closed():
    import datetime as dt

    from app.lib.strategy_engine.market_snapshot import frozen_regime_snapshot

    class Service:
        lookback_days = 60

        def classify_date(self, date):
            assert date == dt.datetime(2026, 10, 23)
            return "bear"

    decision = dt.datetime(2026, 10, 23)
    market = frozen_regime_snapshot(decision, service=Service())
    assert market["regime"] == "bear"
    assert market["regime_lookback_days"] == 60
    assert market["decision_date"] == "2026-10-23"
    with pytest.raises(ValueError, match="after decision"):
        frozen_regime_snapshot(
            decision, service=Service(),
            snapshot_as_of=dt.datetime(2026, 10, 24),
        )

    class Missing:
        lookback_days = 60

        def classify_date(self, date):
            return "unknown"

    with pytest.raises(ValueError, match="unavailable"):
        frozen_regime_snapshot(decision, service=Missing())


def test_risk_replay_halt_snapshot_never_reads_operational_store():
    from app.jobs.strategy_risk_replay import read_frozen_halt_snapshot

    assert read_frozen_halt_snapshot({"halted": False}) is False
    assert read_frozen_halt_snapshot({
        "halted": True, "changed_by": "research-test", "reason": "freeze",
    }) is True
    with pytest.raises(ValueError, match="boolean"):
        read_frozen_halt_snapshot({})
    with pytest.raises(ValueError, match="changed_by"):
        read_frozen_halt_snapshot({"halted": True})


def test_research_nav_returns_independent_daily_legacy_ledger_parity():
    from app.lib.strategy_engine.nav import QuoteView, simulate_paper_nav

    quotes = {
        "sh600000": {
            "2026-10-26": QuoteView(
                open_price=10, close_price=10,
                previous_close=10, stock_code="sh600000",
            ),
            "2026-10-27": QuoteView(
                open_price=9.01, close_price=9.01,
                previous_close=10, stock_code="sh600000",
            ),
            "2026-10-28": QuoteView(
                open_price=9, close_price=9,
                previous_close=9.01, stock_code="sh600000",
            ),
        }
    }
    result = simulate_paper_nav(
        prices=quotes,
        schedule=[
            {
                "date": "2026-10-26", "decision_date": "2026-10-23",
                "holdings": {"sh600000": 0.5},
            },
            {
                "date": "2026-10-27", "decision_date": "2026-10-26",
                "holdings": {},
            },
            {
                "date": "2026-10-28", "decision_date": "2026-10-27",
                "holdings": {},
            },
        ],
        initial_nav=10000,
    )
    parity = result["nav_parity"]
    assert len(parity["points"]) == 3
    assert parity["max_abs_delta_cny"] <= 0.01
    assert [x["ledger_nav"] for x in parity["points"]] == [
        x["nav"] for x in result["curve"]
    ]
    assert all(abs(x["delta"]) <= 0.01 for x in parity["points"])
    restored = ResearchSimulationLedger.rebuild(
        result["ledger_events"], initial_cash=10000
    )
    assert restored.snapshot() == result["ledger_snapshot"]


def test_research_market_regime_gate_uses_snapshot_and_halt_first():
    import datetime as dt

    from app.jobs.strategy_risk_replay import replay_with_market_regime
    from app.lib.strategy_engine.config import DEFAULT_STRATEGY_CONFIG
    from app.lib.strategy_engine.spec import legacy_to_spec

    class Bear:
        lookback_days = 60

        def classify_date(self, at):
            assert at == dt.datetime(2026, 10, 23)
            return "bear"

    spec = legacy_to_spec(DEFAULT_STRATEGY_CONFIG)["spec"]
    spec["risk"]["regime_policy"] = "bull_only"
    intents = [{
        "intent_id": "buy",
        "stock_code": "sh600001",
        "side": "BUY",
        "quantity": 100,
        "price": 2,
    }]
    account = {"cash": 1000, "nav": 1000, "positions": {}}
    result = replay_with_market_regime(
        spec=spec, account=account, intents=intents,
        decision_date=dt.datetime(2026, 10, 23), regime_service=Bear(),
    )
    assert result["rejected"][0]["reason_code"] == "REGIME_BLOCKED"
    assert result["evidence_kind"] == "REPLAY"
    halted = replay_with_market_regime(
        spec=spec, account=account, intents=intents,
        decision_date=dt.datetime(2026, 10, 23), regime_service=Bear(),
        halt_snapshot={
            "halted": True, "changed_by": "operator", "reason": "incident"
        },
    )
    assert halted["status"] == "HALTED"
    assert halted["rejected"][0]["reason_code"] == "HALTED"
