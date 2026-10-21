# -*- coding: utf-8 -*-
"""Synthetic invariants for the causal T+1 research ledger."""

from __future__ import annotations

import pandas as pd
import pytest

from app.lib.factor_lab.ledger import LedgerConfig, replay_ledger


def _row(day, code, open_price=10.0, close=None, **extra):
    return {
        "date": pd.Timestamp(day),
        "stock_code": code,
        "open": open_price,
        "close": close if close is not None else open_price,
        "trade_status": 1,
        "limit_up": False,
        "limit_down": False,
        **extra,
    }


def _frame(rows):
    return pd.DataFrame(rows)


def test_decision_fills_next_open_and_marks_next_close():
    dates = pd.to_datetime(["2026-01-05", "2026-01-06"])
    frame = _frame(
        [
            _row(dates[0], "sh600000", 10.0, 10.0),
            _row(dates[1], "sh600000", 11.0, 12.0),
        ]
    )
    result = replay_ledger(
        frame,
        dates,
        {dates[0]: ["sh600000"]},
        LedgerConfig(initial_cash=50_000, max_positions=1),
    )
    first, second = result["nav"]
    assert first["nav"] == 50_000
    assert first["positions"] == 0
    assert len(result["fills"]) == 1
    assert result["fills"][0]["date"] == "2026-01-06"
    assert second["positions"] == 1
    assert second["nav"] > 50_000
    assert result["summary"]["turnover_initial"] == pytest.approx(
        49_500 / 50_000 / (1 / 365.25), rel=1e-6
    )


def test_cash_is_conserved_and_never_negative():
    dates = pd.to_datetime(["2026-01-05", "2026-01-06"])
    frame = _frame(
        [
            _row(dates[0], "sh600000", 10.0, 10.0),
            _row(dates[1], "sh600000", 10.0, 11.0),
        ]
    )
    result = replay_ledger(
        frame,
        dates,
        {dates[0]: ["sh600000"]},
        LedgerConfig(initial_cash=50_000, max_positions=1),
    )
    ending = result["summary"]
    assert ending["ending_cash"] >= 0
    assert ending["ending_cash"] + result["ending_positions"][
        "sh600000"
    ] * 100 * 11.0 == pytest.approx(result["nav"][-1]["nav"], abs=0.02)


def test_blocked_exit_prevents_new_names_at_the_cap():
    dates = pd.to_datetime(["2026-01-05", "2026-01-06"])
    held = [f"sh60{i:04d}" for i in range(10)]
    new = [f"sh61{i:04d}" for i in range(10)]
    rows = []
    for code in held:
        rows.append(_row(dates[0], code, 10.0, 10.0))
        rows.append(_row(dates[1], code, 10.0, 10.0, limit_down=True))
    for code in new:
        rows.append(_row(dates[1], code, 10.0, 10.0))
    frame = _frame(rows)
    result = replay_ledger(
        frame,
        dates,
        {dates[0]: new},
        LedgerConfig(initial_cash=50_000, max_positions=10),
        initial_positions={code: 1 for code in held},
        initial_prices={code: 10.0 for code in held},
    )
    assert result["summary"]["max_actual_positions"] <= 10
    assert result["summary"]["ending_positions"] == 10
    assert any(item["reason"] == "max_positions" for item in result["blocked_orders"])


def test_share_multiplier_and_cash_dividend_are_applied():
    day = pd.Timestamp("2026-01-05")
    frame = _frame([_row(day, "sh600000", 10.0, 10.0)])
    actions = pd.DataFrame(
        [
            {
                "code": "sh600000",
                "ex_date": day,
                "share_multiplier": 2.0,
                "cash_per_share": 1.0,
            }
        ]
    )
    result = replay_ledger(
        frame,
        [day],
        {},
        LedgerConfig(initial_cash=50_000, max_positions=1),
        initial_positions={"sh600000": 1},
        initial_prices={"sh600000": 10.0},
        corporate_actions=actions,
    )
    assert result["ending_positions"]["sh600000"] == 2
    assert result["nav"][0]["cash"] == pytest.approx(50_100)
    assert result["nav"][0]["book"] == pytest.approx(2_000)


def test_write_off_uses_board_lots():
    day = pd.Timestamp("2026-01-05")
    frame = _frame([_row(day, "sh600000", 10.0, 10.0)])
    exits = pd.DataFrame([{"code": "sh600000", "date": day, "price": 0.0}])
    result = replay_ledger(
        frame,
        [day],
        {},
        LedgerConfig(initial_cash=50_000, max_positions=1),
        initial_positions={"sh600000": 1},
        initial_prices={"sh600000": 10.0},
        delist_exits=exits,
    )
    assert result["summary"]["write_off_cny"] == pytest.approx(1_000)
    assert result["summary"]["ending_positions"] == 0
    assert result["nav"][0]["nav"] == 50_000


def test_stale_quote_is_carried_not_zeroed():
    first = pd.Timestamp("2026-01-05")
    sessions = pd.date_range(first, periods=22, freq="B")
    frame = _frame([_row(first, "sh600000", 10.0, 10.0)])
    result = replay_ledger(
        frame,
        sessions,
        {},
        LedgerConfig(initial_cash=50_000, max_positions=1),
        initial_positions={"sh600000": 1},
        initial_prices={"sh600000": 10.0},
    )
    assert result["summary"]["max_actual_positions"] == 1
    assert result["summary"]["write_off_cny"] == 0
    assert result["nav"][-1]["nav"] == pytest.approx(50_000 + 1_000)
    assert result["summary"]["stale_names"] == 1


def test_duplicate_panel_keys_are_rejected():
    day = pd.Timestamp("2026-01-05")
    frame = _frame([_row(day, "sh600000"), _row(day, "sh600000")])
    with pytest.raises(ValueError, match="duplicate"):
        replay_ledger(frame, [day], {}, LedgerConfig(initial_cash=50_000))


def test_initial_positions_above_cap_are_rejected():
    day = pd.Timestamp("2026-01-05")
    frame = _frame([_row(day, "sh600000")])
    with pytest.raises(ValueError, match="strict cap"):
        replay_ledger(
            frame,
            [day],
            {},
            LedgerConfig(initial_cash=50_000, max_positions=0),
            initial_positions={"sh600000": 1},
            initial_prices={"sh600000": 10.0},
        )


def test_positive_months_excludes_the_partial_tail():
    first = pd.Timestamp("2026-01-05")
    sessions = pd.date_range(first, periods=70, freq="B")
    frame = _frame([_row(day, "sh600000", 10.0, 10.0) for day in sessions])
    result = replay_ledger(
        frame,
        sessions,
        {},
        LedgerConfig(initial_cash=50_000, max_positions=1),
        initial_positions={"sh600000": 1},
        initial_prices={"sh600000": 10.0},
    )
    assert result["summary"]["complete_months"] == 2
