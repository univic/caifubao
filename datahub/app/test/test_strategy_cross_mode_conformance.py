# -*- coding: utf-8 -*-
"""Shared research-mode golden vectors; no live trading account is instantiated."""

import copy

import pytest

from caifubao_contracts.strategy_execution import (
    adjusted_price,
    execution_fees,
)
from caifubao_contracts.strategy_modes import require_research_mode
from app.lib.strategy_engine.config import PAPER_EXECUTION
from app.lib.strategy_engine.nav import QuoteView, simulate_paper_nav


def _quotes():
    return {
        "sh600000": {
            "2026-10-26": QuoteView(
                open_price=10, close_price=10,
                previous_close=10, stock_code="sh600000",
            ),
            "2026-10-27": QuoteView(
                open_price=10.5, close_price=10.5,
                previous_close=10, stock_code="sh600000",
            ),
        },
    }


def _schedule():
    return [
        {
            "date": "2026-10-26",
            "decision_date": "2026-10-23",
            "holdings": {"sh600000": 0.5},
        },
        {
            "date": "2026-10-27",
            "decision_date": "2026-10-26",
            "holdings": {},
        },
    ]


def test_three_research_modes_have_identical_fills_nav_and_event_replay():
    results = []
    for mode in ("BACKTEST", "REPLAY", "RESEARCH_SIMULATION"):
        require_research_mode(mode)
        results.append(
            simulate_paper_nav(
                prices=_quotes(), schedule=_schedule(), mode=mode,
                initial_nav=10000, spec_hash="frozen-identical-spec",
            )
        )
    assert results[0] == results[1] == results[2]
    original = results[0]
    assert [fill["side"] for fill in original["trades"]] == ["BUY", "SELL"]
    assert all(
        fill["quantity"] % 100 == 0 for fill in original["trades"]
    )
    assert len(original["nav_parity"]["points"]) == 2
    assert original["nav_parity"]["max_abs_delta_cny"] <= 0.01
    assert original["ledger_snapshot"]["positions"] == {}
    before = copy.deepcopy(original)
    simulate_paper_nav(
        prices=_quotes(), schedule=_schedule(),
        mode="RESEARCH_SIMULATION", initial_nav=10000,
        spec_hash="frozen-identical-spec",
    )
    assert original == before


@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_shared_trading_conformance_vector_is_pure_and_side_correct(side):
    from app.lib.strategy_engine.nav import order_cost, exec_price

    cfg = dict(PAPER_EXECUTION)
    price = exec_price(10.0, side, cfg)
    expected = adjusted_price(10.0, side, cfg["slippage_per_side"])
    assert price == expected
    quote = order_cost(price, 100, side, cfg)
    from caifubao_contracts.friction import CostProfile

    profile = CostProfile(
        commission_rate=cfg["commission_rate"],
        minimum_commission_cny=cfg["minimum_commission_cny"],
        sell_stamp_duty_rate=cfg["sell_stamp_duty_rate"],
        transfer_fee_rate=cfg["transfer_fee_rate"],
        slippage_per_side=cfg["slippage_per_side"],
        board_lot=cfg["board_lot"],
    )
    assert {key: quote[key] for key in (
        "commission", "stamp_duty", "transfer_fee"
    )} == execution_fees(price, 100, side, profile)
    assert quote["value"] == pytest.approx(price * 100)
    assert (quote["stamp_duty"] > 0) is (side == "SELL")


def test_pinned_shared_conformance_vectors_are_reusable_by_trading():
    from caifubao_contracts.friction import DEFAULT_COST_PROFILE
    from caifubao_contracts.strategy_conformance import GOLDEN_EXECUTION_V1
    from caifubao_contracts.strategy_execution import board_lot_rule

    for row in GOLDEN_EXECUTION_V1:
        minimum, step = board_lot_rule(row["code"])
        assert (minimum, step) == (row["board_minimum"], row["board_step"])
        price = adjusted_price(
            row["open"], row["side"], DEFAULT_COST_PROFILE.slippage_per_side
        )
        assert price == pytest.approx(row["price"])
        fees = execution_fees(
            price, row["quantity"], row["side"], DEFAULT_COST_PROFILE
        )
        for key in ("commission", "transfer_fee", "stamp_duty"):
            assert fees[key] == pytest.approx(row[key])
