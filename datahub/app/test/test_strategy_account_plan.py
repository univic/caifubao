# -*- coding: utf-8 -*-
"""Small-account execution plan (C3): board lots, fees, cash floor, reporting.

The planner is a read-only preview of the fills ``simulate_paper_nav`` would
book: sells of names leaving the target first, then fee-aware board-lot buys
funded by the post-sell cash. These tests pin the contract the spec requires —
STAR 200/300 sizing, unaffordable skips, sells funding buys, projected
positions, BSE rejection, an unvaluable holding failing closed, limit
refusals, a non-negative cash result, deterministic output and the fee math.
"""

import json
import pathlib
import subprocess
import sys

import pytest

from app.lib.strategy_engine.account_plan import build_plan, is_bse_code, plan_json
from app.lib.strategy_engine.nav import QuoteView, simulate_paper_nav

D = "2026-01-05"
BASIC_CONFIG = {"score_model_version": "flip_wide_shadow_v1"}
ZERO_COSTS_CONFIG = {
    **BASIC_CONFIG,
    "execution": {
        "commission_rate": 0,
        "minimum_commission_cny": 0,
        "sell_stamp_duty_rate": 0,
        "transfer_fee_rate": 0,
        "slippage_per_side": 0,
    },
}
SCRIPT = (
    pathlib.Path(__file__).resolve().parents[2] / "scripts" / "strategy_account_plan.py"
)


def _quote(price, previous_close=None):
    return QuoteView(price, price, previous_close=previous_close or price)


# ---------------------------------------------------------------------------
# Board-lot sizing
# ---------------------------------------------------------------------------


def test_star_board_minimum_and_one_share_increments():
    """STAR buys at least 200 shares and then steps by 1 share; main-board and
    ChiNext names keep 100-share lots."""
    plan = build_plan(
        cash=100_000,
        positions={},
        target_weights={"sh600000": 0.025, "sh688001": 0.025},
        prices={
            "sh600000": _quote(10.0),
            "sh688001": _quote(10.0),
        },
        config=ZERO_COSTS_CONFIG,
        as_of=D,
    )
    quantities = {order["code"]: order["quantity"] for order in plan["orders"]}
    assert quantities["sh600000"] == 200  # 100-share lots: 250 -> 200
    assert quantities["sh688001"] == 250  # 200 minimum + 50 one-share steps


def test_star_buy_is_300_shares_not_200_or_400_when_the_budget_fits_three_hundred():
    plan = build_plan(
        cash=100_000,
        positions={},
        target_weights={"sh688001": 0.03},
        prices={"sh688001": _quote(10.0)},
        config=ZERO_COSTS_CONFIG,
        as_of=D,
    )
    assert [order["quantity"] for order in plan["orders"]] == [300]


def test_star_below_its_200_share_minimum_is_skipped_not_bought_in_lots():
    plan = build_plan(
        cash=100_000,
        positions={},
        target_weights={"sh688001": 0.01},
        prices={"sh688001": _quote(10.0)},
        config=ZERO_COSTS_CONFIG,
        as_of=D,
    )
    assert plan["orders"] == []
    assert plan["skipped"] == [
        {
            "code": "sh688001",
            "reason": "budget_below_board_minimum",
            "budget": 1_000.0,
            "minimum_quantity": 200,
            "price": 10.0,
        }
    ]


def test_unaffordable_target_is_skipped_not_partially_filled():
    plan = build_plan(
        cash=50_000,
        positions={},
        target_weights={"sh600000": 0.1},
        prices={"sh600000": _quote(100.0)},
        config=ZERO_COSTS_CONFIG,
        as_of=D,
    )
    assert plan["orders"] == []
    assert plan["skipped"] == [
        {
            "code": "sh600000",
            "reason": "budget_below_board_minimum",
            "budget": 5_000.0,
            "minimum_quantity": 100,
            "price": 100.0,
        }
    ]
    assert plan["cash_after"] == plan["cash_before"] == 50_000.0


# ---------------------------------------------------------------------------
# Sell-first funding and projected positions
# ---------------------------------------------------------------------------


def test_sells_are_planned_first_and_fund_the_buys():
    """A held name leaving the target pays for a buy that the pre-sell cash
    could not have funded (1000 CNY can only buy 200 shares at 5)."""
    plan = build_plan(
        cash=1_000,
        positions={"sh600000": 1_000},
        target_weights={"sz000001": 0.5},
        prices={
            "sh600000": _quote(10.0),
            "sz000001": _quote(5.0),
        },
        config=ZERO_COSTS_CONFIG,
        as_of=D,
    )
    assert [(o["side"], o["code"], o["quantity"]) for o in plan["orders"]] == [
        ("SELL", "sh600000", 1_000),
        ("BUY", "sz000001", 1_100),
    ]
    assert plan["orders"][0]["reason"] == "exit_not_in_target"
    assert plan["orders"][1]["reason"] == "enter_target"
    assert plan["aum"] == 11_000.0
    assert plan["cash_after"] == 5_500.0


def test_projected_position_adds_the_buy_to_the_held_quantity():
    """The spec's reconciliation scenario: 100 held + 200 bought -> 300 planned
    and 300 projected."""
    plan = build_plan(
        cash=2_000,
        positions={"sh600000": 100},
        target_weights={"sh600000": 1.0},
        prices={"sh600000": _quote(10.0)},
        config=ZERO_COSTS_CONFIG,
        as_of=D,
    )
    assert plan["aum"] == 3_000.0
    assert plan["orders"][0]["quantity"] == 200
    assert plan["orders"][0]["reason"] == "add_to_target"
    assert plan["target_quantities"] == {"sh600000": 300}
    assert plan["projected_positions"] == [
        {"code": "sh600000", "quantity": 300, "price": 10.0, "value": 3_000.0}
    ]
    assert plan["target_holdings"] == [{"stock_code": "sh600000", "quantity": 300}]


# ---------------------------------------------------------------------------
# Universe: BSE and unvaluable holdings
# ---------------------------------------------------------------------------


def test_bse_targets_are_rejected_and_never_sized():
    plan = build_plan(
        cash=10_000,
        positions={},
        target_weights={"830799": 0.2, "bj430047": 0.5},
        prices={
            "830799": _quote(10.0),
            "bj430047": _quote(10.0),
        },
        config=ZERO_COSTS_CONFIG,
        as_of=D,
    )
    assert plan["orders"] == []
    assert plan["skipped"] == [
        {"code": "830799", "reason": "bse_outside_universe"},
        {"code": "bj430047", "reason": "bse_outside_universe"},
    ]
    assert plan["target_quantities"] == {}
    assert is_bse_code("bj430047")
    assert is_bse_code("830799")
    assert is_bse_code("920001")
    assert not is_bse_code("sh600000")
    assert not is_bse_code("sz300750")


def test_a_held_bse_name_is_not_a_target_and_exits():
    plan = build_plan(
        cash=1_000,
        positions={"830799": 100},
        target_weights={"sh600000": 0.5},
        prices={
            "830799": _quote(10.0),
            "sh600000": _quote(10.0),
        },
        config=ZERO_COSTS_CONFIG,
        as_of=D,
    )
    assert [(o["side"], o["code"]) for o in plan["orders"]] == [
        ("SELL", "830799"),
        ("BUY", "sh600000"),
    ]
    assert plan["target_quantities"] == {"sh600000": 100}


@pytest.mark.parametrize("quote", [None, QuoteView(None, None), QuoteView(0, 0)])
def test_unvaluable_holding_fails_closed_with_name_and_date(quote):
    prices = {} if quote is None else {"sh600000": quote}
    with pytest.raises(ValueError) as excinfo:
        build_plan(
            cash=1_000,
            positions={"sh600000": 100},
            target_weights={},
            prices=prices,
            config=ZERO_COSTS_CONFIG,
            as_of=D,
        )
    message = str(excinfo.value)
    assert "sh600000" in message
    assert D in message


def test_a_supplied_aum_must_match_the_marked_account():
    common = {
        "cash": 10_000,
        "positions": {"sh600000": 100},
        "target_weights": {},
        "prices": {"sh600000": _quote(10.0)},
        "config": ZERO_COSTS_CONFIG,
        "as_of": D,
    }
    assert build_plan(aum=11_000, **common)["aum"] == 11_000.0
    with pytest.raises(ValueError, match="aum mismatch"):
        build_plan(aum=12_000, **common)


# ---------------------------------------------------------------------------
# Execution gate: limit verdicts are reported, never silently dropped
# ---------------------------------------------------------------------------


def test_limit_up_open_blocks_a_buy_and_is_reported():
    plan = build_plan(
        cash=100_000,
        positions={},
        target_weights={"sh600000": 0.5},
        prices={"sh600000": QuoteView(11.0, 11.0, previous_close=10.0)},
        config=ZERO_COSTS_CONFIG,
        as_of=D,
    )
    assert plan["orders"] == []
    assert plan["blocked"] == [
        {"code": "sh600000", "side": "BUY", "reason": "at_limit_up"}
    ]
    assert plan["cash_after"] == 100_000.0


def test_limit_down_open_blocks_a_sell_and_the_position_rolls_forward():
    plan = build_plan(
        cash=1_000,
        positions={"sh600000": 100},
        target_weights={},
        prices={"sh600000": QuoteView(9.0, 9.0, previous_close=10.0)},
        config=ZERO_COSTS_CONFIG,
        as_of=D,
    )
    assert plan["orders"] == []
    assert plan["blocked"] == [
        {"code": "sh600000", "side": "SELL", "reason": "at_limit_down"}
    ]
    assert plan["target_quantities"] == {"sh600000": 100}
    assert plan["projected_positions"] == [
        {"code": "sh600000", "quantity": 100, "price": 9.0, "value": 900.0}
    ]
    assert plan["cash_after"] == 1_000.0


def test_unknown_limit_verdict_is_blocked_not_traded():
    plan = build_plan(
        cash=100_000,
        positions={},
        target_weights={"sh600000": 0.5},
        prices={"sh600000": QuoteView(10.0, 10.0)},
        config=ZERO_COSTS_CONFIG,
        as_of=D,
    )
    assert plan["orders"] == []
    assert plan["blocked"] == [
        {"code": "sh600000", "side": "BUY", "reason": "limit_unknown"}
    ]


@pytest.mark.parametrize(
    ("quote", "reason"),
    [
        (None, "no_quote"),
        (QuoteView(10.0, 10.0, trade_status=0), "suspended"),
        (QuoteView(None, 10.0), "no_open"),
    ],
)
def test_an_unpriceable_target_is_blocked_not_dropped(quote, reason):
    prices = {} if quote is None else {"sh600000": quote}
    plan = build_plan(
        cash=100_000,
        positions={},
        target_weights={"sh600000": 0.5},
        prices=prices,
        config=ZERO_COSTS_CONFIG,
        as_of=D,
    )
    assert plan["orders"] == []
    assert plan["blocked"] == [{"code": "sh600000", "side": "BUY", "reason": reason}]


def test_a_blocked_sell_cannot_fund_a_buy():
    """A limit-down exit rolls forward, so the buy is sized from the cash on
    hand, never from the unsold position's mark."""
    plan = build_plan(
        cash=1_000,
        positions={"sh600000": 1_000},
        target_weights={"sz000001": 0.5},
        prices={
            "sh600000": QuoteView(9.0, 9.0, previous_close=10.0),
            "sz000001": _quote(5.0),
        },
        config=ZERO_COSTS_CONFIG,
        as_of=D,
    )
    assert [(o["side"], o["code"], o["quantity"]) for o in plan["orders"]] == [
        ("BUY", "sz000001", 200)
    ]
    assert plan["blocked"] == [
        {"code": "sh600000", "side": "SELL", "reason": "at_limit_down"}
    ]
    assert plan["target_quantities"] == {"sh600000": 1_000, "sz000001": 200}
    assert plan["cash_after"] == 0


# ---------------------------------------------------------------------------
# Cash floor
# ---------------------------------------------------------------------------


def test_the_cash_floor_is_never_violated():
    plan = build_plan(
        cash=50_000,
        positions={},
        target_weights={"sh600000": 1.0},
        prices={"sh600000": _quote(10.0)},
        config={**BASIC_CONFIG, "cash_reserve_pct": 0.1},
        as_of=D,
    )
    assert plan["cash_floor"] == 5_000.0
    assert plan["orders"][0]["quantity"] == 4_400
    assert plan["cash_after"] == pytest.approx(5_944.5486)
    assert plan["cash_after"] >= plan["cash_floor"]


def test_cash_below_the_configured_floor_fails_loudly():
    with pytest.raises(ValueError, match="cash floor"):
        build_plan(
            cash=1_000,
            positions={"sh600000": 1_000},
            target_weights={},
            prices={"sh600000": _quote(10.0)},
            config={**BASIC_CONFIG, "cash_reserve_pct": 0.1},
            as_of=D,
        )


def test_zero_aum_targets_are_reported_not_bought():
    plan = build_plan(
        cash=0,
        positions={},
        target_weights={"sh600000": 0.5},
        prices={"sh600000": _quote(10.0)},
        config=ZERO_COSTS_CONFIG,
        as_of=D,
    )
    assert plan["orders"] == []
    assert plan["skipped"] == [{"code": "sh600000", "reason": "zero_aum"}]
    assert plan["cash_after"] == 0


def test_negative_cash_fails_loudly():
    with pytest.raises(ValueError, match="cash must be >= 0"):
        build_plan(
            cash=-1,
            positions={},
            target_weights={},
            prices={},
            config=ZERO_COSTS_CONFIG,
            as_of=D,
        )


def test_a_plan_never_leaves_negative_cash():
    plan = build_plan(
        cash=10_000,
        positions={},
        target_weights={"sh600000": 0.4, "sz000001": 0.4, "sh688001": 0.2},
        prices={
            "sh600000": _quote(10.0),
            "sz000001": _quote(20.0),
            "sh688001": _quote(30.0),
        },
        config=BASIC_CONFIG,
        as_of=D,
    )
    assert plan["cash_after"] >= 0
    spend = sum(
        order["value"] + order["fees"]["commission"] + order["fees"]["transfer_fee"]
        for order in plan["orders"]
    )
    assert plan["cash_before"] - spend == pytest.approx(plan["cash_after"], abs=1e-5)


# ---------------------------------------------------------------------------
# Fee math and parity with the paper NAV engine
# ---------------------------------------------------------------------------


def test_buy_fees_use_slippage_minimum_commission_and_the_transfer_fee():
    plan = build_plan(
        cash=100_000,
        positions={},
        target_weights={"sh600000": 0.5},
        prices={"sh600000": _quote(10.0)},
        config=BASIC_CONFIG,
        as_of=D,
    )
    buy = plan["orders"][0]
    assert buy["side"] == "BUY"
    assert buy["quantity"] == 4_900
    assert buy["price"] == pytest.approx(10.01)  # open * (1 + 0.1 %)
    assert buy["value"] == pytest.approx(49_049.0)
    assert buy["fees"]["commission"] == pytest.approx(12.26225)  # 0.025 %, > 5
    assert buy["fees"]["stamp_duty"] == 0.0
    assert buy["fees"]["transfer_fee"] == pytest.approx(0.49049)  # 0.001 %
    spend = buy["value"] + buy["fees"]["commission"] + buy["fees"]["transfer_fee"]
    assert plan["cash_after"] == pytest.approx(100_000.0 - spend)


def test_sell_fees_include_stamp_duty_and_the_transfer_fee():
    plan = build_plan(
        cash=0,
        positions={"sh600000": 5_000},
        target_weights={},
        prices={"sh600000": _quote(10.0)},
        config=BASIC_CONFIG,
        as_of=D,
    )
    sell = plan["orders"][0]
    assert sell["side"] == "SELL"
    assert sell["quantity"] == 5_000
    assert sell["price"] == pytest.approx(9.99)  # open * (1 - 0.1 %)
    assert sell["fees"]["commission"] == pytest.approx(12.4875)
    assert sell["fees"]["stamp_duty"] == pytest.approx(24.975)  # 0.05 %
    assert sell["fees"]["transfer_fee"] == pytest.approx(0.4995)
    assert plan["cash_after"] == pytest.approx(49_912.038)


def test_plan_sizing_and_fees_match_the_paper_nav_engine():
    quote = _quote(10.0)
    plan = build_plan(
        cash=100_000,
        positions={},
        target_weights={"sh600000": 0.5},
        prices={"sh600000": quote},
        config=BASIC_CONFIG,
        as_of=D,
    )
    nav = simulate_paper_nav(
        prices={"sh600000": {D: quote}},
        schedule=[{"date": D, "holdings": {"sh600000": 0.5}}],
        initial_nav=100_000,
    )
    order, trade = plan["orders"][0], nav["trades"][0]
    assert order["quantity"] == trade["quantity"]
    assert order["price"] == pytest.approx(trade["price"])
    assert order["fees"] == pytest.approx(trade["costs"])


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_plan_is_byte_identical_for_the_same_inputs_and_any_dict_order():
    inputs = {
        "cash": 50_000,
        "positions": {"sh600000": 100, "sz000001": 200},
        "target_weights": {"sh600000": 0.2, "sh688001": 0.1, "sz000001": 0.15},
        "prices": {
            "sh600000": _quote(10.0),
            "sz000001": _quote(5.0),
            "sh688001": _quote(20.0),
        },
        "config": BASIC_CONFIG,
        "as_of": D,
    }
    first = build_plan(**inputs)
    assert plan_json(first) == plan_json(build_plan(**inputs))
    reordered = build_plan(
        cash=inputs["cash"],
        positions=dict(reversed(list(inputs["positions"].items()))),
        target_weights=dict(reversed(list(inputs["target_weights"].items()))),
        prices=dict(reversed(list(inputs["prices"].items()))),
        config=inputs["config"],
        as_of=D,
    )
    assert plan_json(reordered) == plan_json(first)
    assert reordered["input_hash"] == first["input_hash"]
    assert reordered["config_hash"] == first["config_hash"]


def test_a_fee_change_changes_the_recorded_config_hash():
    cheap = build_plan(
        cash=50_000,
        positions={},
        target_weights={"sh600000": 0.2},
        prices={"sh600000": _quote(10.0)},
        config=BASIC_CONFIG,
        as_of=D,
    )
    costly = build_plan(
        cash=50_000,
        positions={},
        target_weights={"sh600000": 0.2},
        prices={"sh600000": _quote(10.0)},
        config={**BASIC_CONFIG, "execution": {"transfer_fee_rate": 0.0001}},
        as_of=D,
    )
    assert cheap["config_hash"] != costly["config_hash"]
    assert (
        cheap["orders"][0]["fees"]["transfer_fee"]
        != (costly["orders"][0]["fees"]["transfer_fee"])
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _run_cli(*args):
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        capture_output=True,
        text=True,
        check=False,
        cwd=str(SCRIPT.parents[1]),
    )


def test_cli_help_documents_the_input_contract():
    result = _run_cli("--help")
    assert result.returncode == 0
    assert "--input" in result.stdout
    assert "trade_status" in result.stdout


def test_cli_emits_the_same_byte_identical_plan_twice(tmp_path):
    document = {
        "cash": 50_000,
        "positions": {},
        "target_weights": {"sh600000": 0.2},
        "prices": {
            "sh600000": {
                "open": 10.0,
                "trade_status": 1,
                "previous_close": 10.0,
            }
        },
        "config": {**BASIC_CONFIG, "cash_reserve_pct": 0.1},
        "as_of": D,
    }
    path = tmp_path / "plan_input.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    first = _run_cli("--input", str(path))
    second = _run_cli("--input", str(path))
    assert first.returncode == 0, first.stderr
    assert first.stdout == second.stdout
    plan = json.loads(first.stdout)
    assert plan["cash_floor"] == 5_000.0
    assert plan["research_only"] is True
    assert plan["orders_placed"] == 0
    assert [order["code"] for order in plan["orders"]] == ["sh600000"]

    written = tmp_path / "plan.json"
    result = _run_cli("--input", str(path), "--output", str(written))
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert json.loads(written.read_text(encoding="utf-8")) == plan


def test_cli_fails_closed_with_a_json_error_on_stderr(tmp_path):
    document = {
        "cash": 1_000,
        "positions": {"sh600000": 100},
        "target_weights": {},
        "prices": {},
        "config": BASIC_CONFIG,
        "as_of": D,
    }
    path = tmp_path / "plan_input.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    result = _run_cli("--input", str(path))
    assert result.returncode == 1
    assert result.stdout == ""
    # Importing the app package logs to stderr; the error document is last.
    error = json.loads(result.stderr.strip().splitlines()[-1])
    assert "sh600000" in error["error"]
    assert D in error["error"]
