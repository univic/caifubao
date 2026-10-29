# -*- coding: utf-8 -*-
"""The rank paper path can now execute a v2 declaration.

Pinned here: the state is built only from what the paper path records and refuses
when it cannot measure a stop; and a v2 rank plan really goes through
``plan_from_state``, so a declared stop-loss and maximum holding period are read
instead of approximated from the previous target book.
"""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "shared") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "shared"))

from app.lib.strategy_engine.rank_state import (  # noqa: E402
    RankStateError,
    build_decision_state,
)
from app.lib.strategy_engine.runner import assemble_daily_plan  # noqa: E402

HOLDINGS = [
    {"stock_code": "sh600000", "weight": 0.05},
    {"stock_code": "sh600001", "weight": 0.05},
]
META = {
    "sh600000": {"hold_count": 3, "entry_reason": "added_by_rank"},
    "sh600001": {"hold_count": 1, "entry_reason": "added_by_rank"},
}
CLOSES = {"sh600000": 10.0, "sh600001": 20.0}


def _state(**overrides):
    args = {
        "as_of": "2026-10-29",
        "previous_holdings": HOLDINGS,
        "held_meta": META,
        "reference_prices": {"sh600000": 9.0, "sh600001": 20.0},
        "closes": CLOSES,
        "initial_nav": 100_000.0,
        "board_lot": 100,
    }
    args.update(overrides)
    return build_decision_state(**args)


def test_state_is_built_from_recorded_prices():
    state = _state()
    assert [p["stock_code"] for p in state["positions"]] == ["sh600000", "sh600001"]
    assert state["cash"] == pytest.approx(100_000.0 * 0.9)


def test_quantity_is_the_notional_rounded_to_a_lot():
    state = _state()
    by_code = {p["stock_code"]: p for p in state["positions"]}
    # 100k * 5% = 5,000 at 10.0 -> 500 shares; at 20.0 -> 250 -> 200 shares.
    assert by_code["sh600000"]["quantity"] == 500
    assert by_code["sh600001"]["quantity"] == 200


def test_cost_basis_comes_from_the_recorded_reference():
    state = _state()
    by_code = {p["stock_code"]: p for p in state["positions"]}
    assert by_code["sh600000"]["average_cost"] == pytest.approx(9.0)


def test_hold_count_and_entry_reason_carry_over():
    state = _state()
    by_code = {p["stock_code"]: p for p in state["positions"]}
    assert by_code["sh600000"]["hold_count"] == 3
    assert by_code["sh600001"]["entry_reason"] == "added_by_rank"


def test_no_open_date_is_invented():
    """The paper path does not track entry dates, so the state must not claim one."""
    for position in _state()["positions"]:
        assert "opened_on" not in position


def test_a_held_name_without_a_cost_basis_is_refused():
    """Substituting today's close would leave the stop unable to ever fire."""
    with pytest.raises(RankStateError, match="no recorded cost basis"):
        _state(reference_prices={"sh600000": 9.0})


def test_a_held_name_without_a_close_keeps_its_basis_and_no_mark():
    """A missing close carries no mark: the planner defers that stop.

    The paper job additionally refuses to *record* a target with no close, so
    this is the library rule the planner relies on, not a promise that an
    unpriced holding is kept.
    """
    state = _state(closes={"sh600000": 10.0})
    by_code = {p["stock_code"]: p for p in state["positions"]}
    assert by_code["sh600001"]["average_cost"] == pytest.approx(20.0)
    # Priced at the recorded basis because no decision-date close exists.
    assert by_code["sh600001"]["quantity"] == 200
    assert state["blocked_exits"] == {}


def test_an_empty_book_is_a_valid_state():
    state = _state(previous_holdings=[], held_meta={})
    assert state["positions"] == []
    assert state["cash"] == pytest.approx(100_000.0)


def test_bad_inputs_are_refused():
    with pytest.raises(RankStateError, match="initial_nav"):
        _state(initial_nav=0)
    with pytest.raises(RankStateError, match="board_lot"):
        _state(board_lot=0)


# --------------------------------------------------------------------------
# the planner delegates when a state is supplied
# --------------------------------------------------------------------------


def _config():
    return {
        "score_model_version": "flip_wide_shadow_v1",
        "horizon": 20,
        "selection": {"mode": "top_n", "portfolio_size": 20},
        "weighting": "equal",
        "cash_reserve_pct": 0,
        "constraints": {},
    }


def _flags(count=30):
    return {
        f"sh60{index:04d}": {
            "is_st": 0,
            "is_bse": False,
            "trade_status": 1,
            "trade_amount": 1e9,
        }
        for index in range(count)
    }


def _predictions(count=30):
    return [
        SimpleNamespace(
            stock_code=f"sh60{index:04d}",
            score=float(count - index),
            percentile=0.9,
        )
        for index in range(count)
    ]


def test_a_declared_stop_loss_sells_a_held_name():
    """With a state, the exit contract is consumed; without one it is not."""
    import datetime as dt

    state = build_decision_state(
        as_of="2026-10-29",
        previous_holdings=[{"stock_code": "sh600000", "weight": 0.05}],
        held_meta={"sh600000": {"hold_count": 3, "entry_reason": "added_by_rank"}},
        reference_prices={"sh600000": 10.0},
        closes={"sh600000": 7.0},  # 30% below the cost basis
        initial_nav=100_000.0,
    )
    exit_policy = {
        "rank_below": 40,
        "stop_loss_pct": 0.20,
        "max_hold_sessions": 40,
        "on_ineligible": "sell_next_open",
    }
    plan = assemble_daily_plan(
        config=_config(),
        date=dt.datetime(2026, 10, 29),
        predictions=_predictions(),
        previous_holdings=[{"stock_code": "sh600000", "weight": 0.05}],
        flags=_flags(),
        held_meta={"sh600000": {"hold_count": 3, "entry_reason": "added_by_rank"}},
        decision_state=state,
        exit_policy=exit_policy,
        marks={"sh600000": 7.0},
    )
    assert plan["skipped"] is False
    codes = [holding["stock_code"] for holding in plan["target_holdings"]]
    assert "sh600000" not in codes
    assert plan["reasons"]["sh600000"] == "sold_stop_loss"


def test_the_legacy_path_is_unchanged_without_a_state():
    import datetime as dt

    plan = assemble_daily_plan(
        config=_config(),
        date=dt.datetime(2026, 10, 29),
        predictions=_predictions(),
        previous_holdings=None,
        flags=_flags(),
    )
    assert plan["skipped"] is False
    assert len(plan["target_holdings"]) == 20
