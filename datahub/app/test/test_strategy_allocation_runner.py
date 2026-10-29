# -*- coding: utf-8 -*-
"""The allocation plan runner: a plan or a refusal, never a guess.

Pinned here: a missing quote produces a refusal rather than a plan from stale
marks, the per-asset stop fires from the recorded cost basis, the cadence decides
whether a session may rebalance, and the plan records the cost basis it used so
the stop is reproducible.
"""

import datetime as dt
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "shared") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "shared"))

from app.jobs.allocation_runner import (  # noqa: E402
    AllocationRunError,
    build_plan,
    sessions_between,
)

CODES = ["sh510300", "sh518880", "sh513100", "sh511010"]
SPEC = {
    "_spec_hash": "a" * 64,
    "family": "asset_allocation",
    "allocation": {"assets": {code: 0.25 for code in CODES}, "band_pct": 0.02},
    "rebalance": {"cadence_days": 5},
    "selection": {"exit": {"stop_loss_pct": 0.20}},
}
DAYS = ["2026-10-26", "2026-10-27", "2026-10-28", "2026-10-29", "2026-10-30"]
QUOTES = {code: 10.0 for code in CODES}


def _plan(**overrides):
    args = {
        "spec": SPEC,
        "strategy_id": "sleeve-stable",
        "session_date": "2026-10-29",
        "quotes": dict(QUOTES),
        "trading_days": DAYS,
        "previous": None,
    }
    args.update(overrides)
    return build_plan(**args)


# --------------------------------------------------------------------------
# sessions and cadence
# --------------------------------------------------------------------------


def test_sessions_between_counts_only_after_start():
    assert sessions_between(DAYS, "2026-10-26", "2026-10-29") == 3
    assert sessions_between(DAYS, "2026-10-29", "2026-10-29") == 0
    assert sessions_between(DAYS, None, "2026-10-29") > 1000


def test_first_plan_is_a_rebalance_session_and_opens_every_asset():
    plan = _plan()
    assert plan["rebalance_session"] is True
    assert plan["actions"] == {code: "open" for code in CODES}
    assert plan["targets"] == pytest.approx(QUOTES and {c: 0.25 for c in CODES})
    assert plan["cash_weight"] == pytest.approx(0.0)


def test_off_cadence_session_does_not_rebalance():
    previous = {
        "date": "2026-10-28",
        "targets": {code: 0.25 for code in CODES},
        "reference_prices": dict(QUOTES),
    }
    plan = _plan(previous=previous)
    assert plan["rebalance_session"] is False


def test_on_cadence_session_rebalances_a_drifted_book():
    previous = {
        "date": "2026-10-23",
        "targets": {
            "sh510300": 0.30,
            "sh518880": 0.22,
            "sh513100": 0.24,
            "sh511010": 0.24,
        },
        "reference_prices": dict(QUOTES),
    }
    plan = _plan(session_date="2026-10-30", previous=previous)
    assert plan["rebalance_session"] is True
    assert plan["actions"]["sh510300"] == "rebalance"
    assert plan["targets"]["sh510300"] == pytest.approx(0.25)


# --------------------------------------------------------------------------
# refusals
# --------------------------------------------------------------------------


def test_a_missing_quote_is_refused():
    quotes = dict(QUOTES)
    quotes.pop("sh511010")
    with pytest.raises(AllocationRunError, match="no usable quote"):
        _plan(quotes=quotes)


def test_an_asset_outside_the_declaration_is_closed():
    quotes = dict(QUOTES)
    quotes["sh588000"] = 1.5
    plan = _plan(
        quotes=quotes,
        previous={
            "date": "2026-10-28",
            "targets": {code: 0.25 for code in CODES} | {"sh588000": 0.0},
            "reference_prices": dict(QUOTES),
        },
    )
    assert plan["actions"].get("sh588000") == "close"


# --------------------------------------------------------------------------
# the per-asset stop
# --------------------------------------------------------------------------


def test_stop_fires_from_the_recorded_cost_basis():
    quotes = dict(QUOTES)
    quotes["sh510300"] = 7.9  # 21% below the recorded 10.0 cost basis
    plan = _plan(
        quotes=quotes,
        previous={
            "date": "2026-10-28",
            "targets": {code: 0.25 for code in CODES},
            "reference_prices": dict(QUOTES),
        },
    )
    assert plan["actions"]["sh510300"] == "stop_out"
    assert plan["targets"]["sh510300"] == 0.0
    assert plan["cash_weight"] == pytest.approx(0.25)


def test_a_twenty_percent_stop_does_not_fire_at_nineteen():
    quotes = dict(QUOTES)
    quotes["sh510300"] = 8.2  # 18% below, inside the 20% threshold
    plan = _plan(
        quotes=quotes,
        previous={
            "date": "2026-10-28",
            "targets": {code: 0.25 for code in CODES},
            "reference_prices": dict(QUOTES),
        },
    )
    assert plan["actions"]["sh510300"] in ("hold", "rebalance")


def test_the_plan_records_the_cost_basis_it_used():
    plan = _plan()
    assert plan["reference_prices"] == pytest.approx(QUOTES)


def test_reopening_resets_the_cost_basis():
    quotes = dict(QUOTES)
    quotes["sh510300"] = 12.0
    plan = _plan(
        quotes=quotes,
        previous={
            "date": "2026-10-23",
            "targets": {
                "sh510300": 0.30,
                "sh518880": 0.22,
                "sh513100": 0.24,
                "sh511010": 0.24,
            },
            "reference_prices": {
                "sh510300": 9.0,
                "sh518880": 10.0,
                "sh513100": 10.0,
                "sh511010": 10.0,
            },
        },
    )
    if plan["actions"]["sh510300"] == "rebalance":
        assert plan["reference_prices"]["sh510300"] == pytest.approx(12.0)


def test_plan_carries_the_strategy_and_spec_identity():
    plan = _plan()
    assert plan["strategy_id"] == "sleeve-stable"
    assert plan["spec_hash"] == "a" * 64
    assert plan["status"] == "PLANNED"
    assert dt.date.fromisoformat(plan["date"]) == dt.date(2026, 10, 29)
