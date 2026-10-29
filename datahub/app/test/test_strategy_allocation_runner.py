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
    "spec_version": "2",
    "universe": {"market": "CN_A", "as_of": "PIT"},
    "selection": {
        "mode": "top_n",
        "portfolio_size": 4,
        "exit": {"stop_loss_pct": 0.20},
    },
    "sizing": {"weighting": "equal", "cash_reserve_pct": 0},
    "constraints": {},
    "rebalance": {"cadence_days": 5},
    "risk": {},
    "execution": {},
    "evidence": {
        "timing_version": "paper_causal_v1",
        "policy": "research",
        "kill_criteria": {
            "min_excess_per_period": 0.0,
            "max_drawdown": 0.20,
            "review_after_sessions": 60,
        },
    },
    "initial_nav": 50000,
    "family": "asset_allocation",
    "allocation": {"assets": {code: 0.25 for code in CODES}, "band_pct": 0.02},
    "accounting": {"corporate_actions": "price_only", "mark_price": "close_hfq"},
    "benchmark": {"kind": "equal_weight_universe"},
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
    """A rebalance resets the basis the stop is measured against."""
    quotes = dict(QUOTES)
    quotes["sh510300"] = 12.0
    plan = _plan(
        session_date="2026-10-30",  # far enough back to be a rebalance session
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
    assert plan["rebalance_session"] is True
    assert plan["actions"]["sh510300"] == "rebalance"
    assert plan["reference_prices"]["sh510300"] == pytest.approx(12.0)
    # Assets that were only held keep their original basis.
    assert plan["reference_prices"]["sh518880"] == pytest.approx(10.0)


def test_plan_carries_the_strategy_and_spec_identity():
    plan = _plan()
    assert plan["strategy_id"] == "sleeve-stable"
    assert plan["spec_hash"] == "a" * 64
    assert plan["status"] == "PLANNED"
    assert dt.date.fromisoformat(plan["date"]) == dt.date(2026, 10, 29)


# --------------------------------------------------------------------------
# risk budget and the job entry point
# --------------------------------------------------------------------------


def test_a_configured_drawdown_stop_is_refused_not_ignored():
    """This runner has no high-water mark, so it must refuse the declaration."""
    spec = dict(SPEC, risk={"max_drawdown_pct": 0.2})
    with pytest.raises(AllocationRunError, match="high-water mark"):
        build_plan(
            spec=spec,
            strategy_id="sleeve-stable",
            session_date="2026-10-29",
            quotes=dict(QUOTES),
            trading_days=DAYS,
            previous=None,
        )


def test_a_regime_policy_is_refused_without_a_classifier():
    spec = dict(SPEC, risk={"regime_policy": "bear_reduce_only"})
    with pytest.raises(AllocationRunError, match="regime classifier"):
        build_plan(
            spec=spec,
            strategy_id="sleeve-stable",
            session_date="2026-10-29",
            quotes=dict(QUOTES),
            trading_days=DAYS,
            previous=None,
        )


def test_a_turnover_breach_refuses_the_plan():
    spec = dict(SPEC, risk={"max_turnover_pct": 0.01})
    # A single-asset book being spread across four names trades 75% of the
    # sleeve, far above a 1% budget.
    previous = {
        "date": "2026-10-20",
        "targets": {"sh510300": 1.0},
        "reference_prices": dict(QUOTES),
    }
    with pytest.raises(AllocationRunError, match="refused the plan"):
        build_plan(
            spec=spec,
            strategy_id="sleeve-stable",
            session_date="2026-10-29",
            quotes=dict(QUOTES),
            trading_days=DAYS,
            previous=previous,
        )


def test_the_risk_verdict_is_recorded_on_the_plan():
    spec = dict(SPEC, risk={"max_positions": 2})
    plan = build_plan(
        spec=spec,
        strategy_id="sleeve-stable",
        session_date="2026-10-29",
        quotes=dict(QUOTES),
        trading_days=DAYS,
        previous=None,
    )
    assert plan["risk_verdict"]["admissible"] is True
    # Four assets against a two-position cap scales the book down, not up.
    assert plan["risk_verdict"]["scale"] == pytest.approx(0.5)
    assert sum(plan["targets"].values()) == pytest.approx(0.5)
    assert plan["cash_weight"] == pytest.approx(0.5)


def test_the_job_has_a_real_entry_point():
    from app.jobs import allocation_runner

    assert callable(allocation_runner.main)
    # Without a subcommand argparse exits non-zero instead of returning silently.
    with pytest.raises(SystemExit):
        allocation_runner.main([])


def test_a_refused_run_exits_non_zero(tmp_path):
    from app.jobs import allocation_runner

    spec = dict(SPEC)
    spec.pop("_spec_hash")
    spec["risk"] = {"max_drawdown_pct": 0.2}
    path = tmp_path / "declaration.json"
    import json as _json

    path.write_text(_json.dumps(spec), encoding="utf-8")
    code = allocation_runner.main(
        ["run", "--spec", str(path), "--strategy-id", "sleeve-stable", "--dry-run"]
    )
    assert code == 2
