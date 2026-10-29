# -*- coding: utf-8 -*-
"""Allocation windows: isolation, refusal to burn the clock, and honest counting.

The 120-session window is the only irreversible resource in this program, so these
tests concentrate on the ways it can be wasted by accident: a window opened with
nothing able to fill it, one strategy's certification closing another's, or a
backfilled plan counting as forward evidence.
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "shared") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "shared"))

from app.lib.strategy_engine.allocation_window import (  # noqa: E402
    REQUIRED_SESSIONS,
    AllocationWindowError,
    allocation_forward_progress,
    plan_certification,
    validate_certification,
)

SPEC = {
    "spec_version": "2",
    "family": "asset_allocation",
    "accounting": {"corporate_actions": "price_only", "mark_price": "close_raw"},
    "benchmark": {"kind": "equal_weight_universe"},
}


def _identity(**overrides):
    base = validate_certification(SPEC, strategy_id="sleeve-stable", has_plan_path=True)
    base.update(overrides)
    return base


# --------------------------------------------------------------------------
# certification preconditions
# --------------------------------------------------------------------------


def test_allocation_certification_needs_no_model_version():
    identity = validate_certification(
        SPEC, strategy_id="sleeve-stable", has_plan_path=True
    )
    assert identity["strategy_id"] == "sleeve-stable"
    assert "model_version" not in identity


def test_rank_selection_spec_is_refused():
    spec = dict(SPEC, family="rank_selection")
    with pytest.raises(AllocationWindowError, match="only asset_allocation"):
        validate_certification(spec, strategy_id="s", has_plan_path=True)


def test_v1_spec_is_refused():
    spec = dict(SPEC, spec_version="1")
    with pytest.raises(AllocationWindowError, match="require a v2 declaration"):
        validate_certification(spec, strategy_id="s", has_plan_path=True)


def test_missing_accounting_policy_blocks_certification():
    spec = {key: value for key, value in SPEC.items() if key != "accounting"}
    with pytest.raises(AllocationWindowError, match="accounting policy"):
        validate_certification(spec, strategy_id="s", has_plan_path=True)


def test_missing_benchmark_blocks_certification():
    spec = {key: value for key, value in SPEC.items() if key != "benchmark"}
    with pytest.raises(AllocationWindowError, match="benchmark"):
        validate_certification(spec, strategy_id="s", has_plan_path=True)


def test_certification_without_a_plan_path_is_refused():
    """Opening the clock with nothing able to fill it wastes 120 sessions."""
    with pytest.raises(AllocationWindowError, match="120-session clock"):
        validate_certification(SPEC, strategy_id="s", has_plan_path=False)


def test_empty_strategy_id_is_refused():
    with pytest.raises(AllocationWindowError, match="strategy_id"):
        validate_certification(SPEC, strategy_id="  ", has_plan_path=True)


# --------------------------------------------------------------------------
# isolation
# --------------------------------------------------------------------------


def test_certifying_a_second_strategy_keeps_the_first_active():
    decision = plan_certification(
        identity=_identity(strategy_id="sleeve-aggressive"),
        spec_hash="a" * 64,
        active_windows=[{"strategy_id": "sleeve-stable", "status": "ACTIVE"}],
        today="2026-10-29",
    )
    assert decision["close"] == []
    assert decision["window"]["strategy_id"] == "sleeve-aggressive"


def test_recertifying_the_same_strategy_closes_its_predecessor():
    decision = plan_certification(
        identity=_identity(),
        spec_hash="b" * 64,
        active_windows=[{"strategy_id": "sleeve-stable", "status": "ACTIVE"}],
        today="2026-10-29",
    )
    assert decision["close"] == ["sleeve-stable"]
    assert decision["window"]["start_date"] == "2026-10-29"


def test_a_closed_window_is_not_closed_twice():
    decision = plan_certification(
        identity=_identity(),
        spec_hash="b" * 64,
        active_windows=[{"strategy_id": "sleeve-stable", "status": "CLOSED"}],
        today="2026-10-29",
    )
    assert decision["close"] == []


def test_start_date_is_the_certification_session():
    decision = plan_certification(
        identity=_identity(), spec_hash="c" * 64, active_windows=[], today="2026-11-02"
    )
    assert decision["window"]["start_date"] == "2026-11-02"


def test_window_records_the_measurement_identity():
    decision = plan_certification(
        identity=_identity(), spec_hash="c" * 64, active_windows=[], today="2026-10-29"
    )
    assert decision["window"]["accounting_policy"]["mark_price"] == "close_raw"
    assert decision["window"]["benchmark"]["kind"] == "equal_weight_universe"
    assert decision["window"]["hash_kind"] == "spec"


# --------------------------------------------------------------------------
# progress
# --------------------------------------------------------------------------


def _window(**overrides):
    base = {
        "strategy_id": "sleeve-stable",
        "spec_hash": "c" * 64,
        "start_date": "2026-10-26",
        "status": "ACTIVE",
    }
    base.update(overrides)
    return base


def test_progress_counts_only_qualifying_sessions():
    progress = allocation_forward_progress(
        window=_window(),
        decided_dates=["2026-10-26", "2026-10-27"],
        trading_days=["2026-10-23", "2026-10-26", "2026-10-27", "2026-10-28"],
    )
    # 10-23 predates the window and must not be counted.
    assert progress["observed_sessions"] == 2
    assert progress["sessions_in_window"] == 3
    assert progress["required_sessions"] == REQUIRED_SESSIONS


def test_a_missing_session_is_a_permanent_gap():
    progress = allocation_forward_progress(
        window=_window(),
        decided_dates=["2026-10-26", "2026-10-28"],
        trading_days=["2026-10-26", "2026-10-27", "2026-10-28"],
    )
    assert progress["gaps"] == ["2026-10-27"]
    assert progress["observed_sessions"] == 2
    assert progress["contiguous_leading_span"] == 1


def test_a_duplicate_date_counts_once():
    progress = allocation_forward_progress(
        window=_window(),
        decided_dates=["2026-10-26", "2026-10-26", "2026-10-26"],
        trading_days=["2026-10-26"],
    )
    assert progress["observed_sessions"] == 1


def test_dates_outside_the_window_are_ignored():
    progress = allocation_forward_progress(
        window=_window(),
        decided_dates=["2026-10-20", "2026-10-26"],
        trading_days=["2026-10-20", "2026-10-26"],
    )
    assert progress["observed_sessions"] == 1


def test_closed_window_stops_at_its_close_date():
    progress = allocation_forward_progress(
        window=_window(status="CLOSED", closed_at="2026-10-27"),
        decided_dates=["2026-10-26", "2026-10-28"],
        trading_days=["2026-10-26", "2026-10-27", "2026-10-28"],
    )
    assert progress["observed_sessions"] == 1
    assert progress["sessions_in_window"] == 2


def test_completion_needs_the_full_requirement():
    days = [
        f"2026-{(1 + index // 28):02d}-{(1 + index % 28):02d}" for index in range(5)
    ]
    progress = allocation_forward_progress(
        window=_window(start_date=days[0]), decided_dates=days, trading_days=days
    )
    assert progress["complete"] is False
    assert progress["remaining_sessions"] == REQUIRED_SESSIONS - 5


def test_invalid_window_status_is_rejected():
    with pytest.raises(AllocationWindowError, match="status"):
        allocation_forward_progress(
            window=_window(status="PAUSED"), decided_dates=[], trading_days=[]
        )
