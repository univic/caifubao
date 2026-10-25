"""Pure Research FORWARD gap checks: missed sessions never become replay evidence."""

import datetime as dt
from zoneinfo import ZoneInfo

from app.jobs.research_forward_watch import evaluate_forward_gap

CST = ZoneInfo("Asia/Shanghai")
D1, D2, D3 = (dt.date(2026, 10, 23), dt.date(2026, 10, 26), dt.date(2026, 10, 27))
CAL = [D1, D2, D3, dt.date(2026, 10, 28)]


def _window():
    return {
        "status": "ACTIVE",
        "model_version": "shadow",
        "horizon": 20,
        "hash_kind": "spec",
        "config_hash": "abc123",
        "start_date": D1,
    }


def _run(day, **overrides):
    value = {
        "date": day,
        "status": "COMPLETED",
        "evidence_kind": "FORWARD",
        "config_hash": "abc123",
        "hash_kind": "spec",
        "model_version": "shadow",
        "horizon": 20,
    }
    value.update(overrides)
    return value


def _check(*, now=None, calendar=None, window=None, runs=None, job_runs=None):
    return evaluate_forward_gap(
        now=now or dt.datetime(2026, 10, 27, 23, 10, tzinfo=CST),
        calendar=calendar if calendar is not None else CAL,
        window=window if window is not None else _window(),
        runs=runs if runs is not None else [_run(D1), _run(D2), _run(D3)],
        job_runs=job_runs if job_runs is not None else [],
    )


def test_complete_unique_sessions_ignore_duplicate_attempts():
    result = _check(runs=[_run(D1), _run(D2), _run(D3), _run(D3)])
    assert result["healthy"]
    assert result["forward_count"] == 3
    assert result["status"] == "COMPLETE"
    assert result["automated_job_success_today"] is False


def test_missing_forward_day_reported_as_permanent_gap():
    result = _check(runs=[_run(D1), _run(D3)])
    assert result["status"] == "MISSING_FORWARD_SESSIONS"
    assert result["missing_dates"] == ["2026-10-26"]
    assert not result["healthy"]


def test_replay_and_wrong_spec_never_count_as_forward_evidence():
    result = _check(
        runs=[
            _run(D1),
            _run(D2, evidence_kind="REPLAY"),
            _run(D3, config_hash="wrong"),
        ]
    )
    assert result["forward_count"] == 1
    assert result["invalid_runs_excluded"] == 2
    assert result["missing_dates"] == ["2026-10-26", "2026-10-27"]


def test_matching_job_success_is_reported_but_not_required_for_manual_smoke():
    result = _check(
        job_runs=[
            {
                "status": "SUCCESS",
                "summary": {"target": "2026-10-27", "config_hash": "abc123"},
            }
        ]
    )
    assert result["automated_job_success_today"] is True


def test_holiday_does_not_create_false_missing_session():
    today = dt.datetime(2026, 10, 25, 23, 10, tzinfo=CST)
    result = _check(now=today)
    assert result["status"] == "NON_TRADING_DAY"
    assert result["healthy"] is True


def test_missing_window_or_future_calendar_is_loud():
    assert _check(window={})["status"] == "NO_ACTIVE_WINDOW"
    assert _check(calendar=[D1, D2, D3])["status"] == "CALENDAR_MISSING_FUTURE"


def test_no_alert_before_monitor_slot():
    t = dt.datetime(2026, 10, 27, 22, 30, tzinfo=CST)
    assert _check(now=t)["status"] == "NOT_DUE"
    assert _check(now=t)["healthy"] is False
