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


def test_duplicate_completed_forward_sessions_fail_closed():
    result = _check(runs=[_run(D1), _run(D2), _run(D3), _run(D3)])
    assert not result["healthy"]
    assert result["forward_count"] == 3
    assert result["status"] == "DUPLICATE_FORWARD_SESSIONS"
    assert result["duplicate_forward_dates"] == ["2026-10-27"]
    assert result["automation_healthy"] is False


def test_manual_recovery_preserves_evidence_without_automation_credit():
    result = _check()
    assert result["healthy"]
    assert result["status"] == "COMPLETE"
    assert result["automated_job_success_today"] is False
    assert result["automation_healthy"] is False
    assert result["capture_source_today"] == "MANUAL_OR_UNVERIFIED"


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
    assert result["automated_job_success_today"] is False
    # JobRun from a manually cloned CronJob is indistinguishable from cron
    # without trusted Kubernetes ownerReference verification.
    assert result["automation_healthy"] is False
    assert result["scheduler_attestation"] == "NOT_VERIFIED_BY_KUBERNETES"
    assert result["job_run_success_today"] is True
    assert result["capture_source_today"] == "JOB_RUN_ORIGIN_UNVERIFIED"


def test_failed_capture_job_is_visible_even_if_operator_capture_succeeded():
    result = _check(
        job_runs=[
            {
                "status": "FAILED",
                "summary": {},
                "started_at": dt.datetime(2026, 10, 27, 15, 15),
            }
        ]
    )
    assert result["status"] == "CAPTURE_JOB_FAILURE"
    assert result["forward_count"] == 3
    assert result["failed_capture_jobs_today"] == 1
    assert result["healthy"] is False
    assert result["automation_healthy"] is False


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


def test_cloned_manual_cronjob_success_cannot_claim_unattended_run():
    result = _check(
        job_runs=[
            {
                "status": "SUCCESS",
                "trigger": "cron",
                "source": "k8s-cronjob",
                "summary": {"target": "2026-10-27", "config_hash": "abc123"},
            }
        ]
    )
    assert result["healthy"] is True
    assert result["automated_job_success_today"] is False
    assert result["automation_healthy"] is False
    assert result["scheduler_attestation"] == "NOT_VERIFIED_BY_KUBERNETES"
