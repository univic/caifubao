"""Tests for the health watcher (job failures + freshness gaps)."""

from collections import Counter

import pytest

from app.jobs.health_watcher import _build_report


def test_report_is_healthy_when_no_issues():
    report = _build_report(
        failed_jobs=[],
        skipped_jobs=[],
        stale_assets=[],
        no_data_assets=[],
        latest_dates={"quote": Counter({"2026-08-28": 5208})},
    )

    assert report["healthy"] is True
    assert report["issues"] == []
    assert report["freshness"]["stale_count"] == 0
    assert report["freshness"]["latest_dates"]["quote"]["2026-08-28"] == 5208


def test_report_flags_failed_jobs_as_issues():
    report = _build_report(
        failed_jobs=[
            {
                "job_name": "datahub_quote_stock_daily",
                "status": "FAILED",
                "started_at": "2026-08-28 18:00:11",
                "error_message": "DeadlineExceeded",
            }
        ],
        skipped_jobs=[],
        stale_assets=[],
        no_data_assets=[],
        latest_dates={},
    )

    assert report["healthy"] is False
    assert any(
        "datahub_quote_stock_daily FAILED" in issue for issue in report["issues"]
    )
    assert report["job_runs"]["failed"][0]["error_message"] == "DeadlineExceeded"


def test_report_flags_no_data_assets_as_issues():
    report = _build_report(
        failed_jobs=[],
        skipped_jobs=[],
        stale_assets=[],
        no_data_assets=[{"code": "sz999999", "asset": "daily_quote"}],
        latest_dates={},
    )

    assert report["healthy"] is False
    assert any("1 NO_DATA assets" in issue for issue in report["issues"])
    assert report["freshness"]["no_data_count"] == 1


def test_stale_assets_are_reported_but_not_fatal():
    # Suspended stocks legitimately stay STALE; must not fail the health job.
    report = _build_report(
        failed_jobs=[],
        skipped_jobs=[],
        stale_assets=[
            {"code": "sz000635", "asset": "daily_quote", "latest": "2026-08-27"}
        ],
        no_data_assets=[],
        latest_dates={"quote": Counter({"2026-08-28": 5208, "2026-08-25": 4})},
    )

    assert report["healthy"] is True
    assert report["freshness"]["stale_count"] == 1
    assert report["freshness"]["stale_assets"][0]["code"] == "sz000635"


def test_skipped_jobs_are_reported_but_not_issues():
    report = _build_report(
        failed_jobs=[],
        skipped_jobs=[
            {
                "job_name": "datahub_signal_daily",
                "status": "SKIPPED",
                "started_at": "2026-08-28 18:30:00",
                "error_message": None,
            }
        ],
        stale_assets=[],
        no_data_assets=[],
        latest_dates={},
    )

    # SKIPPED (e.g. dependency-gate) is informational, not an issue.
    assert report["healthy"] is True
    assert report["job_runs"]["skipped"][0]["job_name"] == "datahub_signal_daily"


def test_cli_rejects_zero_hours():
    from app.jobs import health_watcher

    with pytest.raises(SystemExit) as excinfo:
        health_watcher.main(["--hours", "0"])
    assert excinfo.value.code == 2


def test_cli_help_exits_zero():
    from app.jobs import health_watcher

    with pytest.raises(SystemExit) as excinfo:
        health_watcher.main(["--help"])
    assert excinfo.value.code == 0
