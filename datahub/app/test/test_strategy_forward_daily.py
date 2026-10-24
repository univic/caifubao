"""Fail-closed causal preflight for the suspended research daily CronJob."""

import datetime as dt
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from app.jobs.strategy_forward_daily import verify_daily_gate

CST = ZoneInfo("Asia/Shanghai")
TODAY = dt.date(2026, 10, 26)
NOW = dt.datetime(2026, 10, 26, 22, 15, tzinfo=CST)
CALENDAR = [
    dt.datetime(2026, 10, 23),
    dt.datetime(2026, 10, 26),
    dt.datetime(2026, 10, 27),
]


def _gate(**overrides):
    report = {
        "overall": "READY",
        "expected_session": TODAY.isoformat(),
        "checks": [
            {"id": name, "status": "PASS"}
            for name in (
                "quote_freshness",
                "scoring_freshness",
                "signal_freshness",
                "halt_switch",
            )
        ],
    }
    window = SimpleNamespace(
        status="ACTIVE",
        model_version="shadow_v1",
        horizon=20,
        config_hash="abc123",
        hash_kind="spec",
        start_date=dt.datetime(2026, 10, 26),
    )
    params = dict(
        now_beijing=NOW,
        calendar=CALENDAR,
        report=report,
        window=window,
        config_hash="abc123",
        model_version="shadow_v1",
        horizon=20,
        execution_date=dt.datetime(2026, 10, 27),
    )
    params.update(overrides)
    return verify_daily_gate(**params)


def test_approved_active_same_day_runs():
    assert _gate() == "RUN"


def test_holiday_skips_without_writing_plan():
    sunday = dt.datetime(2026, 10, 25, 22, 15, tzinfo=CST)
    assert _gate(now_beijing=sunday) == "SKIP_NON_SESSION"


@pytest.mark.parametrize(
    "field,value",
    [
        ("calendar", []),
        ("window", None),
        ("config_hash", "other"),
        ("execution_date", None),
        ("now_beijing", dt.datetime(2026, 10, 26, 18, 0, tzinfo=CST)),
        ("now_beijing", dt.datetime(2026, 10, 26, 22, 0)),
    ],
)
def test_refuses_missing_identity_or_causality_inputs(field, value):
    with pytest.raises(ValueError):
        _gate(**{field: value})


def test_non_spec_window_refused():
    with pytest.raises(ValueError, match="hash_kind"):
        _gate(window=SimpleNamespace(
            status="ACTIVE",
            model_version="shadow_v1",
            horizon=20,
            config_hash="abc123",
            hash_kind="legacy",
            start_date=dt.datetime(2026, 10, 26),
        ))


def test_requires_current_session_readiness():
    report = {
        "overall": "READY",
        "expected_session": "2026-10-23",
        "checks": [{"id": n, "status": "PASS"} for n in (
            "quote_freshness", "scoring_freshness", "signal_freshness", "halt_switch"
        )],
    }
    with pytest.raises(ValueError, match="today"):
        _gate(report=report)


def test_warned_critical_freshness_not_accepted():
    report = {
        "overall": "DEGRADED",
        "expected_session": TODAY.isoformat(),
        "checks": [
            {"id": "quote_freshness", "status": "PASS"},
            {"id": "scoring_freshness", "status": "WARN"},
            {"id": "signal_freshness", "status": "PASS"},
            {"id": "halt_switch", "status": "PASS"},
        ],
    }
    with pytest.raises(ValueError, match="scoring_freshness"):
        _gate(report=report)


def test_allows_noncritical_gap_warn_once_window_active():
    report = {
        "overall": "DEGRADED",
        "expected_session": TODAY.isoformat(),
        "checks": [
            {"id": n, "status": "PASS"} for n in (
                "quote_freshness", "scoring_freshness",
                "signal_freshness", "halt_switch"
            )
        ] + [{"id": "forward_window", "status": "WARN"}],
    }
    assert _gate(report=report) == "RUN"


def test_no_forward_future_session_coverage_fails():
    with pytest.raises(ValueError, match="coverage"):
        _gate(calendar=[dt.datetime(2026, 10, 23), dt.datetime(2026, 10, 26)])


def test_missing_halt_check_fails():
    report = {"overall": "READY", "expected_session": TODAY.isoformat(), "checks": []}
    with pytest.raises(ValueError, match="quote_freshness"):
        _gate(report=report)
