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
                "fq_coverage",
                "etf_market_data",
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
        _gate(
            window=SimpleNamespace(
                status="ACTIVE",
                model_version="shadow_v1",
                horizon=20,
                config_hash="abc123",
                hash_kind="legacy",
                start_date=dt.datetime(2026, 10, 26),
            )
        )


def test_requires_current_session_readiness():
    report = {
        "overall": "READY",
        "expected_session": "2026-10-23",
        "checks": [
            {"id": n, "status": "PASS"}
            for n in (
                "quote_freshness",
                "scoring_freshness",
                "signal_freshness",
                "halt_switch",
                "fq_coverage",
                "etf_market_data",
            )
        ],
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
            {"id": "fq_coverage", "status": "PASS"},
            {"id": "etf_market_data", "status": "PASS"},
        ],
    }
    with pytest.raises(ValueError, match="scoring_freshness"):
        _gate(report=report)


def test_allows_noncritical_gap_warn_once_window_active():
    report = {
        "overall": "DEGRADED",
        "expected_session": TODAY.isoformat(),
        "checks": [
            {"id": n, "status": "PASS"}
            for n in (
                "quote_freshness",
                "scoring_freshness",
                "signal_freshness",
                "halt_switch",
                "fq_coverage",
                "etf_market_data",
            )
        ]
        + [{"id": "forward_window", "status": "WARN"}],
    }
    assert _gate(report=report) == "RUN"


def test_no_forward_future_session_coverage_fails():
    with pytest.raises(ValueError, match="coverage"):
        _gate(calendar=[dt.datetime(2026, 10, 23), dt.datetime(2026, 10, 26)])


def test_missing_halt_check_fails():
    report = {"overall": "READY", "expected_session": TODAY.isoformat(), "checks": []}
    with pytest.raises(ValueError, match="quote_freshness"):
        _gate(report=report)


def test_forward_halt_requires_real_persisted_file(tmp_path):
    import json

    from app.lib.strategy_engine.halt import (
        HaltError,
        assert_persisted_forward_halt_off,
    )

    state = tmp_path / "strategy-halt.json"
    with pytest.raises(ValueError, match="missing"):
        assert_persisted_forward_halt_off(str(state))
    state.write_text(json.dumps({"halted": False}), encoding="utf-8")
    assert_persisted_forward_halt_off(str(state)) is None
    state.write_text(json.dumps({"halted": True}), encoding="utf-8")
    with pytest.raises(HaltError, match="engaged"):
        assert_persisted_forward_halt_off(str(state))
    state.write_text(json.dumps({"halted": "false"}), encoding="utf-8")
    with pytest.raises(ValueError, match="explicit boolean"):
        assert_persisted_forward_halt_off(str(state))
    state.write_text("not-json", encoding="utf-8")
    with pytest.raises(ValueError, match="cannot be parsed"):
        assert_persisted_forward_halt_off(str(state))
    state.write_text(json.dumps({"halted": False}), encoding="utf-8")
    alias = tmp_path / "link.json"
    alias.symlink_to(state)
    with pytest.raises(ValueError, match="real regular file"):
        assert_persisted_forward_halt_off(str(alias))


def test_forward_halt_rejects_relative_file(tmp_path, monkeypatch):
    from app.lib.strategy_engine.halt import assert_persisted_forward_halt_off

    monkeypatch.chdir(tmp_path)
    (tmp_path / "halt.json").write_text('{"halted":false}', encoding="utf-8")
    with pytest.raises(ValueError, match="absolute"):
        assert_persisted_forward_halt_off("halt.json")


@pytest.mark.parametrize("missing", ["fq_coverage", "etf_market_data"])
def test_forward_rejects_missing_accounting_data_checks(missing):
    report = {
        "overall": "DEGRADED",
        "expected_session": TODAY.isoformat(),
        "checks": [
            {"id": key, "status": "PASS"}
            for key in (
                "quote_freshness",
                "fq_coverage",
                "scoring_freshness",
                "signal_freshness",
                "halt_switch",
                "etf_market_data",
            )
            if key != missing
        ],
    }
    with pytest.raises(ValueError, match=missing):
        _gate(report=report)


@pytest.mark.parametrize("failed", ["fq_coverage", "etf_market_data"])
def test_forward_rejects_warned_accounting_data_checks(failed):
    checks = (
        "quote_freshness",
        "fq_coverage",
        "scoring_freshness",
        "signal_freshness",
        "halt_switch",
        "etf_market_data",
    )
    report = {
        "overall": "DEGRADED",
        "expected_session": TODAY.isoformat(),
        "checks": [
            {"id": key, "status": "WARN" if key == failed else "PASS"} for key in checks
        ],
    }
    with pytest.raises(ValueError, match=failed):
        _gate(report=report)


def test_the_daily_job_hands_the_declaration_to_the_runner(monkeypatch):
    """The automated FORWARD capture must pass the declaration, not only its hash.

    A hash alone makes the runner re-derive a v1 identity from the legacy
    projection, which cannot match a v2 declaration: the only automated path
    would refuse every v2 run while the operator CLI kept working.
    """
    from app.jobs import strategy_forward_daily, strategy_runner

    captured = {}

    def fake_run_strategy(**kwargs):
        captured.update(kwargs)
        return {"status": "COMPLETED", "evidence_kind": "FORWARD"}

    monkeypatch.setattr(strategy_runner, "run_strategy", fake_run_strategy)
    spec = {"spec_version": "2", "family": "rank_selection"}
    target = dt.datetime(2026, 10, 26)

    strategy_forward_daily.run_forward_plan(
        target=target,
        config={"score_model_version": "shadow_v1", "horizon": 20},
        spec=spec,
        identity="abc123",
        policy=None,
    )

    assert captured["strategy_spec"] is spec
    assert captured["strategy_spec_hash"] == "abc123"
    assert captured["date"] == target
    assert captured["rebalance_policy"] is None
