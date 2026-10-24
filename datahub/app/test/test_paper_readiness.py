"""Tests for the read-only paper-trading readiness assessment."""

import datetime

from app.lib import paper_readiness


def _calendar():
    return [
        datetime.datetime(2026, 10, 19),
        datetime.datetime(2026, 10, 20),
        datetime.datetime(2026, 10, 21),
        datetime.datetime(2026, 10, 22),
        datetime.datetime(2026, 10, 23),
    ]


def _inputs(**overrides):
    defaults = dict(
        now_beijing=datetime.datetime(2026, 10, 23, 18, 0),
        calendar=_calendar(),
        active_stock_count=100,
        latest_quote_date=datetime.date(2026, 10, 23),
        quoted_active_count=100,
        missing_fq_count=0,
        latest_score_date=datetime.date(2026, 10, 23),
        latest_signal_date=datetime.date(2026, 10, 23),
        active_window={
            "model_version": "flip_wide_shadow_v1",
            "horizon": 20,
            "config_hash": "abc",
            "start_date": datetime.datetime(2026, 10, 20),
        },
        forward_dates=[
            datetime.datetime(2026, 10, 20),
            datetime.datetime(2026, 10, 21),
            datetime.datetime(2026, 10, 22),
            datetime.datetime(2026, 10, 23),
        ],
        halt_path="/tmp/halt.json",
        halt_state={"halted": False},
        etf_count=5,
        etf_latest_quote_date=datetime.date(2026, 10, 23),
        etf_active_count=5,
        etf_missing_fq_count=0,
    )
    defaults.update(overrides)
    return paper_readiness.ReadinessInputs(**defaults)


def test_expected_session_before_close_uses_previous_session():
    calendar = _calendar()
    assert paper_readiness.expected_session(
        calendar, datetime.datetime(2026, 10, 23, 10, 0)
    ) == datetime.date(2026, 10, 22)


def test_expected_session_after_close_uses_today():
    assert paper_readiness.expected_session(
        _calendar(), datetime.datetime(2026, 10, 23, 16, 0)
    ) == datetime.date(2026, 10, 23)


def test_expected_session_on_weekend_uses_last_session():
    assert paper_readiness.expected_session(
        _calendar(), datetime.datetime(2026, 10, 25, 12, 0)
    ) == datetime.date(2026, 10, 23)


def test_evaluate_ready_when_every_check_passes():
    report = paper_readiness.evaluate(_inputs())

    assert report["overall"] == "READY"
    statuses = {check["id"]: check["status"] for check in report["checks"]}
    assert statuses["quote_freshness"] == "PASS"
    assert statuses["fq_coverage"] == "PASS"
    assert statuses["forward_window"] == "PASS"
    assert statuses["etf_market_data"] == "PASS"
    assert report["limitations"]


def test_evaluate_fails_when_quotes_are_stale():
    report = paper_readiness.evaluate(
        _inputs(latest_quote_date=datetime.date(2026, 10, 21))
    )

    assert report["overall"] == "NOT_READY"
    check = {item["id"]: item for item in report["checks"]}["quote_freshness"]
    assert check["status"] == "FAIL"
    assert check["detail"]["lag_days"] == 2


def test_evaluate_fails_when_active_quotes_lack_fq():
    report = paper_readiness.evaluate(_inputs(missing_fq_count=3))

    assert report["overall"] == "NOT_READY"
    assert {item["id"]: item for item in report["checks"]}["fq_coverage"][
        "status"
    ] == "FAIL"


def test_evaluate_degrades_when_forward_window_has_a_gap():
    report = paper_readiness.evaluate(
        _inputs(
            forward_dates=[
                datetime.datetime(2026, 10, 20),
                datetime.datetime(2026, 10, 22),
                datetime.datetime(2026, 10, 23),
            ]
        )
    )

    assert report["overall"] == "DEGRADED"
    check = {item["id"]: item for item in report["checks"]}["forward_window"]
    assert check["status"] == "WARN"
    assert check["detail"]["missing_sessions"] == 1


def test_evaluate_warns_when_no_window_is_certified():
    report = paper_readiness.evaluate(_inputs(active_window=None, forward_dates=[]))

    assert report["overall"] == "DEGRADED"
    assert {item["id"]: item for item in report["checks"]}["forward_window"][
        "status"
    ] == "WARN"


def test_evaluate_fails_when_etf_quotes_are_stale():
    report = paper_readiness.evaluate(
        _inputs(etf_latest_quote_date=datetime.date(2026, 10, 22))
    )

    assert report["overall"] == "NOT_READY"
    assert {item["id"]: item for item in report["checks"]}["etf_market_data"][
        "status"
    ] == "FAIL"


def test_evaluate_warns_when_etf_universe_is_empty():
    report = paper_readiness.evaluate(
        _inputs(etf_count=0, etf_latest_quote_date=None, etf_active_count=0)
    )

    assert report["overall"] == "DEGRADED"
    assert {item["id"]: item for item in report["checks"]}["etf_market_data"][
        "status"
    ] == "WARN"


def test_evaluate_warns_when_halt_path_is_not_configured():
    report = paper_readiness.evaluate(_inputs(halt_path=None, halt_state=None))

    assert report["overall"] == "DEGRADED"
    assert {item["id"]: item for item in report["checks"]}["halt_switch"][
        "status"
    ] == "WARN"


def test_current_session_not_yet_run_is_not_a_gap():
    # Readiness is checked before the daily run, so the expected session's run
    # not existing yet must not be reported as a missing (permanent) session.
    report = paper_readiness.evaluate(
        _inputs(
            forward_dates=[
                datetime.datetime(2026, 10, 20),
                datetime.datetime(2026, 10, 21),
                datetime.datetime(2026, 10, 22),
            ]
        )
    )

    check = {item["id"]: item for item in report["checks"]}["forward_window"]
    assert check["status"] == "PASS"
    assert check["detail"]["missing_sessions"] == 0
    assert check["detail"]["current_session_recorded"] is False


def test_forward_run_query_matches_the_engine_hash_namespace():
    # Merged #307 requires BOTH config_hash and hash_kind to match. Pre-#307
    # rows have no hash_kind field and are legacy, so a legacy window must
    # match the explicit legacy value and the missing field.
    legacy = paper_readiness.forward_run_query(
        {
            "model_version": "flip_wide_shadow_v1",
            "horizon": 20,
            "config_hash": "abc",
            "hash_kind": "legacy",
            "start_date": datetime.datetime(2026, 10, 20),
        }
    )
    assert legacy["config_hash"] == "abc"
    assert legacy["evidence_kind"] == "FORWARD"
    assert legacy["$or"] == [
        {"hash_kind": "legacy"},
        {"hash_kind": {"$exists": False}},
    ]
    assert "hash_kind" not in legacy

    spec = paper_readiness.forward_run_query(
        {
            "model_version": "v",
            "horizon": 20,
            "config_hash": "spec-hash",
            "hash_kind": "spec",
            "start_date": datetime.datetime(2026, 10, 20),
        }
    )
    assert spec["hash_kind"] == "spec"
    assert "$or" not in spec


def test_window_check_reports_the_hash_kind():
    report = paper_readiness.evaluate(
        _inputs(
            active_window={
                "model_version": "flip_wide_shadow_v1",
                "horizon": 20,
                "config_hash": "spec-hash",
                "hash_kind": "spec",
                "spec_hash": "spec-hash",
                "legacy_config_hash": "legacy-hash",
                "start_date": datetime.datetime(2026, 10, 20),
            }
        )
    )

    check = {item["id"]: item for item in report["checks"]}["forward_window"]
    assert check["detail"]["hash_kind"] == "spec"
    assert check["detail"]["spec_hash"] == "spec-hash"
    assert check["detail"]["legacy_config_hash"] == "legacy-hash"
