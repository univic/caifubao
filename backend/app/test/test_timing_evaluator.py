"""Pure timing decision and pooled-report contract tests."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from app.services.timing_evaluator import (
    build_timing_decision,
    canonical_json_sha256,
    evaluate_timing_pool,
)

CALENDAR = [date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3)]
CONFIG = {
    "entry_percentile": 0.90,
    "exit_percentile": 0.30,
    "expiry_sessions": 1,
}
COHORT_AS_OF = "2026-09-14T00:00:00Z"
COHORT_SOURCE = "fixture:snapshot-v1"


def _evidence(percentile=0.95, *, freshness="FRESH"):
    return {
        "stock_code": "sh600001",
        "signal_date": "2026-09-01",
        "signal_close_at": "2026-09-01T07:00:00Z",
        "decision_at": "2026-09-01T07:00:01Z",
        "percentile": percentile,
        "data_as_of": "2026-09-01T07:00:00Z",
        "freshness": freshness,
        "score_status": "VERIFIED",
        "model_version": "timing-model-v1",
        "horizon": 20,
        "evidence_kind": "REPLAY",
    }


def _run_result(
    *,
    stock_code="sh600001",
    timing_return_pct=5.0,
    buy_hold_return_pct=2.0,
    max_drawdown=-4.0,
    sharpe_ratio=0.5,
    completed_trades=5,
    observed_days=120,
    timing_error=None,
    buy_hold_error=None,
):
    timing_trades = [
        {
            "date": "2026-03-02",
            "side": "SELL",
            "status": "FILLED",
            "quantity": 100,
            "exec_price": 10.0,
        }
    ] * completed_trades
    assumptions = {
        "initial_cash": 100000.0,
        "window": {"from": "2026-01-01", "to": "2026-06-30"},
        "execution_timing": "next_trading_day_open",
        "valuation_timing": "last_close",
        "board_lot": 100,
        "friction": {
            "commission_rate": 0.00025,
            "minimum_commission": 5.0,
            "stamp_duty_rate": 0.001,
            "slippage_rate": 0.001,
        },
    }
    return {
        "timing": {
            "stock_code": stock_code,
            "assumptions": assumptions,
            "return_pct": timing_return_pct,
            "max_drawdown": max_drawdown,
            "sharpe_ratio": sharpe_ratio,
            "trades": timing_trades,
            "daily_values": [
                {
                    "date": (date(2026, 1, 1) + timedelta(days=i)).isoformat(),
                    "equity": 100000.0 + i,
                }
                for i in range(observed_days)
            ],
            **({"error": timing_error} if timing_error else {}),
        },
        "buy_hold": {
            "stock_code": stock_code,
            "assumptions": assumptions,
            "return_pct": buy_hold_return_pct,
            "daily_values": [
                {
                    "date": (date(2026, 1, 1) + timedelta(days=i)).isoformat(),
                    "equity": 100000.0 + i,
                }
                for i in range(observed_days)
            ],
            **({"error": buy_hold_error} if buy_hold_error else {}),
        },
    }


def test_enter_artifact_is_causal_research_evidence_and_uses_percentile_only():
    artifact = build_timing_decision(
        _evidence(), position_state="FLAT", config=CONFIG, calendar=CALENDAR
    )

    assert artifact["action"] == "ENTER"
    assert artifact["research_only"] is True
    assert artifact["unvalidated"] is True
    assert artifact["percentile"] == 0.95
    assert "score" not in artifact
    assert artifact["signal_date"] == "2026-09-01"
    assert artifact["execution_date"] == "2026-09-02"
    assert artifact["decision_at"].endswith("+00:00")
    assert artifact["execution_at"] == "2026-09-02T01:30:00+00:00"
    assert artifact["config_hash"] == canonical_json_sha256(CONFIG)


def test_held_exit_and_stale_evidence_fail_closed():
    exit_artifact = build_timing_decision(
        _evidence(0.20), position_state="HELD", config=CONFIG, calendar=CALENDAR
    )
    stale_artifact = build_timing_decision(
        _evidence(0.99, freshness="STALE"),
        position_state="FLAT",
        config=CONFIG,
        calendar=CALENDAR,
    )

    assert exit_artifact["action"] == "EXIT"
    assert "exit_percentile" in exit_artifact["reason_tokens"]
    assert stale_artifact["action"] == "NO_TRADE"
    assert "stale_evidence" in stale_artifact["reason_tokens"]


@pytest.mark.parametrize("freshness", ["CURRENT", "OK", True, " fresh "])
def test_only_exact_fresh_status_can_trade(freshness):
    artifact = build_timing_decision(
        _evidence(0.99, freshness=freshness),
        position_state="FLAT",
        config=CONFIG,
        calendar=CALENDAR,
    )

    assert artifact["action"] == "NO_TRADE"


def test_missing_percentile_is_no_trade_but_noncausal_timestamps_raise():
    missing = _evidence()
    missing.pop("percentile")
    artifact = build_timing_decision(
        missing, position_state="FLAT", config=CONFIG, calendar=CALENDAR
    )
    assert artifact["action"] == "NO_TRADE"
    assert "missing_percentile" in artifact["reason_tokens"]

    late = _evidence()
    late["decision_at"] = "2026-09-02T01:30:00Z"
    with pytest.raises(ValueError, match="next session open"):
        build_timing_decision(
            late, position_state="FLAT", config=CONFIG, calendar=CALENDAR
        )


@pytest.mark.parametrize("score_status", ["FAILED", "UNKNOWN", "", None])
def test_unusable_score_status_is_explicit_no_trade(score_status):
    evidence = _evidence()
    evidence["score_status"] = score_status

    artifact = build_timing_decision(
        evidence, position_state="FLAT", config=CONFIG, calendar=CALENDAR
    )

    assert artifact["action"] == "NO_TRADE"
    assert "unusable_score_status" in artifact["reason_tokens"]


def test_calendar_must_supply_a_strictly_later_session_and_timestamps_are_aware():
    with pytest.raises(ValueError, match="calendar"):
        build_timing_decision(
            _evidence(),
            position_state="FLAT",
            config=CONFIG,
            calendar=[date(2026, 9, 1)],
        )

    naive = _evidence()
    naive["decision_at"] = "2026-09-01T07:00:01"
    with pytest.raises(ValueError, match="timezone-aware"):
        build_timing_decision(
            naive, position_state="FLAT", config=CONFIG, calendar=CALENDAR
        )


def test_signal_close_and_expiry_must_match_authoritative_session_boundaries():
    missing_close = _evidence()
    missing_close.pop("signal_close_at")
    with pytest.raises(ValueError, match="signal_close_at"):
        build_timing_decision(
            missing_close, position_state="FLAT", config=CONFIG, calendar=CALENDAR
        )

    early_close = _evidence()
    early_close["signal_close_at"] = "2026-09-01T00:00:00Z"
    with pytest.raises(ValueError, match="signal session close"):
        build_timing_decision(
            early_close, position_state="FLAT", config=CONFIG, calendar=CALENDAR
        )

    with pytest.raises(ValueError, match="execution session close"):
        build_timing_decision(
            _evidence(),
            position_state="FLAT",
            config=CONFIG,
            calendar=CALENDAR,
            expires_at="2026-09-02T06:59:59Z",
        )


def test_pool_canonicalizes_cohort_and_does_not_select_a_best_stock():
    calls = []

    def runner(stock_code, **kwargs):
        calls.append((stock_code, kwargs))
        return _run_result(
            stock_code=stock_code,
            timing_return_pct=6.0 if stock_code == "sh600001" else 4.0,
            buy_hold_return_pct=2.0,
        )

    first = evaluate_timing_pool(
        ["sh600002", "sh600001", "sh600001"],
        runner,
        model_version="timing-model-v1",
        config=CONFIG,
        window=("2026-01-01", "2026-06-30"),
        cohort_as_of=COHORT_AS_OF,
        cohort_source=COHORT_SOURCE,
        delisted_completeness="VERIFIED",
    )
    second = evaluate_timing_pool(
        ["sh600001", "sh600002"],
        runner,
        model_version="timing-model-v1",
        config=CONFIG,
        window={"from": "2026-01-01", "to": "2026-06-30"},
        cohort_as_of=COHORT_AS_OF,
        cohort_source=COHORT_SOURCE,
        delisted_completeness="VERIFIED",
    )

    assert [row["stock_code"] for row in first["results"]] == [
        "sh600001",
        "sh600002",
    ]
    assert first["identity_sha256"] == second["identity_sha256"]
    assert first["successful_count"] == 2
    assert first["coverage"]["requested_count"] == 2
    assert first["excess_distribution"]["median"] == 3.0
    assert first["positive_excess_share"] == 1.0
    assert first["research_only"] is True
    assert first["unvalidated"] is True
    assert first["validation_status"] == "UNVALIDATED"
    assert first["results"][0]["status"] == "SUCCESS"
    assert first["results"][0]["completed_trades"] == 5
    assert "best_stock" not in first
    assert [call[0] for call in calls[:2]] == ["sh600001", "sh600002"]
    assert all(call[1]["save_result"] is False for call in calls[:2])
    assert first["cohort_as_of"] == COHORT_AS_OF
    assert first["cohort_source"] == COHORT_SOURCE
    assert first["cohort_sha256"] == canonical_json_sha256(
        {
            "codes": ["sh600001", "sh600002"],
            "as_of": COHORT_AS_OF,
            "source": COHORT_SOURCE,
        }
    )


def test_pool_retains_failures_and_exposes_coverage_and_eligibility_gates():
    def runner(stock_code, **_kwargs):
        if stock_code == "sh600002":
            raise RuntimeError("missing delisted quote coverage")
        return _run_result(completed_trades=4, observed_days=119)

    report = evaluate_timing_pool(
        ["sh600001", "sh600002"],
        runner,
        model_version="timing-model-v1",
        config=CONFIG,
        window=("2026-01-01", "2026-06-30"),
        cohort_as_of=COHORT_AS_OF,
        cohort_source=COHORT_SOURCE,
        delisted_completeness="NOT_VERIFIED",
    )

    failed = next(row for row in report["results"] if row["stock_code"] == "sh600002")
    low_evidence = next(
        row for row in report["results"] if row["stock_code"] == "sh600001"
    )
    assert failed["status"] == "FAILED"
    assert failed["reason_code"] == "runner_exception"
    assert "delisted quote coverage" in failed["error"]
    assert low_evidence["status"] == "SUCCESS"
    assert low_evidence["evidence_eligible"] is False
    assert report["coverage"] == {
        "requested_count": 2,
        "successful_count": 1,
        "failed_count": 1,
        "success_rate": 0.5,
    }
    assert report["gates"]["requested"]["passed"] is False
    assert report["gates"]["evidence_eligible"]["passed"] is False
    assert report["gates"]["coverage"]["passed"] is False
    assert report["gate_passed"] is False


def test_pool_gates_pass_only_with_fifty_eligible_requested_stocks():
    cohort = [f"sh60{i:04d}" for i in range(50)]
    report = evaluate_timing_pool(
        cohort,
        lambda stock_code, **_kwargs: _run_result(stock_code=stock_code),
        model_version="timing-model-v1",
        config=CONFIG,
        window=("2026-01-01", "2026-06-30"),
        cohort_as_of=COHORT_AS_OF,
        cohort_source=COHORT_SOURCE,
        delisted_completeness="VERIFIED",
    )

    assert report["gate_passed"] is True
    assert report["gates"]["requested"]["value"] == 50
    assert report["gates"]["evidence_eligible"]["value"] == 50
    assert report["gates"]["coverage"]["value"] == 1.0


def test_pool_rejects_implicit_index_benchmark_and_requires_two_sides():
    def index_benchmark_result(_stock_code, **_kwargs):
        return {
            "total_return_pct": 5.0,
            "benchmark_return_pct": 2.0,
            "max_drawdown": -4.0,
            "sharpe_ratio": 0.5,
            "total_trades": 5,
            "daily_values": [
                {"date": (date(2026, 1, 1) + timedelta(days=i)).isoformat()}
                for i in range(120)
            ],
        }

    report = evaluate_timing_pool(
        ["sh600001"],
        index_benchmark_result,
        model_version="timing-model-v1",
        config=CONFIG,
        window=("2026-01-01", "2026-06-30"),
        cohort_as_of=COHORT_AS_OF,
        cohort_source=COHORT_SOURCE,
        delisted_completeness="UNKNOWN",
    )

    row = report["results"][0]
    assert row["status"] == "FAILED"
    assert row["reason_code"] == "missing_timing_side"

    def nested_index_result(_stock_code, **_kwargs):
        result = _run_result(stock_code=_stock_code)
        result["buy_hold"]["benchmark_code"] = "sh000300"
        return result

    report = evaluate_timing_pool(
        ["sh600001"],
        nested_index_result,
        model_version="timing-model-v1",
        config=CONFIG,
        window=("2026-01-01", "2026-06-30"),
        cohort_as_of=COHORT_AS_OF,
        cohort_source=COHORT_SOURCE,
        delisted_completeness="UNKNOWN",
    )
    assert report["results"][0]["reason_code"] == "buy_hold_not_same_stock"


def test_observed_sessions_are_daily_date_intersection_and_sells_are_timing_only():
    def runner(_stock_code, **_kwargs):
        result = _run_result(observed_days=3, completed_trades=2)
        result["timing"]["daily_values"] = [
            {"date": "2026-01-01", "equity": 100000.0},
            {"date": "2026-01-02", "equity": 100100.0},
            {"date": "2026-01-03", "equity": 100200.0},
        ]
        result["buy_hold"]["daily_values"] = [
            {"date": "2026-01-02", "equity": 100050.0},
            {"date": "2026-01-03", "equity": 100100.0},
            {"date": "2026-01-04", "equity": 100150.0},
        ]
        result["timing"]["trades"].append({"date": "2026-01-03", "side": "BUY"})
        result["timing"]["total_trades"] = 99
        result["buy_hold"]["trades"] = [{"side": "SELL"}] * 99
        return result

    report = evaluate_timing_pool(
        ["sh600001"],
        runner,
        model_version="timing-model-v1",
        config=CONFIG,
        window=("2026-01-01", "2026-06-30"),
        cohort_as_of=COHORT_AS_OF,
        cohort_source=COHORT_SOURCE,
        delisted_completeness="UNKNOWN",
    )

    row = report["results"][0]
    assert row["observed_sessions"] == 2
    assert row["completed_trades"] == 2


def test_pool_rejects_mismatched_execution_assumptions_and_unfilled_sells():
    def mismatched_runner(stock_code, **_kwargs):
        result = _run_result(stock_code=stock_code)
        result["buy_hold"]["assumptions"] = {
            **result["buy_hold"]["assumptions"],
            "execution_timing": "first_session_close",
        }
        return result

    report = evaluate_timing_pool(
        ["sh600001"],
        mismatched_runner,
        model_version="timing-model-v1",
        config=CONFIG,
        window=("2026-01-01", "2026-06-30"),
        cohort_as_of=COHORT_AS_OF,
        cohort_source=COHORT_SOURCE,
        delisted_completeness="VERIFIED",
    )
    assert report["results"][0]["reason_code"] == ("comparison_assumptions_mismatch")

    def blocked_sell_runner(stock_code, **_kwargs):
        result = _run_result(stock_code=stock_code, completed_trades=0)
        result["timing"]["trades"] = [
            {
                "date": "2026-03-02",
                "side": "SELL",
                "status": "BLOCKED",
                "quantity": 100,
                "exec_price": 10.0,
            },
            {
                "date": "2026-03-03",
                "side": "SELL",
                "status": "FILLED",
                "quantity": 0,
                "exec_price": 10.0,
            },
        ]
        return result

    report = evaluate_timing_pool(
        ["sh600001"],
        blocked_sell_runner,
        model_version="timing-model-v1",
        config=CONFIG,
        window=("2026-01-01", "2026-06-30"),
        cohort_as_of=COHORT_AS_OF,
        cohort_source=COHORT_SOURCE,
        delisted_completeness="VERIFIED",
    )
    assert report["results"][0]["completed_trades"] == 0


def test_pool_rejects_observations_and_trades_outside_declared_window():
    cohort = [f"sh60{i:04d}" for i in range(50)]

    def runner(stock_code, **_kwargs):
        result = _run_result(stock_code=stock_code)
        old_dates = [
            {
                "date": (date(2025, 1, 1) + timedelta(days=i)).isoformat(),
                "equity": 100000.0 + i,
            }
            for i in range(120)
        ]
        result["timing"]["daily_values"] = old_dates
        result["buy_hold"]["daily_values"] = old_dates
        result["timing"]["trades"] = [
            {
                "date": "2025-03-03",
                "side": "SELL",
                "status": "FILLED",
                "quantity": 100,
                "exec_price": 10.0,
            }
        ] * 5
        return result

    report = evaluate_timing_pool(
        cohort,
        runner,
        model_version="timing-model-v1",
        config=CONFIG,
        window=("2026-01-01", "2026-06-30"),
        cohort_as_of=COHORT_AS_OF,
        cohort_source=COHORT_SOURCE,
        delisted_completeness="VERIFIED",
    )

    assert report["gate_passed"] is False
    assert report["coverage"]["successful_count"] == 0
    assert {row["reason_code"] for row in report["results"]} == {
        "observations_outside_window"
    }


@pytest.mark.parametrize(
    "status", [None, "REJECTED", "CANCELLED", "EXPIRED", "PARTIALLY_FILLED"]
)
def test_only_explicit_filled_sells_count_as_completed(status):
    result = _run_result(completed_trades=0)
    result["timing"]["trades"] = [
        {
            "date": "2026-03-02",
            "side": "SELL",
            "status": status,
            "quantity": 100,
            "exec_price": 10.0,
        }
    ]

    report = evaluate_timing_pool(
        ["sh600001"],
        lambda _stock_code, **_kwargs: result,
        model_version="timing-model-v1",
        config=CONFIG,
        window=("2026-01-01", "2026-06-30"),
        cohort_as_of=COHORT_AS_OF,
        cohort_source=COHORT_SOURCE,
    )

    assert report["results"][0]["completed_trades"] == 0


def test_daily_observations_require_finite_equity_values():
    result = _run_result()
    result["timing"]["daily_values"][0].pop("equity")

    report = evaluate_timing_pool(
        ["sh600001"],
        lambda _stock_code, **_kwargs: result,
        model_version="timing-model-v1",
        config=CONFIG,
        window=("2026-01-01", "2026-06-30"),
        cohort_as_of=COHORT_AS_OF,
        cohort_source=COHORT_SOURCE,
    )

    row = report["results"][0]
    assert row["status"] == "FAILED"
    assert row["reason_code"] == "missing_metrics"
    assert "timing_daily_values" in row["error"]


def test_either_explicit_side_failure_is_a_stable_failed_row():
    for reason, kwargs in (
        ("timing_side_failed", {"timing_error": "timing unavailable"}),
        ("buy_hold_side_failed", {"buy_hold_error": "buy_hold unavailable"}),
    ):
        report = evaluate_timing_pool(
            ["sh600001"],
            lambda _stock_code, side_kwargs=kwargs, **_kwargs: _run_result(
                **side_kwargs
            ),
            model_version="timing-model-v1",
            config=CONFIG,
            window=("2026-01-01", "2026-06-30"),
            cohort_as_of=COHORT_AS_OF,
            cohort_source=COHORT_SOURCE,
            delisted_completeness="UNKNOWN",
        )
        row = report["results"][0]
        assert row["status"] == "FAILED"
        assert row["reason_code"] == reason


@pytest.mark.parametrize("value", ["verified", " VERIFIED ", "PARTIAL", True, None])
def test_delisted_completeness_is_a_closed_enum(value):
    def runner(_stock_code, **_kwargs):
        return _run_result()

    with pytest.raises((TypeError, ValueError), match="delisted_completeness"):
        evaluate_timing_pool(
            ["sh600001"],
            runner,
            model_version="timing-model-v1",
            config=CONFIG,
            window=("2026-01-01", "2026-06-30"),
            cohort_as_of=COHORT_AS_OF,
            cohort_source=COHORT_SOURCE,
            delisted_completeness=value,
        )


@pytest.mark.parametrize("value", ["VERIFIED", "NOT_VERIFIED", "UNKNOWN"])
def test_delisted_completeness_accepts_only_named_statuses(value):
    report = evaluate_timing_pool(
        ["sh600001"],
        lambda _stock_code, **_kwargs: _run_result(),
        model_version="timing-model-v1",
        config=CONFIG,
        window=("2026-01-01", "2026-06-30"),
        cohort_as_of=COHORT_AS_OF,
        cohort_source=COHORT_SOURCE,
        delisted_completeness=value,
    )
    assert report["delisted_completeness"] == value
