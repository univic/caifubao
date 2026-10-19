"""Tests for the bounded quote/index degradation policy (C1)."""

from __future__ import annotations

import datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas
import pytest


class DummyQuerySet(list):
    def count(self):
        return len(self)


class EmptyQuerySet(list):
    def count(self):
        return 0


def _stock(code, *, active_status=0):
    return SimpleNamespace(
        code=code,
        name=code,
        object_type="individual_stock",
        active_status=active_status,
        data_capabilities=SimpleNamespace(
            daily_quote=True, fq_factor=True, ma_factor=True
        ),
        save=Mock(),
    )


def _index(code):
    return SimpleNamespace(
        code=code,
        name=code,
        object_type="stock_index",
        active_status=0,
        data_capabilities=SimpleNamespace(
            daily_quote=True, fq_factor=True, ma_factor=True
        ),
        save=Mock(),
    )


def _remote_frame(codes):
    return pandas.DataFrame(
        [{"code": code, "name": code, "close": 1} for code in codes]
    )


def _ok_result():
    return {
        "code": "GOOD",
        "written_count": 1,
        "validated_count": 1,
        "freshness_status": "OK",
    }


def _failed_result(*, no_available_history=False):
    return {
        "code": "FAIL",
        "written_count": 0,
        "validated_count": 0,
        "freshness_status": "NO_DATA",
        "no_available_history": no_available_history,
    }


def _make_processor(results_by_code):
    from app.lib.datahub.processors.china_a_stock import ChinaAStock

    processor = object.__new__(ChinaAStock)
    processor.market = SimpleNamespace(name="ChinaAStock", trade_calendar=[])
    processor.most_recent_trading_day = pandas.Timestamp("2026-08-21")
    processor.perform_date_check = lambda: None
    processor.check_data_freshness = lambda stock, **kwargs: "INC"
    processor.perform_stock_name_check = Mock()
    processor.update_active_status = Mock()
    processor.get_hist_quote_data = Mock(
        side_effect=lambda stock_obj, hist_quote_handler: dict(
            results_by_code[stock_obj.code]
        )
    )
    return processor


def _run_quote_phase(processor, stocks):
    codes = [stock.code for stock in stocks]
    with (
        patch(
            "app.lib.datahub.processors.china_a_stock.progress_bar",
            return_value=lambda *args: None,
        ),
        patch(
            "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
            return_value={},
        ),
    ):
        return processor.check_data_integrity(
            obj_type="stock",
            local_data_list=DummyQuerySet(stocks),
            remote_data_df=_remote_frame(codes),
            hist_handler="get_hist_stock_quote_data",
            allow_update=True,
        )


def _quote_validation_processor(failure_count, attempted=5223):
    codes = [f"sh6{index:05d}" for index in range(attempted)]
    failed = set()
    if failure_count:
        # Spread the failures out so the consecutive-history circuit breaker
        # never trips: this test exercises the tolerance cap, not the breaker.
        step = max(1, attempted // failure_count)
        for index in range(0, attempted, step):
            if len(failed) >= failure_count:
                break
            failed.add(codes[index])
    results = {
        code: (_failed_result() if code in failed else _ok_result()) for code in codes
    }
    stocks = [_stock(code) for code in codes]
    return _make_processor(results), stocks, sorted(failed)


def test_quote_failure_cap_is_absolute_and_relative():
    from app.lib.utilities.pipeline_degradation import quote_failure_cap

    assert quote_failure_cap(0) == 5
    assert quote_failure_cap(100) == 5
    assert quote_failure_cap(5223) == 11
    assert quote_failure_cap(10000) == 20


def test_coverage_rate_clamps_to_the_attempted_denominator():
    from app.lib.utilities.pipeline_degradation import coverage_rate

    assert coverage_rate(5221, 5223) == pytest.approx(0.999617)
    assert coverage_rate(0, 0) == 0.0
    assert coverage_rate(10, 5) == 1.0


def test_two_failures_of_5223_degrade_with_persisted_counters():
    processor, stocks, failed = _quote_validation_processor(2)

    result = _run_quote_phase(processor, stocks)

    validation = result["quote_validation"]
    assert validation["attempted_symbols"] == 5223
    assert validation["validated_symbols"] == 5221
    assert validation["failed_count"] == 2
    assert validation["failed_codes"] == failed
    # The OpenSpec scenario names the same list ``failed_symbols``.
    assert validation["failed_symbols"] == failed
    assert validation["coverage_rate"] >= 0.99
    assert validation["degraded"] is True
    assert result["degraded"] is True


def test_hundreds_of_failures_still_fail_the_run():
    processor, stocks, _ = _quote_validation_processor(522)

    with pytest.raises(RuntimeError, match="validation failed for 522 symbols"):
        _run_quote_phase(processor, stocks)


def test_strict_switch_fails_on_a_single_symbol(monkeypatch):
    monkeypatch.setenv("DATAHUB_QUOTE_STRICT_VALIDATION", "1")
    # 1 failure of 200: coverage 0.995 clears the floor and the failure cap,
    # so only the strict switch can fail this run.
    processor, stocks, _ = _quote_validation_processor(1, attempted=200)

    with pytest.raises(RuntimeError, match="Quote validation failed for 1 symbols"):
        _run_quote_phase(processor, stocks)


def test_strict_switch_leaves_clean_runs_successful(monkeypatch):
    monkeypatch.setenv("DATAHUB_QUOTE_STRICT_VALIDATION", "1")
    processor, stocks, _ = _quote_validation_processor(0, attempted=200)

    result = _run_quote_phase(processor, stocks)

    assert result["quote_validation"]["degraded"] is False
    assert result["degraded"] is False


def test_untouched_skip_symbols_stay_out_of_the_denominator():
    from app.lib.utilities.pipeline_degradation import evaluate_validation

    validation = evaluate_validation(
        obj_type="stock",
        attempted_symbols=5223,
        validated_symbols=5221,
        failed_codes=["sh600001", "sh600002"],
        attempted_codes=[f"sh6{index:05d}" for index in range(5223)],
        enforce=True,
        strict=False,
    )

    assert validation["attempted_symbols"] == 5223
    assert validation["coverage_rate"] >= 0.99
    assert validation["degraded"] is True


def test_degraded_phase_marks_the_run_degraded_and_persists_evidence():

    processor, stocks, failed = _quote_validation_processor(2)
    processor.market_name = "ChinaAStock"
    processor.last_job_summary = None
    processor._partial_phase_result = None
    processor._progress_callback = None

    def run_phase(allow_update=False):
        return _run_quote_phase(processor, stocks)

    summary = processor._run_job("stock_quote_sync", [("quotes", run_phase)])

    assert summary["status"] == "DEGRADED"
    assert summary["degraded"] is True
    assert summary["quote_validation"]["failed_codes"] == failed
    assert (
        summary["phase_stats"]["quotes"]["quote_validation"]["validated_symbols"]
        == 5221
    )


def test_failed_phase_keeps_degradation_counters_for_diagnosis():

    processor, stocks, _ = _quote_validation_processor(522)
    processor.market_name = "ChinaAStock"
    processor.last_job_summary = None
    processor._partial_phase_result = None
    processor._progress_callback = None

    def run_phase(allow_update=False):
        return _run_quote_phase(processor, stocks)

    with pytest.raises(RuntimeError):
        processor._run_job("stock_quote_sync", [("quotes", run_phase)])

    summary = processor.last_job_summary
    assert summary["status"] == "FAILED"
    validation = summary["phase_stats"]["quotes"]["quote_validation"]
    assert validation["failed_count"] == 522
    assert validation["degraded"] is False


def test_core_index_failure_fails_the_run():
    cores = ["sh000001", "sz399001"]
    results = {
        "sh000001": _ok_result(),
        "sz399001": _failed_result(no_available_history=True),
    }
    processor = _make_processor(results)
    indexes = [_index(code) for code in cores]

    with pytest.raises(RuntimeError, match="sz399001"):
        with (
            patch(
                "app.lib.datahub.processors.china_a_stock.progress_bar",
                return_value=lambda *args: None,
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
                return_value={},
            ),
        ):
            processor.check_data_integrity(
                obj_type="index",
                local_data_list=DummyQuerySet(indexes),
                remote_data_df=_remote_frame(cores),
                hist_handler="get_hist_index_quote_data",
                allow_update=True,
            )


def test_non_allowlisted_non_core_index_skip_is_degraded():

    codes = ["sh000300", "sh000015"]
    results = {
        "sh000300": _ok_result(),
        "sh000015": _failed_result(no_available_history=True),
    }
    processor = _make_processor(results)
    indexes = [_index(code) for code in codes]

    with (
        patch(
            "app.lib.datahub.processors.china_a_stock.progress_bar",
            return_value=lambda *args: None,
        ),
        patch(
            "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
            return_value={},
        ),
    ):
        result = processor.check_data_integrity(
            obj_type="index",
            local_data_list=DummyQuerySet(indexes),
            remote_data_df=_remote_frame(codes),
            hist_handler="get_hist_index_quote_data",
            allow_update=True,
        )

    validation = result["index_validation"]
    assert validation["skipped_indices"] == ["sh000015"]
    assert validation["failed_count"] == 0
    assert validation["degraded"] is True
    assert result["degraded"] is True


def test_allowlisted_non_core_index_skip_stays_clean(monkeypatch):
    monkeypatch.setenv("DATAHUB_INDEX_SKIP_ALLOWLIST", "sh000015")
    codes = ["sh000300", "sh000015"]
    results = {
        "sh000300": _ok_result(),
        "sh000015": _failed_result(no_available_history=True),
    }
    processor = _make_processor(results)
    indexes = [_index(code) for code in codes]

    with (
        patch(
            "app.lib.datahub.processors.china_a_stock.progress_bar",
            return_value=lambda *args: None,
        ),
        patch(
            "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
            return_value={},
        ),
    ):
        result = processor.check_data_integrity(
            obj_type="index",
            local_data_list=DummyQuerySet(indexes),
            remote_data_df=_remote_frame(codes),
            hist_handler="get_hist_index_quote_data",
            allow_update=True,
        )

    validation = result["index_validation"]
    assert validation["allowlisted_skips"] == ["sh000015"]
    assert validation["degraded"] is False
    assert result["degraded"] is False


def test_non_core_index_skips_do_not_trip_the_history_circuit_breaker():
    from app.lib.datahub.processors.china_a_stock import (
        HISTORY_FAILURE_CIRCUIT_LIMIT,
    )

    skips = [f"sh9{index:05d}" for index in range(HISTORY_FAILURE_CIRCUIT_LIMIT + 5)]
    codes = ["sh000300"] + skips
    results = {code: _failed_result(no_available_history=True) for code in skips}
    results["sh000300"] = _ok_result()
    processor = _make_processor(results)
    indexes = [_index(code) for code in codes]

    with (
        patch(
            "app.lib.datahub.processors.china_a_stock.progress_bar",
            return_value=lambda *args: None,
        ),
        patch(
            "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
            return_value={},
        ),
    ):
        result = processor.check_data_integrity(
            obj_type="index",
            local_data_list=DummyQuerySet(indexes),
            remote_data_df=_remote_frame(codes),
            hist_handler="get_hist_index_quote_data",
            allow_update=True,
        )

    validation = result["index_validation"]
    assert validation["skipped_count"] == len(skips)
    assert validation["degraded"] is True
    assert processor.get_hist_quote_data.call_count == len(codes)


def test_index_degraded_run_marks_the_job_degraded():

    codes = ["sh000300", "sh000015"]
    results = {
        "sh000300": _ok_result(),
        "sh000015": _failed_result(no_available_history=True),
    }
    processor = _make_processor(results)
    processor.market_name = "ChinaAStock"
    processor.last_job_summary = None
    processor._partial_phase_result = None
    processor._progress_callback = None
    indexes = [_index(code) for code in codes]

    def run_phase(allow_update=False):
        with (
            patch(
                "app.lib.datahub.processors.china_a_stock.progress_bar",
                return_value=lambda *args: None,
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
                return_value={},
            ),
        ):
            return processor.check_data_integrity(
                obj_type="index",
                local_data_list=DummyQuerySet(indexes),
                remote_data_df=_remote_frame(codes),
                hist_handler="get_hist_index_quote_data",
                allow_update=True,
            )

    summary = processor._run_job("index_market_sync", [("indexes", run_phase)])

    assert summary["status"] == "DEGRADED"
    assert summary["index_validation"]["skipped_indices"] == ["sh000015"]
    assert summary["phase_stats"]["indexes"]["index_validation"]["degraded"] is True


def test_has_active_job_run_treats_a_degraded_claim_as_done(monkeypatch):
    import app.lib.utilities.job_run_helper as job_run_helper

    captured = {}

    class FakeRecord:
        status = "DEGRADED"
        started_at = None

    def fake_latest_job_run(**kwargs):
        captured.update(kwargs)
        return FakeRecord()

    monkeypatch.setattr(job_run_helper, "latest_job_run", fake_latest_job_run)

    assert job_run_helper.has_active_job_run(job_family="quote_daily") is True
    assert "DEGRADED" in captured["statuses"]


def test_quote_runner_persists_degraded_status_and_validation(monkeypatch):
    from app.jobs.quote_runner import TARGET_STOCK, QuoteJobMetadata, run_quote_job

    validation = {
        "attempted_symbols": 5223,
        "validated_symbols": 5221,
        "failed_count": 2,
        "failed_codes": ["sh600001", "sz000002"],
        "coverage_rate": 0.999617,
        "degraded": True,
    }

    class DegradedDatahub:
        def set_progress_callback(self, callback):
            pass

        def start_stock_quote_job(self):
            return {
                "job_name": "stock_quote_sync",
                "status": "DEGRADED",
                "degraded": True,
                "failed_phase": None,
                "pulled_total": 5223,
                "written_total": 5221,
                "validated_total": 5221,
                "as_of_date": "2026-08-21T00:00:00",
                "phase_stats": {
                    "check_stock_data_integrity": {
                        "written_count": 5221,
                        "validated_count": 5221,
                        "quote_validation": validation,
                        "degraded": True,
                    }
                },
                "quote_validation": validation,
            }

    captured = {}
    monkeypatch.setattr(
        "app.jobs.quote_runner.job_run_helper.create_job_run", lambda context: object()
    )
    monkeypatch.setattr(
        "app.jobs.quote_runner.job_run_helper.finish_job_run",
        lambda job_run, **kwargs: captured.update(kwargs),
    )

    result = run_quote_job(
        TARGET_STOCK,
        datahub_factory=DegradedDatahub,
        job_metadata=QuoteJobMetadata(
            job_name="datahub_quote_stock_daily",
            job_family="quote_daily",
            trigger="cron",
            source="k8s-cronjob",
        ),
    )

    assert result["status"] == "DEGRADED"
    assert captured["status"] == "DEGRADED"
    assert captured["summary"]["quote_validation"]["failed_count"] == 2
    assert captured["summary"]["quote_validation"]["coverage_rate"] == 0.999617
    assert (
        captured["summary"]["phase_stats"]["check_stock_data_integrity"][
            "quote_validation"
        ]["validated_symbols"]
        == 5221
    )


def test_all_target_status_is_degraded_when_one_target_degrades(monkeypatch):
    from app.jobs.quote_runner import TARGET_ALL, QuoteJobMetadata, run_quote_job

    index_validation = {
        "attempted_symbols": 6,
        "validated_symbols": 5,
        "failed_count": 0,
        "skipped_indices": ["sh000015"],
        "coverage_rate": 0.833333,
        "degraded": True,
    }

    class Datahub:
        def set_progress_callback(self, callback):
            pass

        def start_index_job(self):
            return {
                "status": "DEGRADED",
                "degraded": True,
                "failed_phase": None,
                "pulled_total": 6,
                "written_total": 5,
                "validated_total": 5,
                "as_of_date": "2026-08-21T00:00:00",
                "phase_stats": {},
                "index_validation": index_validation,
            }

        def start_stock_quote_job(self):
            return {
                "status": "SUCCESS",
                "failed_phase": None,
                "pulled_total": 10,
                "written_total": 10,
                "validated_total": 10,
                "as_of_date": "2026-08-21T00:00:00",
                "phase_stats": {},
            }

    captured = {}
    monkeypatch.setattr(
        "app.jobs.quote_runner.job_run_helper.create_job_run", lambda context: object()
    )
    monkeypatch.setattr(
        "app.jobs.quote_runner.job_run_helper.finish_job_run",
        lambda job_run, **kwargs: captured.update(kwargs),
    )

    result = run_quote_job(
        TARGET_ALL,
        datahub_factory=Datahub,
        job_metadata=QuoteJobMetadata(
            job_name="datahub_quote_startup_catchup",
            job_family="quote_daily",
            trigger="startup",
            source="datahub-service",
            scheduled_at=datetime.datetime(2026, 8, 21, 10, 10),
        ),
    )

    assert result["status"] == "DEGRADED"
    assert captured["status"] == "DEGRADED"
    assert captured["summary"]["index_validation"]["skipped_indices"] == ["sh000015"]
