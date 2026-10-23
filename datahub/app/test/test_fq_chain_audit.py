"""Tests for the read-only FQ/HFQ chain audit."""

import datetime

import pytest


def _row(day, close, factor, hfq=None):
    return {
        "date": datetime.datetime(2024, 1, day),
        "open": close,
        "close": close,
        "high": close,
        "low": close,
        "fq_factor": factor,
        "open_hfq": close * factor if hfq is None else hfq,
        "close_hfq": close * factor if hfq is None else hfq,
        "high_hfq": close * factor if hfq is None else hfq,
        "low_hfq": close * factor if hfq is None else hfq,
    }


def test_compare_factor_series_reports_exact_within_and_beyond_tolerance():
    from app.lib.datahub.data_integrity_keeper.handler import fq_chain_audit

    rows = [
        _row(2, 10.0, 2.0),
        _row(3, 10.0, 7.2118),
        _row(4, 10.0, 3.0),
    ]
    source = {
        datetime.datetime(2024, 1, 2): 2.0,
        datetime.datetime(2024, 1, 3): 7.212,
        datetime.datetime(2024, 1, 4): 3.001,
    }

    result = fq_chain_audit.compare_factor_series(rows, source, tolerance=5e-4)

    assert result["compared"] == 3
    # 0.0002 rounds inside tolerance; 0.001 is a real divergence
    assert result["mismatch_count"] == 1
    assert result["mismatch_examples"][0]["date"] == "2024-01-04"
    assert result["max_abs_diff"] == pytest.approx(0.001)
    assert result["missing_in_source"] == 0
    assert result["leading_before_source"] is False


def test_compare_factor_series_flags_source_gaps_and_leading_rows():
    from app.lib.datahub.data_integrity_keeper.handler import fq_chain_audit

    rows = [_row(1, 10.0, 2.0), _row(2, 10.0, 2.0)]
    source = {datetime.datetime(2024, 1, 2): 2.0}

    result = fq_chain_audit.compare_factor_series(rows, source)

    assert result["compared"] == 1
    assert result["missing_in_source"] == 1
    assert result["leading_before_source"] is True


def test_check_hfq_arithmetic_detects_scaling_and_bad_factors():
    from app.lib.datahub.data_integrity_keeper.handler import fq_chain_audit

    good = _row(2, 10.0, 2.0)
    bad_scale = _row(3, 10.0, 2.0, hfq=999.0)
    zero_factor = _row(4, 10.0, 0.0)

    result = fq_chain_audit.check_hfq_arithmetic([good, bad_scale, zero_factor])

    assert result["checked"] == 8  # two rows with a positive factor x 4 fields
    assert result["bad_count"] == 4  # every field of the bad row
    assert result["non_positive_factor_count"] == 1


def test_run_audit_verdict_fails_on_missing_coverage(monkeypatch):
    from app.lib.datahub.data_integrity_keeper.handler import fq_chain_audit

    monkeypatch.setattr(
        fq_chain_audit,
        "load_coverage",
        lambda db: fq_chain_audit.coverage_summary(
            active_stock_count=10,
            quoted_active_count=10,
            missing_fq_count=1,
            latest_quote_date=datetime.date(2024, 1, 4),
        ),
    )
    monkeypatch.setattr(fq_chain_audit, "load_status_summary", lambda db: {"OK": 10})

    report = fq_chain_audit.run_audit(object(), sample_codes=[], fetch_source=None)

    assert report["verdict"] == "FAIL"


def test_run_audit_verdict_passes_clean_sample(monkeypatch):
    from app.lib.datahub.data_integrity_keeper.handler import fq_chain_audit

    monkeypatch.setattr(
        fq_chain_audit,
        "load_coverage",
        lambda db: fq_chain_audit.coverage_summary(
            active_stock_count=10,
            quoted_active_count=10,
            missing_fq_count=0,
            latest_quote_date=datetime.date(2024, 1, 4),
        ),
    )
    monkeypatch.setattr(fq_chain_audit, "load_status_summary", lambda db: {"OK": 10})
    monkeypatch.setattr(
        fq_chain_audit,
        "audit_code",
        lambda db, code, source, tolerance=5e-4: {
            "code": code,
            "status": "PASS",
            "rows": 2,
        },
    )

    report = fq_chain_audit.run_audit(
        object(),
        sample_codes=["sh600000"],
        fetch_source=lambda code: {},
    )

    assert report["verdict"] == "PASS"
    assert report["sample"][0]["status"] == "PASS"


def test_run_audit_reports_source_errors_without_hiding_them(monkeypatch):
    from app.lib.datahub.data_integrity_keeper.handler import fq_chain_audit

    monkeypatch.setattr(
        fq_chain_audit,
        "load_coverage",
        lambda db: fq_chain_audit.coverage_summary(
            active_stock_count=10,
            quoted_active_count=10,
            missing_fq_count=0,
            latest_quote_date=datetime.date(2024, 1, 4),
        ),
    )
    monkeypatch.setattr(fq_chain_audit, "load_status_summary", lambda db: {"OK": 10})

    def _boom(code):
        raise RuntimeError("token missing")

    report = fq_chain_audit.run_audit(
        object(), sample_codes=["sh600000"], fetch_source=_boom
    )

    assert report["sample"][0]["status"] == "ERROR"
    assert "token missing" in report["sample"][0]["message"]


def test_compare_factor_series_treats_exact_tolerance_as_match():
    from app.lib.datahub.data_integrity_keeper.handler import fq_chain_audit

    rows = [_row(2, 10.0, 2.4765)]
    source = {datetime.datetime(2024, 1, 2): 2.476}

    result = fq_chain_audit.compare_factor_series(rows, source, tolerance=5e-4)

    # 0.0005 is the declared tolerance (provider re-publication rounding), so
    # it must not fail because of a 1e-16 float representation error.
    assert result["mismatch_count"] == 0
    assert result["max_abs_diff"] == pytest.approx(0.0005)
