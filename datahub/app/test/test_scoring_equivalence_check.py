# -*- coding: utf-8 -*-
"""Tests for the C1 dev-side equivalence/timing check (perf 3.5/5.3).

The check's production entry point is
``python -m app.jobs.scoring_runner equivalence-check``; here it runs against the
in-memory fake models so the orchestration (read-only preview, two replace
passes, field diff, fail-closed exit code, report shape) is covered without a
database.
"""

import datetime

import pytest

from app.lib.scoring_engine.equivalence_check import (
    diff_snapshots,
    run_equivalence_check,
    snapshot_predictions,
)
from app.lib.scoring_engine.scoring_service import StockScoringService
import app.test.test_scoring_batch_equivalence as batch_harness_module
from app.test.test_scoring_batch_equivalence import (
    FakeIndustryClassification,
    FakeIndustryMetrics,
    FakeFactor,
    FakePrediction,
    FakeQuote,
    FakeSignal,
    FakeStock,
    CALENDAR,
    EVAL_DATE,
    seed_market,
)


@pytest.fixture
def calendar():
    return list(CALENDAR)


@pytest.fixture
def batch_harness(calendar):
    """Reuse the C1 equivalence fixture (patched market/industry/index reads)."""
    yield from batch_harness_module.batch_harness.__wrapped__(calendar)


@pytest.fixture
def fake_service_factory(batch_harness):
    """Factory mirroring the job's, backed by the shared fake models."""

    def factory(batch_prefetch: bool):
        service = StockScoringService(
            stock_model=FakeStock,
            quote_model=FakeQuote,
            factor_model=FakeFactor,
            signal_model=FakeSignal,
            prediction_model=FakePrediction,
            industry_model=FakeIndustryClassification,
            industry_metrics_model=FakeIndustryMetrics,
            batch_prefetch=batch_prefetch,
        )
        service.calendar = batch_harness.calendar
        return service

    return factory


def test_read_only_preview_runs_nothing(batch_harness, fake_service_factory):
    seed_market()
    FakePrediction.bulk_calls = []

    report = run_equivalence_check(
        fake_service_factory, date=EVAL_DATE, horizons=[5], apply=False
    )

    assert report["applied"] is False
    assert report["ok"] is None
    assert report["active_codes"] == len(FakeStock.records)
    assert report["market"]["quotes"] > 0
    assert report["plan"]
    assert "horizons_checked" in report and report["horizons_checked"] == []
    # Preflight window coverage: enough history rows and industry metrics.
    assert report["window"]["quote_rows"] > 0
    assert report["window"]["expected_rows"] == len(FakeStock.records) * 20
    assert report["window"]["index_rows"] > 0
    assert report["window"]["industry_metrics_before_date"] > 0
    assert report["warnings"] == []
    # No scoring pass means no prediction writes at all.
    assert FakePrediction.records == []
    assert FakePrediction.bulk_calls == []


def test_apply_reports_equivalence_timing_and_rows(fake_service_factory):
    seed_market()

    report = run_equivalence_check(
        fake_service_factory, date=EVAL_DATE, horizons=[5, 20], apply=True
    )

    assert report["applied"] is True
    assert report["ok"] is True
    assert report["diff_count"] == 0
    assert report["diffs"] == []
    assert [entry["horizon"] for entry in report["horizons_checked"]] == [5, 20]
    for entry in report["horizons_checked"]:
        assert entry["rows"] == entry["expected_rows"] == len(FakeStock.records)
        assert entry["legacy_seconds"] >= 0
        assert entry["batch_seconds"] >= 0
        assert "scored_count" in entry["legacy_result"]
        assert entry["diff_count"] == 0
    assert report["warnings"] == []


def test_apply_ranked_mode_matches(fake_service_factory):
    seed_market()

    report = run_equivalence_check(
        fake_service_factory, date=EVAL_DATE, horizons=[20], mode="ranked", apply=True
    )

    assert report["mode"] == "ranked"
    assert report["ok"] is True
    assert report["diff_count"] == 0


def test_mode_contradicting_env_fails_closed(fake_service_factory, monkeypatch):
    """`--mode raw` must not silently run the ranked path.

    ``mode="raw"`` delegates to ``score_all_stocks``, which re-dispatches on
    DATAHUB_SCORING_MODE; without this guard the report would label a ranked
    run as raw and compare one path against itself.
    """
    import pytest

    seed_market()
    monkeypatch.setenv("DATAHUB_SCORING_MODE", "ranked")

    with pytest.raises(ValueError, match="contradicts"):
        run_equivalence_check(
            fake_service_factory, date=EVAL_DATE, horizons=[20], mode="raw", apply=True
        )

    # The matching mode still works with the same env in place.
    report = run_equivalence_check(
        fake_service_factory, date=EVAL_DATE, horizons=[20], mode="ranked", apply=True
    )
    assert report["ok"] is True


def test_partial_ranked_cohort_repair_matches(fake_service_factory):
    """The check must also pass when the date already holds stored rows: both
    passes use replace=True, so pre-existing state cannot leak into the diff."""
    seed_market()
    run_equivalence_check(
        fake_service_factory, date=EVAL_DATE, horizons=[20], mode="ranked", apply=True
    )

    report = run_equivalence_check(
        fake_service_factory, date=EVAL_DATE, horizons=[20], mode="ranked", apply=True
    )

    assert report["ok"] is True
    assert report["diff_count"] == 0


def test_apply_reports_divergence_and_fails_closed(fake_service_factory, monkeypatch):
    seed_market()
    original = StockScoringService._calculate_score

    def perturbed(self, components, penalties):
        if self.batch_prefetch:
            return round(original(self, components, penalties) + 0.01, 2)
        return original(self, components, penalties)

    monkeypatch.setattr(StockScoringService, "_calculate_score", perturbed)

    report = run_equivalence_check(
        fake_service_factory, date=EVAL_DATE, horizons=[5], apply=True
    )

    assert report["ok"] is False
    assert report["diff_count"] > 0
    assert any("score:" in diff for diff in report["diffs"])


def test_snapshot_and_diff_helpers():
    date = datetime.datetime(2026, 4, 10)
    left = [
        FakePrediction(
            stock_code="sh600000",
            date=date,
            horizon=5,
            model_version="v1",
            score=10.0,
            rank=1,
            stock="oid-sh600000",
        )
    ]
    right = [
        FakePrediction(
            stock_code="sh600000",
            date=date,
            horizon=5,
            model_version="v1",
            score=10.0,
            rank=1,
            stock="oid-sh600000",
        )
    ]
    assert snapshot_predictions(left) == snapshot_predictions(right)
    assert diff_snapshots(snapshot_predictions(left), snapshot_predictions(right)) == []

    right[0].score = 10.5
    right.append(
        FakePrediction(
            stock_code="sh600001",
            date=date,
            horizon=5,
            model_version="v1",
            score=1.0,
        )
    )
    diffs = diff_snapshots(snapshot_predictions(left), snapshot_predictions(right))
    assert any("score:" in item for item in diffs)
    assert any("missing from the per-stock path" in item for item in diffs)

    only_left = diff_snapshots(snapshot_predictions(right), snapshot_predictions(left))
    assert any("missing from the batch path" in item for item in only_left)


def test_empty_date_is_warned_but_not_applied(fake_service_factory):
    seed_market()
    empty_date = EVAL_DATE + datetime.timedelta(days=1)

    report = run_equivalence_check(
        fake_service_factory, date=empty_date, horizons=[5], apply=False
    )

    assert report["market"]["quotes"] == 0
    assert any("no quotes" in warning for warning in report["warnings"])
    assert report["applied"] is False


def test_missing_history_window_is_warned(fake_service_factory):
    """A date far past the seeded history has no window: the preflight must say
    so before the operator pays for two full-market passes."""
    seed_market()
    future_date = EVAL_DATE + datetime.timedelta(days=400)

    report = run_equivalence_check(
        fake_service_factory, date=future_date, horizons=[5], apply=False
    )

    assert report["window"]["quote_rows"] == 0
    assert report["window"]["index_rows"] == 0
    assert any("history window" in warning for warning in report["warnings"])
