"""Tests for the read-only market-wide FQ factor jump scan."""

from __future__ import annotations

import datetime

import pytest

from app.lib.datahub.data_integrity_keeper.handler import fq_factor_integrity as fqi


DAY = datetime.datetime(2026, 8, 1)


def _quote(code, day, fq_factor, object_type="individual_stock"):
    return {
        "code": code,
        "date": datetime.datetime.combine(day, datetime.time.min),
        "fq_factor": fq_factor,
        "object_type": object_type,
    }


class _FakeCursor:
    def __init__(self, docs):
        self.docs = docs

    def sort(self, spec):
        pairs = spec if isinstance(spec, list) else [spec]
        for key, direction in reversed(pairs):
            self.docs = sorted(
                self.docs, key=lambda d: d.get(key), reverse=direction < 0
            )
        return self

    def batch_size(self, _n):
        return self

    def __iter__(self):
        return iter(self.docs)


class _FakeCollection:
    def __init__(self, docs, universe=None):
        self.docs = docs
        self.universe = universe or []

    def _matches(self, doc, flt):
        for key, value in flt.items():
            if key == "object_type" and value == "individual_stock":
                if doc.get("object_type") != "individual_stock":
                    return False
            elif isinstance(value, dict) and "$gte" in value:
                if not (value["$gte"] <= doc["date"] <= value["$lte"]):
                    return False
            elif doc.get(key) != value:
                return False
        return True

    def find(self, flt=None, projection=None):
        if self.universe:
            return _FakeCursor([{"code": c} for c in self.universe])
        return _FakeCursor([d for d in self.docs if self._matches(d, flt or {})])


class _FakeDb:
    def __init__(self, quotes, universe=None):
        self._collections = {
            "stock_daily_quote": _FakeCollection(quotes),
            "basic_stock": _FakeCollection([], universe=universe),
        }

    def __getitem__(self, name):
        return self._collections[name]


def _window():
    return {
        "date_from": datetime.date(2026, 8, 1),
        "date_to": datetime.date(2026, 8, 10),
    }


def test_requires_an_explicit_window():
    db = _FakeDb([])
    with pytest.raises(ValueError, match="date_from and date_to are required"):
        fqi.scan_fq_factor_jumps(db, date_from=None, date_to=None)


def test_rejects_inverted_window():
    db = _FakeDb([])
    with pytest.raises(ValueError, match="must not be after"):
        fqi.scan_fq_factor_jumps(
            db,
            date_from=datetime.date(2026, 8, 10),
            date_to=datetime.date(2026, 8, 1),
        )


def test_fails_loud_on_oversized_window():
    db = _FakeDb([])
    with pytest.raises(ValueError, match="exceeds the 500-day guard"):
        fqi.scan_fq_factor_jumps(
            db,
            date_from=datetime.date(2024, 1, 1),
            date_to=datetime.date(2026, 8, 1),
        )


def test_market_wide_jump_is_ranked_and_reported_for_the_target_date():
    """A 2026-08-31-style market-wide discontinuity must dominate the report."""
    quotes = []
    for i in range(50):
        code = f"sh60{i:04d}"
        quotes.append(_quote(code, datetime.date(2026, 8, 28), 30.95))
        quotes.append(_quote(code, datetime.date(2026, 8, 31), 6.66))
        quotes.append(_quote(code, datetime.date(2026, 9, 1), 6.66))
    db = _FakeDb(quotes, universe=[f"sh60{i:04d}" for i in range(50)])

    report = fqi.scan_fq_factor_jumps(
        db,
        date_from=datetime.date(2026, 8, 1),
        date_to=datetime.date(2026, 9, 10),
        target_date=datetime.date(2026, 8, 31),
    )

    assert report["top_dates"][0]["date"] == "2026-08-31"
    assert report["top_dates"][0]["jump_count"] == 50
    assert report["top_dates"][0]["fraction_of_universe"] == 1.0
    assert report["target_date"]["jump_count"] == 50
    assert report["target_date"]["date"] == "2026-08-31"
    assert report["scanned"]["codes"] == 50
    assert report["scanned"]["universe_size"] == 50
    assert report["max_change_ref"]["date"] == "2026-08-31"


def test_stable_factors_produce_no_jumps():
    quotes = []
    for i in range(20):
        code = f"sz00{i:04d}"
        for day in range(1, 6):
            quotes.append(_quote(code, datetime.date(2026, 8, day), 6.66))
    db = _FakeDb(quotes, universe=[f"sz00{i:04d}" for i in range(20)])

    report = fqi.scan_fq_factor_jumps(
        db, date_from=datetime.date(2026, 8, 1), date_to=datetime.date(2026, 8, 10)
    )

    assert report["top_dates"] == []
    assert report["jump_dates"] == 0
    assert report["counters"]["pairs_compared"] == 80


def test_long_gap_rows_are_not_counted_as_one_day_jumps():
    """A stock resuming after a suspension must not look like a daily jump."""
    quotes = [
        _quote("sh600000", datetime.date(2026, 8, 1), 30.95),
        _quote("sh600000", datetime.date(2026, 8, 28), 6.66),
    ]
    db = _FakeDb(quotes, universe=["sh600000"])

    report = fqi.scan_fq_factor_jumps(
        db, date_from=datetime.date(2026, 8, 1), date_to=datetime.date(2026, 8, 31)
    )

    assert report["top_dates"] == []
    assert report["counters"]["skipped_gap"] == 1


def test_index_rows_and_missing_factors_are_excluded():
    quotes = [
        _quote("sh000001", datetime.date(2026, 8, 1), 10.0, object_type="stock_index"),
        _quote("sh000001", datetime.date(2026, 8, 4), 6.0, object_type="stock_index"),
        _quote("sh600000", datetime.date(2026, 8, 1), 30.95),
        _quote("sh600000", datetime.date(2026, 8, 4), None),
        _quote("sh600001", datetime.date(2026, 8, 1), 30.95),
        _quote("sh600001", datetime.date(2026, 8, 4), 6.66),
    ]
    db = _FakeDb(quotes, universe=["sh600000", "sh600001"])

    report = fqi.scan_fq_factor_jumps(
        db, date_from=datetime.date(2026, 8, 1), date_to=datetime.date(2026, 8, 10)
    )

    assert report["counters"]["docs_skipped_non_stock"] == 2
    assert report["counters"]["docs_skipped_missing_factor"] == 1
    # sh600000 contributes its 08-01 row (the 08-04 row has no factor) and
    # sh600001 contributes both, so two codes are in the scanned universe.
    assert report["scanned"]["codes"] == 2
    assert report["counters"]["pairs_compared"] == 1
    assert report["target_date"] is None
    assert report["top_dates"][0]["jump_count"] == 1


def test_threshold_is_configurable():
    quotes = [
        _quote("sh600000", datetime.date(2026, 8, 1), 10.0),
        _quote("sh600000", datetime.date(2026, 8, 4), 11.0),
    ]
    db = _FakeDb(quotes, universe=["sh600000"])

    strict = fqi.scan_fq_factor_jumps(
        db,
        date_from=datetime.date(2026, 8, 1),
        date_to=datetime.date(2026, 8, 10),
        threshold=0.05,
    )
    loose = fqi.scan_fq_factor_jumps(
        db,
        date_from=datetime.date(2026, 8, 1),
        date_to=datetime.date(2026, 8, 10),
        threshold=0.5,
    )

    assert strict["top_dates"][0]["jump_count"] == 1
    assert loose["top_dates"] == []
    assert loose["counters"]["pairs_compared"] == 1


def test_unknown_universe_disables_non_stock_filtering_and_is_reported():
    quotes = [
        _quote("sh600000", datetime.date(2026, 8, 1), 30.95),
        _quote("sh600000", datetime.date(2026, 8, 4), 6.66),
    ]
    db = _FakeDb(quotes, universe=[])

    report = fqi.scan_fq_factor_jumps(
        db, date_from=datetime.date(2026, 8, 1), date_to=datetime.date(2026, 8, 10)
    )

    assert report["scanned"]["universe_known"] is False
    assert report["scanned"]["universe_size"] is None
    assert report["top_dates"][0]["fraction_of_universe"] is None
    assert report["target_date"] is None


def test_report_carries_the_baseline_note_and_window():
    db = _FakeDb([], universe=["sh600000"])
    report = fqi.scan_fq_factor_jumps(
        db, date_from=datetime.date(2026, 8, 1), date_to=datetime.date(2026, 8, 10)
    )
    assert report["window"] == {"from": "2026-08-01", "to": "2026-08-10"}
    assert "2026-08-31" in report["baseline_note"]
    assert report["counters"]["docs_scanned"] == 0
