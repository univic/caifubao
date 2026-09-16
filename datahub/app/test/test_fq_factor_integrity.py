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

    def find_one(self, flt=None, projection=None):
        for doc in self.docs:
            if self._matches(doc, flt or {}):
                return doc
        return None

    def _matches(self, doc, flt):
        for key, value in flt.items():
            if key == "object_type" and value == "individual_stock":
                if doc.get("object_type") != "individual_stock":
                    return False
            elif isinstance(value, dict) and "$gte" in value:
                candidate = doc.get(key)
                if not isinstance(candidate, datetime.datetime):
                    return False
                if not (value["$gte"] <= candidate <= value["$lte"]):
                    return False
            elif doc.get(key) != value:
                return False
        return True

    def find(self, flt=None, projection=None):
        if self.universe:
            # The universe query carries object_type/name predicates; apply
            # them so a filter regression cannot pass unnoticed.
            return _FakeCursor(
                [d for d in self.universe if self._matches(d, flt or {})]
            )
        return _FakeCursor([d for d in self.docs if self._matches(d, flt or {})])


class _FakeDb:
    def __init__(self, quotes, universe=None, trading_days=None):
        universe_docs = [
            {"code": code, "object_type": "individual_stock"}
            for code in (universe or [])
        ]
        # finance_market.trade_calendar is a ListField(DateTimeField)
        calendar = [
            datetime.datetime.combine(day, datetime.time.min)
            for day in (trading_days or [])
        ]
        self._collections = {
            "stock_daily_quote": _FakeCollection(quotes),
            "basic_stock": _FakeCollection([], universe=universe_docs),
            "finance_market": _FakeCollection(
                [{"name": "ChinaAStock", "trade_calendar": calendar}]
            ),
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
    assert report["warnings"]


def test_trade_calendar_adjacency_covers_a_long_holiday():
    """The first session after a long holiday must still be compared."""
    quotes = [
        _quote("sh600000", datetime.date(2026, 9, 30), 30.95),
        _quote("sh600000", datetime.date(2026, 10, 12), 6.66),
    ]
    # 12 calendar days apart, but adjacent sessions in the trade calendar.
    db = _FakeDb(
        quotes,
        universe=["sh600000"],
        trading_days=[datetime.date(2026, 9, 30), datetime.date(2026, 10, 12)],
    )

    report = fqi.scan_fq_factor_jumps(
        db, date_from=datetime.date(2026, 9, 1), date_to=datetime.date(2026, 10, 31)
    )

    assert report["basis"]["adjacency"] == "trade_calendar"
    assert report["counters"]["skipped_gap"] == 0
    assert report["top_dates"][0]["date"] == "2026-10-12"
    assert report["top_dates"][0]["jump_count"] == 1


def test_calendar_days_fallback_and_override():
    quotes = [
        _quote("sh600000", datetime.date(2026, 8, 1), 30.95),
        _quote("sh600000", datetime.date(2026, 8, 12), 6.66),
    ]
    db = _FakeDb(quotes, universe=["sh600000"])

    default = fqi.scan_fq_factor_jumps(
        db, date_from=datetime.date(2026, 8, 1), date_to=datetime.date(2026, 8, 31)
    )
    widened = fqi.scan_fq_factor_jumps(
        db,
        date_from=datetime.date(2026, 8, 1),
        date_to=datetime.date(2026, 8, 31),
        max_gap_days=15,
    )

    assert default["basis"]["adjacency"] == "calendar_days"
    assert default["counters"]["skipped_gap"] == 1
    assert default["skipped_gap_by_date"] == {"2026-08-12": 1}
    assert widened["top_dates"][0]["jump_count"] == 1


def test_zero_factor_on_either_side_is_not_a_jump():
    quotes = [
        _quote("sh600000", datetime.date(2026, 8, 1), 0.0),
        _quote("sh600000", datetime.date(2026, 8, 4), 5.0),
        _quote("sh600001", datetime.date(2026, 8, 1), 10.0),
        _quote("sh600001", datetime.date(2026, 8, 4), 0.0),
    ]
    db = _FakeDb(quotes, universe=["sh600000", "sh600001"])

    report = fqi.scan_fq_factor_jumps(
        db, date_from=datetime.date(2026, 8, 1), date_to=datetime.date(2026, 8, 10)
    )

    assert report["top_dates"] == []
    assert report["counters"]["skipped_zero_factor"] == 2
    assert report["skipped_zero_factor_by_date"] == {"2026-08-04": 2}
    assert report["max_change"] == 0.0


def test_duplicate_dates_are_counted_not_compared():
    day = datetime.date(2026, 8, 4)
    quotes = [
        _quote("sh600000", day, 6.66),
        _quote("sh600000", day, 30.95),
        _quote("sh600000", datetime.date(2026, 8, 1), 30.95),
    ]
    db = _FakeDb(quotes, universe=["sh600000"])

    report = fqi.scan_fq_factor_jumps(
        db, date_from=datetime.date(2026, 8, 1), date_to=datetime.date(2026, 8, 10)
    )

    assert report["counters"]["duplicate_dates"] == 1
    # The duplicated pair is dropped and the next older row re-baselines, so
    # no jump is attributed to a date whose value is ambiguous.
    assert report["top_dates"] == []
    assert report["counters"]["pairs_compared"] == 0


def test_non_datetime_dates_never_reach_the_scan():
    """The date-range filter excludes them, so the counter stays defensive."""
    day = datetime.date(2026, 8, 4)
    quotes = [
        _quote("sh600000", day, 30.95),
        {"code": "sh600000", "date": "not-a-date", "fq_factor": 1.0},
    ]
    db = _FakeDb(quotes, universe=["sh600000"])

    report = fqi.scan_fq_factor_jumps(
        db, date_from=datetime.date(2026, 8, 1), date_to=datetime.date(2026, 8, 10)
    )

    assert report["counters"]["docs_scanned"] == 1
    assert report["counters"]["docs_skipped_bad_date"] == 0


def test_require_universe_refuses_an_unfiltered_acceptance_scan():
    quotes = [_quote("sh600000", datetime.date(2026, 8, 1), 30.95)]
    db = _FakeDb(quotes, universe=[])

    with pytest.raises(ValueError, match="universe is empty"):
        fqi.scan_fq_factor_jumps(
            db,
            date_from=datetime.date(2026, 8, 1),
            date_to=datetime.date(2026, 8, 10),
            require_universe=True,
        )


def test_non_finite_and_negative_thresholds_are_rejected():
    db = _FakeDb([], universe=["sh600000"])
    window = {
        "date_from": datetime.date(2026, 8, 1),
        "date_to": datetime.date(2026, 8, 10),
    }
    for bad in (float("nan"), float("inf"), -0.1):
        with pytest.raises(ValueError, match="finite number >= 0"):
            fqi.scan_fq_factor_jumps(db, threshold=bad, **window)
    with pytest.raises(ValueError, match="max_gap_days must be >= 1"):
        fqi.scan_fq_factor_jumps(db, max_gap_days=0, **window)
    with pytest.raises(ValueError, match="top_n must be >= 1"):
        fqi.scan_fq_factor_jumps(db, top_n=0, **window)


def test_duplicate_dates_are_tie_order_independent():
    """Both duplicate orders must produce the same report."""
    day = datetime.date(2026, 8, 4)
    older = datetime.date(2026, 8, 1)
    first = _FakeDb(
        [
            _quote("sh600000", day, 6.66),
            _quote("sh600000", day, 30.95),
            _quote("sh600000", older, 30.95),
        ],
        universe=["sh600000"],
    )
    second = _FakeDb(
        [
            _quote("sh600000", day, 30.95),
            _quote("sh600000", day, 6.66),
            _quote("sh600000", older, 30.95),
        ],
        universe=["sh600000"],
    )
    window = {
        "date_from": datetime.date(2026, 8, 1),
        "date_to": datetime.date(2026, 8, 10),
    }
    reports = [fqi.scan_fq_factor_jumps(db, **window) for db in (first, second)]
    assert reports[0]["top_dates"] == reports[1]["top_dates"] == []
    assert reports[0]["counters"]["duplicate_dates"] == 1
    assert reports[1]["counters"]["duplicate_dates"] == 1


def test_calendar_fallback_pairs_are_counted():
    quotes = [
        _quote("sh600000", datetime.date(2026, 8, 10), 30.95),
        _quote("sh600000", datetime.date(2026, 8, 11), 6.66),
    ]
    # The newer date is outside the loaded calendar window keys, so the pair
    # uses the calendar-day fallback and must be counted as such.
    db = _FakeDb(
        quotes,
        universe=["sh600000"],
        trading_days=[datetime.date(2026, 8, 10), datetime.date(2026, 8, 11)],
    )
    report = fqi.scan_fq_factor_jumps(
        db,
        date_from=datetime.date(2026, 8, 10),
        date_to=datetime.date(2026, 8, 11),
    )
    assert report["basis"]["trading_days_in_window"] == 2
    assert report["counters"]["pairs_via_calendar_fallback"] == 0
    assert report["counters"]["pairs_compared"] == 1


def test_allow_long_window_opts_out_of_the_guard():
    db = _FakeDb([], universe=["sh600000"])
    report = fqi.scan_fq_factor_jumps(
        db,
        date_from=datetime.date(2024, 1, 1),
        date_to=datetime.date(2026, 8, 1),
        allow_long_window=True,
    )
    assert report["scanned"]["docs"] == 0


def test_universe_query_filter_is_applied():
    """A wrong object_type filter must not silently return every code."""
    db = _FakeDb(
        [_quote("sh600000", datetime.date(2026, 8, 1), 30.95)],
        universe=["sh600000"],
    )
    basic_stock = db["basic_stock"]
    assert basic_stock.find({"object_type": "stock_index"}).docs == []
    assert basic_stock.find({"object_type": "individual_stock"}).docs == [
        {"code": "sh600000", "object_type": "individual_stock"}
    ]


def test_cli_requires_from_date_for_the_jump_scan():
    import subprocess
    import sys
    from pathlib import Path

    datahub_root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "scripts/check_fq_factor_integrity.py", "--jump-scan"],
        cwd=datahub_root,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert "--jump-scan requires --from-date" in result.stderr


def test_report_carries_the_baseline_note_and_window():
    db = _FakeDb([], universe=["sh600000"])
    report = fqi.scan_fq_factor_jumps(
        db, date_from=datetime.date(2026, 8, 1), date_to=datetime.date(2026, 8, 10)
    )
    assert report["window"] == {"from": "2026-08-01", "to": "2026-08-10"}
    assert "2026-08-31" in report["baseline_note"]
    assert report["counters"]["docs_scanned"] == 0
