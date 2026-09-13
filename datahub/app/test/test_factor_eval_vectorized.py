# -*- coding: utf-8 -*-
"""Equivalence tests for the vectorised factor-evaluation dataset (perf 3.19).

``FactorEvaluationService.evaluate`` can resolve forward returns two ways:

* no ``quote_frame`` -> the legacy per-observation DB reads
  (``code + date`` for the base, ``code + date range`` per horizon);
* with ``quote_frame`` -> positional ``groupby("code").shift(-h)`` over a
  preloaded price series.

Both must produce the SAME dataset and the SAME report, including the legacy
quirks: the ``int(h * 1.5)`` calendar window, a missing/suspended base quote
dropping the observation, a non-positive forward price yielding ``None``, and
fewer than ``h`` remaining rows yielding ``None``.
"""

import datetime
import random

import pytest

from app.lib.scoring_engine.factor_eval import (
    DECAY_HORIZONS,
    FactorEvaluationService,
    _row_price,
)

HORIZONS = [5, 20, 60]


class _QuerySet:
    def __init__(self, items):
        self.items = list(items)

    def first(self):
        return self.items[0] if self.items else None

    def order_by(self, *fields):
        items = self.items
        for field in reversed(fields):
            reverse = field.startswith("-")
            key = field[1:] if field[:1] in "+-" else field
            items = sorted(items, key=lambda row: row[key], reverse=reverse)
        return _QuerySet(items)

    def limit(self, count):
        return _QuerySet(self.items[:count])

    def __iter__(self):
        return iter(self.items)


def _matches(row, query):
    for key, value in query.items():
        if key.endswith("__gt"):
            if not row[key[:-4]] > value:
                return False
        elif key.endswith("__lte"):
            if not row[key[:-5]] <= value:
                return False
        elif row.get(key) != value:
            return False
    return True


class _Manager:
    def __init__(self, rows, reads):
        self._rows = rows
        self._reads = reads

    def __call__(self, **query):
        self._reads.append(query)
        return _QuerySet([row for row in self._rows if _matches(row, query)])


class FakeQuoteModel:
    """Minimal stand-in exposing only the read shapes ``factor_eval`` uses."""

    def __init__(self, rows):
        self.rows = list(rows)
        self.reads = []
        self.objects = _Manager(self.rows, self.reads)


def _frame_from(rows):
    frame = {}
    for row in rows:
        frame.setdefault(row["code"], []).append((row["date"], _row_price(row)))
    for series in frame.values():
        series.sort(key=lambda pair: pair[0])
    return frame


def _service(rows):
    service = FactorEvaluationService(quote_model=FakeQuoteModel(rows))
    # Correlation reads persisted predictions; it is orthogonal to forward
    # returns and needs a live DB, so stub it out for the differential test.
    service._compute_component_correlation = lambda *a, **k: {}
    return service


def _panel(rng, codes=4, days=45):
    """Random price panel with gaps, zero prices and missing HFQ values."""
    start = datetime.datetime(2026, 1, 5)
    rows = []
    for code_index in range(codes):
        code = f"sh60{code_index:04d}"
        day = start
        emitted = 0
        while emitted < days:
            if rng.random() < 0.85:  # gaps: suspended / missing rows
                price = rng.choice([0.0, rng.uniform(3, 60)])
                hfq = None if rng.random() < 0.3 else round(price * 7, 4)
                rows.append(
                    {
                        "code": code,
                        "date": day,
                        "close": price,
                        "close_hfq": hfq,
                    }
                )
                emitted += 1
            day += datetime.timedelta(days=1)
    return rows


def _factor_values(rng, rows):
    """Observations, including ones the legacy path must reject."""
    by_code = {}
    for row in rows:
        by_code.setdefault(row["code"], []).append(row["date"])
    values = {}
    for code, dates in by_code.items():
        picked = {}
        for date in dates:
            if rng.random() < 0.7:
                picked[date.isoformat()] = rng.uniform(-3, 3)
        # A value on a date with no quote at all: must be skipped by both paths.
        picked[datetime.datetime(2030, 1, 1).isoformat()] = 1.0
        # An unparseable date and an explicit None value.
        picked["not-a-date"] = 2.0
        if dates:
            picked[dates[0].isoformat()] = None
        values[code] = picked
    return values


def test_vectorised_dataset_matches_per_observation_path():
    rng = random.Random(20260914)
    rows = _panel(rng)
    values = _factor_values(rng, rows)
    service = _service(rows)
    start, end = datetime.datetime(2026, 1, 1), datetime.datetime(2026, 4, 30)

    legacy = service._build_dataset(values, start, end, sorted(HORIZONS))
    frame = service._build_dataset(
        values, start, end, sorted(HORIZONS), quote_frame=_frame_from(rows)
    )

    assert legacy, "fixture must produce observations"
    assert legacy == frame


def test_evaluate_report_matches_between_paths():
    rng = random.Random(7)
    rows = _panel(rng)
    values = _factor_values(rng, rows)
    service = _service(rows)
    start, end = datetime.datetime(2026, 1, 1), datetime.datetime(2026, 4, 30)

    legacy = service.evaluate(values, start, end, forward_horizons=HORIZONS)
    vectorised = service.evaluate(
        values,
        start,
        end,
        forward_horizons=HORIZONS,
        quote_frame=_frame_from(rows),
    )

    assert legacy["observation_count"] > 0
    assert legacy == vectorised
    # The decay curve must still be present and go through the same dataset.
    assert set(vectorised["decay"]) == {str(h) for h in DECAY_HORIZONS}


def test_forward_returns_cover_every_decay_horizon_in_one_dataset():
    """One build serves the requested horizons AND the decay curve."""
    rows = _panel(random.Random(3), codes=2, days=140)
    values = _factor_values(random.Random(4), rows)
    service = _service(rows)
    start, end = datetime.datetime(2026, 1, 1), datetime.datetime(2026, 8, 31)

    dataset = service._build_dataset(
        values,
        start,
        end,
        sorted(set(HORIZONS) | set(DECAY_HORIZONS)),
        quote_frame=_frame_from(rows),
    )

    horizon_keys = {key for entry in dataset for key in entry["forward_returns"]}
    assert horizon_keys == {str(h) for h in set(HORIZONS) | set(DECAY_HORIZONS)}
    assert service._compute_decay(dataset) == {
        str(h): service._compute_ic(dataset, list(DECAY_HORIZONS))
        .get(str(h), {})
        .get("ic_mean")
        for h in DECAY_HORIZONS
    }


def test_vectorised_path_performs_no_reads():
    rng = random.Random(11)
    rows = _panel(rng)
    values = _factor_values(rng, rows)
    service = _service(rows)
    start, end = datetime.datetime(2026, 1, 1), datetime.datetime(2026, 4, 30)

    service.evaluate(
        values, start, end, forward_horizons=HORIZONS, quote_frame=_frame_from(rows)
    )
    assert service.quote_model.reads == []

    service.evaluate(values, start, end, forward_horizons=HORIZONS)
    assert len(service.quote_model.reads) > len(values)


def test_calendar_window_guard_is_preserved():
    """``h=1`` only resolves when the next trading day is the next calendar day.

    Monday->Tuesday resolves; Friday->Monday does not (3 calendar days > int(1.5)).
    """
    monday = datetime.datetime(2026, 3, 2)
    assert monday.weekday() == 0
    friday = monday + datetime.timedelta(days=4)
    assert friday.weekday() == 4
    rows = []
    for offset, price in ((0, 10.0), (1, 11.0), (4, 12.0), (7, 13.0)):
        rows.append(
            {
                "code": "sh600000",
                "date": monday + datetime.timedelta(days=offset),
                "close": price,
                "close_hfq": price,
            }
        )
    values = {
        "sh600000": {
            monday.isoformat(): 1.0,
            friday.isoformat(): 1.0,
        }
    }
    service = _service(rows)
    start, end = datetime.datetime(2026, 1, 1), datetime.datetime(2026, 12, 31)

    legacy = service._build_dataset(values, start, end, [1])
    vectorised = service._build_dataset(
        values, start, end, [1], quote_frame=_frame_from(rows)
    )

    assert legacy == vectorised
    by_date = {entry["date"]: entry["forward_returns"]["1"] for entry in legacy}
    assert by_date[monday] == pytest.approx(0.1)  # Tue is within 1 calendar day
    assert by_date[friday] is None  # Fri -> Mon is 3 calendar days


def test_missing_and_non_positive_prices_match_legacy():
    day = datetime.datetime(2026, 3, 2)
    rows = [
        # base row with no HFQ value -> falls back to raw close
        {"code": "sh600001", "date": day, "close": 10.0, "close_hfq": None},
        {
            "code": "sh600001",
            "date": day + datetime.timedelta(days=1),
            "close": 11.0,
            "close_hfq": None,
        },
        # base row priced at zero -> observation dropped by both paths
        {"code": "sh600002", "date": day, "close": 0.0, "close_hfq": 0.0},
        {
            "code": "sh600002",
            "date": day + datetime.timedelta(days=1),
            "close": 5.0,
            "close_hfq": 5.0,
        },
        # forward price at zero -> None rather than a -100% return
        {"code": "sh600003", "date": day, "close": 10.0, "close_hfq": 10.0},
        {
            "code": "sh600003",
            "date": day + datetime.timedelta(days=1),
            "close": 0.0,
            "close_hfq": 0.0,
        },
        # no forward row at all -> None
        {"code": "sh600004", "date": day, "close": 10.0, "close_hfq": 10.0},
    ]
    values = {
        code: {day.isoformat(): 1.0}
        for code in ("sh600001", "sh600002", "sh600003", "sh600004")
    }
    service = _service(rows)
    start, end = datetime.datetime(2026, 1, 1), datetime.datetime(2026, 12, 31)

    legacy = service._build_dataset(values, start, end, [1], quote_frame=None)
    vectorised = service._build_dataset(
        values, start, end, [1], quote_frame=_frame_from(rows)
    )

    assert legacy == vectorised
    kept = {entry["stock_code"]: entry["forward_returns"]["1"] for entry in legacy}
    assert kept == {
        "sh600001": pytest.approx(0.1),
        "sh600003": None,
        "sh600004": None,
    }


def test_randomised_differential_over_many_panels():
    """Same dataset from both paths across 40 random panels and horizons."""
    for seed in range(40):
        rng = random.Random(seed)
        rows = _panel(rng, codes=rng.randint(1, 4), days=rng.randint(3, 50))
        values = _factor_values(rng, rows)
        service = _service(rows)
        start, end = datetime.datetime(2026, 1, 1), datetime.datetime(2026, 12, 31)
        horizons = sorted({1, 3, 5, 10, 20, 60, rng.randint(1, 60)})

        legacy = service._build_dataset(values, start, end, horizons)
        vectorised = service._build_dataset(
            values, start, end, horizons, quote_frame=_frame_from(rows)
        )
        assert legacy == vectorised, f"seed={seed} horizons={horizons}"
