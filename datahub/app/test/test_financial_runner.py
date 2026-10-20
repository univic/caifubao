# -*- coding: utf-8 -*-
"""Financial indicator ingestion: normalization, period math, idempotency."""

from __future__ import annotations

import datetime
from types import SimpleNamespace

import pandas as pd

from app.jobs import financial_runner


def test_normalize_row_maps_code_and_rejects_missing_dates():
    row = {
        "ts_code": "600519.SH",
        "ann_date": "20260815",
        "end_date": "20260630",
        "report_type": "1",
        "roe": 17.95,
        "grossprofit_margin": 89.55,
    }
    payload = financial_runner.normalize_row(row)
    assert payload["code"] == "sh600519"
    assert payload["ann_date"] == datetime.datetime(2026, 8, 15)
    assert payload["end_date"] == datetime.datetime(2026, 6, 30)
    assert payload["roe"] == 17.95

    assert financial_runner.normalize_row({**row, "ann_date": None}) is None
    assert financial_runner.normalize_row({**row, "ts_code": "bad"}) is None


def test_non_finite_report_type_falls_back_to_one():
    payload = financial_runner.normalize_row(
        {
            "ts_code": "600519.SH",
            "ann_date": "20260815",
            "end_date": "20260630",
            "report_type": float("nan"),
        }
    )
    assert payload["report_type"] == 1


def test_fetch_code_uses_injected_client():
    frame = pd.DataFrame(
        [
            {
                "ts_code": "600519.SH",
                "ann_date": "20260815",
                "end_date": "20260630",
                "report_type": 1,
                "roe": 17.95,
            }
        ]
    )
    pro = SimpleNamespace(fina_indicator=lambda **kwargs: frame)
    rows, rejected = financial_runner.fetch_code(
        pro, "sh600519", "20200101", "20260930"
    )
    assert rejected == 0
    assert len(rows) == 1
    assert rows[0]["code"] == "sh600519"


def test_fetch_code_counts_rows_without_announcement_dates():
    frame = pd.DataFrame(
        [
            {
                "ts_code": "600519.SH",
                "ann_date": "20260815",
                "end_date": "20260630",
                "report_type": 1,
            },
            {
                "ts_code": "600519.SH",
                "ann_date": None,
                "end_date": "20260930",
                "report_type": 1,
            },
        ]
    )
    pro = SimpleNamespace(fina_indicator=lambda **kwargs: frame)
    rows, rejected = financial_runner.fetch_code(
        pro, "sh600519", "20200101", "20260930"
    )
    assert len(rows) == 1
    assert rejected == 1


def test_persist_rows_is_idempotent(monkeypatch):
    store = {}

    class FakeDoc:
        def __init__(self, **kwargs):
            self.data = kwargs

        def save(self):
            key = (
                self.data["code"],
                self.data["end_date"],
                self.data["report_type"],
                self.data["ann_date"],
            )
            store[key] = self.data

    class Query:
        def __init__(self, collection, identity):
            self.collection = collection
            self.identity = identity

        def first(self):
            key = (
                self.identity["code"],
                self.identity["end_date"],
                self.identity["report_type"],
                self.identity["ann_date"],
            )
            if key not in self.collection.store:
                return None
            return SimpleNamespace(
                **self.collection.store[key],
                save=lambda: None,
            )

    class Collection:
        def __init__(self):
            self.store = store

        def __call__(self, **identity):
            return Query(self, identity)

    monkeypatch.setattr(
        "app.model.financial.StockFinancialIndicator", FakeDoc, raising=False
    )
    collection = Collection()
    row = {
        "code": "sh600519",
        "ann_date": datetime.datetime(2026, 8, 15),
        "end_date": datetime.datetime(2026, 6, 30),
        "report_type": 1,
        **{field: None for field in financial_runner.NUMERIC},
        "roe": 17.95,
    }
    first = financial_runner.persist_rows([row], collection=collection)
    cleared = {**row, "roe": None}
    second = financial_runner.persist_rows([cleared], collection=collection)
    assert first["created"] == 1
    assert second["updated"] == 1
    assert len(store) == 1
    # A partial re-fetch must not blank the stored vintage.
    assert next(iter(store.values()))["roe"] == 17.95


def test_run_reports_rejected_rows(monkeypatch):
    frame = pd.DataFrame(
        [
            {
                "ts_code": "600519.SH",
                "ann_date": None,
                "end_date": "20260930",
                "report_type": 1,
            }
        ]
    )
    pro = SimpleNamespace(fina_indicator=lambda **kwargs: frame)
    monkeypatch.setattr(
        financial_runner,
        "persist_rows",
        lambda rows: {"created": len(rows), "updated": 0},
    )
    summary = financial_runner.run(["sh600519"], pro=pro, pause=0)
    assert summary["rejected_rows"] == 1
    assert summary["rows"] == 0


def test_persist_failure_is_per_code_not_whole_backfill(monkeypatch):
    frame = pd.DataFrame(
        [
            {
                "ts_code": "600519.SH",
                "ann_date": "20260815",
                "end_date": "20260630",
                "report_type": 1,
                "roe": 17.95,
            }
        ]
    )
    pro = SimpleNamespace(fina_indicator=lambda **kwargs: frame)

    def boom(rows):
        raise RuntimeError("mongo write failed")

    monkeypatch.setattr(financial_runner, "persist_rows", boom)
    summary = financial_runner.run(["sh600519", "sz000001"], pro=pro, pause=0)
    assert len(summary["failed_codes"]) == 2
    assert summary["rows"] == 0
