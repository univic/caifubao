"""Tests for the read-only writer-switch verifier (TASK-404 slice 2)."""

import datetime
import json

import pytest

from app.jobs import writer_switch_verify as wsv


def _doc(collection_alias, code, day, **extra):
    keys = wsv.COLLECTION_SPECS[collection_alias]["keys"]
    values = {
        "code": code,
        "stock_code": code,
        "date": day,
        "horizon": 5,
        "model_version": "score_v2_202604",
        "signal_name": "breakout",
    }
    base = {k: values[k] for k in keys}
    base.update({"score": 3.25, "rank": 7, "close": 12.5, "fq": 1.5})
    base.update(extra)
    return base


class _FakeCollection:
    def __init__(self, docs):
        self.docs = docs

    @staticmethod
    def _matches(doc, flt):
        for k, v in flt.items():
            if isinstance(v, dict) and "$gte" in v:
                value = doc.get(k)
                day = value.date() if isinstance(value, datetime.datetime) else value
                if not (v["$gte"].date() <= day <= v["$lte"].date()):
                    return False
            elif doc.get(k) != v:
                return False
        return True

    def find_one(self, flt=None, sort=None, projection=None):
        flt = flt or {}
        matches = [d for d in self.docs if self._matches(d, flt)]
        if not matches:
            return None
        if sort:
            field, direction = sort[0]
            matches.sort(key=lambda d: d[field], reverse=direction < 0)
        return matches[0]

    def count_documents(self, flt):
        return sum(1 for d in self.docs if self._matches(d, flt))

    def find(self, flt, projection=None):
        return _LimitedList([d for d in self.docs if self._matches(d, flt)])


class _LimitedList(list):
    def limit(self, n):
        return self[:n]

    def sort(self, key_or_list):
        key_fn = key_or_list
        if isinstance(key_or_list, list):
            keys = [f for f, _ in key_or_list]
            key_fn = lambda d: tuple(d[f] for f in keys)  # noqa: E731
        return _LimitedList(sorted(self, key=key_fn))


class _FakeDb:
    def __init__(self, collections):
        self._collections = collections

    def __getitem__(self, name):
        return self._collections[name]


def _day(n=1):
    return datetime.datetime(2026, 9, 20, 0, 0, 0) + datetime.timedelta(days=n - 1)


def _make_pair(alias="quote", count=50, stable_count=None, mutate=None):
    day = _day()
    research_docs = [_doc(alias, f"sz{i:06d}", day) for i in range(count)]
    stable_docs = [_doc(alias, f"sz{i:06d}", day) for i in range(stable_count or count)]
    if mutate:
        mutate(stable_docs)
    return (
        _FakeDb(
            {wsv.COLLECTION_SPECS[alias]["collection"]: _FakeCollection(research_docs)}
        ),
        _FakeDb(
            {wsv.COLLECTION_SPECS[alias]["collection"]: _FakeCollection(stable_docs)}
        ),
    )


def test_resolve_collections_rejects_unknown_alias():
    with pytest.raises(ValueError, match="unknown collection alias"):
        wsv._resolve_collections("quote,nope")


def test_resolve_collections_defaults_to_all():
    assert wsv._resolve_collections(None) == list(wsv.COLLECTION_SPECS)


def test_relative_diff_handles_zero():
    assert wsv._relative_diff(0, 0) == 0.0
    assert wsv._relative_diff(0, 4) == 1.0
    assert wsv._relative_diff(99, 100) == pytest.approx(0.01)


def test_values_equal_float_tolerance_and_bool_strictness():
    assert wsv._values_equal(3.25, 3.25 + 1e-12)
    assert not wsv._values_equal(3.25, 3.5)
    # bool strictness: 1 == True in Python, but a coerced flag is a data
    # difference between independently produced environments
    assert not wsv._values_equal(1, True)
    assert wsv._values_equal(True, True)
    assert wsv._values_equal("a", "a")


def test_doc_diffs_reports_missing_and_value_mismatch():
    research = {"code": "sz000001", "date": _day(), "close": 12.5, "fq": 1.5}
    stable = {"code": "sz000001", "date": _day(), "close": 12.5, "extra": 1}
    diffs = wsv._doc_diffs(research, stable)
    assert "fq: missing on stable side" in diffs
    assert "extra: missing on research side" in diffs


def test_doc_diffs_ignores_bookkeeping_fields():
    research = {"code": "x", "updated_at": 1, "close": 12.5}
    stable = {"code": "x", "updated_at": 999, "close": 12.5}
    assert wsv._doc_diffs(research, stable) == []


def test_freshness_sort_picks_max_date():
    """The freshness probe must honor sort, not fixture order."""
    earlier = _day(0)
    later = _day(1)
    docs = [_doc("quote", "sz000001", later), _doc("quote", "sz000002", earlier)]
    db = _FakeDb({wsv.COLLECTION_SPECS["quote"]["collection"]: _FakeCollection(docs)})
    spec = wsv.COLLECTION_SPECS["quote"]
    assert wsv._latest_date(db, spec) == later.date()


def test_check_collection_passes_on_identical_environments():
    research_db, stable_db = _make_pair("quote", count=40)
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 20, 0.005
    )
    assert result["status"] == "PASS"
    assert result["checks"]["freshness"]["pass"] is True
    assert result["checks"]["count"]["relative_diff"] == 0.0
    assert result["checks"]["samples"]["pass"] is True
    assert result["checks"]["samples"]["compared"] == 20


def test_check_collection_fails_on_freshness_gap():
    research_db, stable_db = _make_pair("quote", count=40)
    stable_db = _FakeDb(
        {
            wsv.COLLECTION_SPECS["quote"]["collection"]: _FakeCollection(
                [_doc("quote", "sz000001", _day(0))]
            )
        }
    )
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 20, 0.005
    )
    assert result["status"] == "FAIL"
    assert result["checks"]["freshness"]["pass"] is False


def test_check_collection_fails_when_count_drift_above_tolerance():
    research_db, stable_db = _make_pair("quote", count=100, stable_count=90)
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 5, 0.005
    )
    assert result["checks"]["count"]["relative_diff"] == pytest.approx(0.1)
    assert result["status"] == "FAIL"


def test_insufficient_samples_fails_closed():
    """§5.2 gate: fewer candidates than requested samples must FAIL."""
    research_db, stable_db = _make_pair("quote", count=5)
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 20, 0.005
    )
    assert result["status"] == "FAIL"
    assert "insufficient samples" in result["checks"]["samples"]["error"]
    assert result["checks"]["samples"]["compared"] == 5


def test_reference_fields_skipped_across_environments():
    """stock DBRefs are minted per environment and must never false-FAIL."""

    research_db, stable_db = _make_pair("factor", count=40)
    stable_db = _FakeDb(
        {
            wsv.COLLECTION_SPECS["factor"]["collection"]: _FakeCollection(
                [
                    {
                        **d,
                        "stock": {
                            "$ref": "basic_stock",
                            "$id": f"env-specific-{i}",
                        },
                    }
                    for i, d in enumerate(
                        research_db[wsv.COLLECTION_SPECS["factor"]["collection"]].docs
                    )
                ]
            )
        }
    )
    result = wsv._check_collection(
        "factor", research_db, stable_db, _day().date(), 20, 0.005
    )
    assert result["status"] == "PASS", result


def test_check_collection_fails_on_field_mismatch():
    def bump_fq_everywhere(docs):
        for d in docs:
            d["fq"] = 9.99

    research_db, stable_db = _make_pair("quote", count=40, mutate=bump_fq_everywhere)
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 20, 0.005
    )
    assert result["status"] == "FAIL"
    assert result["checks"]["samples"]["failures"]


def test_run_verify_aggregates_statuses(monkeypatch):
    from app.model.stock import StockDailyQuote

    research_db, stable_db = _make_pair("quote", count=40)
    monkeypatch.setattr(wsv, "_stable_db", lambda client: stable_db, raising=False)
    monkeypatch.setattr(
        StockDailyQuote, "_get_collection", lambda: _FakeCollection([]), raising=False
    )

    summary = wsv.run_verify(
        aliases=["quote"],
        trade_date=_day().date(),
        samples=10,
        tolerance=0.005,
        research_db=research_db,
        stable_client=object(),
    )
    assert summary["status"] == "PASS"
    assert json.dumps(summary, default=str)


def test_build_stable_client_fails_closed_on_missing_credentials(monkeypatch):
    from app.conf import app_config as cfg

    # HOST/USERNAME/PASSWORD/NAME are import-time class attrs; USER/PASS env
    # keys only flow through them at config load, so patch the attrs directly
    for name in ("MONGODB_SRC_HOST", "MONGODB_SRC_USERNAME", "MONGODB_SRC_PASSWORD"):
        monkeypatch.setattr(cfg, name, "")
    monkeypatch.setattr(cfg, "MONGODB_SRC_NAME", "")
    with pytest.raises(RuntimeError, match="--env-from-secret") as excinfo:
        wsv._build_stable_client()
    for name in ("MONGODB_SRC_HOST", "MONGODB_SRC_NAME"):
        assert name in str(excinfo.value)


def test_parse_args_validates_samples_and_tolerance():
    with pytest.raises(SystemExit):
        wsv.parse_args(["--samples", "0"])
    with pytest.raises(SystemExit):
        wsv.parse_args(["--samples", "5", "--tolerance", "-1"])


def test_scoring_spec_uses_model_version_key():
    assert wsv.COLLECTION_SPECS["scoring"]["keys"] == [
        "stock_code",
        "date",
        "horizon",
        "model_version",
    ]
