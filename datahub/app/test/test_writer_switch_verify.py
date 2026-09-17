"""Tests for the read-only writer-switch verifier (TASK-404 slice 2)."""

import datetime

import pandas as pd
import pytest

from app.jobs import writer_switch_verify as wsv


def _field_value(field, code, day):
    if field == "date":
        return day
    if field in ("code", "stock_code"):
        return code
    if field == "stock_name":
        return f"name-{code}"
    if field == "category":
        return "stock"
    if field == "signal_name":
        return "breakout"
    if field == "signal_version":
        return "v1"
    if field == "direction":
        return "bullish"
    if field == "signal_type":
        return "momentum"
    if field == "reason":
        return "reason"
    if field == "horizon":
        return 5
    if field == "model_version":
        return "score_v2_202604"
    if field == "recommendation":
        return "HOLD"
    if field == "status":
        return "PENDING"
    if field == "target_date":
        return day + datetime.timedelta(days=5)
    if field in (
        "price_snapshot",
        "factor_snapshot",
        "source_freshness",
        "explanation",
        "verification",
        "input_snapshot",
    ):
        return {"v": 1}
    if field == "rank":
        return 7
    if field == "percentile":
        return 0.5
    if field == "isST":
        return 0
    if field == "volume":
        return 1000
    return 1.5


def _doc(alias, code, day, row_class=None, **extra):
    """Build a document carrying every field the declared scope classifies."""
    spec = wsv.COLLECTION_SPECS[alias]
    row_class = row_class or spec["default_row_class"]
    scope = spec["row_classes"][row_class]
    doc = {}
    for field in (
        scope["required_fields"]
        + scope["derived_fields"]
        + scope["research_populated_fields"]
    ):
        doc[field] = _field_value(field, code, day)
    doc[spec["code_field"]] = code
    doc["date"] = day
    doc["_id"] = f"id-{alias}-{code}"
    doc["_cls"] = "IndividualStock" if row_class == "individual_stock" else "StockIndex"
    doc["stock"] = {"$ref": "basic_stock", "$id": code}
    doc["updated_at"] = day
    doc.update(extra)
    return doc


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


def _db(alias, docs, basic_types, extra=None):
    spec = wsv.COLLECTION_SPECS[alias]
    collections = {
        spec["collection"]: _FakeCollection(docs),
        "basic_stock": _FakeCollection(
            [
                {"code": code, "object_type": object_type}
                for code, object_type in basic_types.items()
            ]
        ),
    }
    if extra:
        collections.update(extra)
    return _FakeDb(collections)


@pytest.fixture(autouse=True)
def _relax_index_floor(monkeypatch):
    """Small fixtures cannot carry the measured 562-row index universe."""
    monkeypatch.setitem(
        wsv.COLLECTION_SPECS["quote"]["count_classes"]["stock_index"],
        "min_research_rows",
        0,
    )


def _make_pair(
    alias="quote",
    count=50,
    stable_count=None,
    mutate=None,
    index_count=None,
    stable_index_count=None,
    in_progress=False,
    day=None,
):
    day = day or _day()
    spec = wsv.COLLECTION_SPECS[alias]
    row_class = spec["default_row_class"]
    stable_count = count if stable_count is None else stable_count
    # A declared count class with zero rows on either side must FAIL, so a
    # quote pair carries an index class by default (mirroring the real
    # 562-index universe); tests that exercise a missing class override it.
    if index_count is None:
        index_count = 5 if alias == "quote" else 0
    stable_index_count = (
        index_count if stable_index_count is None else stable_index_count
    )
    research_stock = [
        _doc(alias, f"sz{i:06d}", day, row_class=row_class) for i in range(count)
    ]
    stable_stock = [
        _doc(alias, f"sz{i:06d}", day, row_class=row_class) for i in range(stable_count)
    ]
    research_index = [
        _doc(alias, f"sh{i:06d}", day, row_class="stock_index")
        for i in range(index_count)
    ]
    stable_index = [
        _doc(alias, f"sh{i:06d}", day, row_class="stock_index")
        for i in range(stable_index_count)
    ]
    research_basic = {f"sz{i:06d}": row_class for i in range(count)}
    stable_basic = {f"sz{i:06d}": row_class for i in range(stable_count)}
    research_basic.update({f"sh{i:06d}": "stock_index" for i in range(index_count)})
    stable_basic.update(
        {f"sh{i:06d}": "stock_index" for i in range(stable_index_count)}
    )
    if in_progress:
        later = day + datetime.timedelta(days=1)
        research_stock += [
            _doc(alias, f"sz{i:06d}", later, row_class=row_class)
            for i in range(min(count, 3))
        ]
        stable_stock += [
            _doc(alias, f"sz{i:06d}", later, row_class=row_class)
            for i in range(min(stable_count, 2))
        ]
    # Mutations model a value difference in the declared row class only, so an
    # index document never gains a stock-only field.
    if mutate:
        mutate(stable_stock)
    return (
        _db(alias, research_stock + research_index, research_basic),
        _db(alias, stable_stock + stable_index, stable_basic),
    )


class _FakeTushare:
    def __init__(self, frame=None, error=None):
        self.frame = frame
        self.error = error
        self.calls = []

    def to_tushare_ts_code(self, code):
        return code[2:] + ".SZ"

    def adj_factor(self, ts_code, start_date=None, end_date=None):
        self.calls.append((ts_code, start_date, end_date))
        if self.error is not None:
            raise self.error
        return self.frame


def _adj_frame(pairs):
    return pd.DataFrame(
        {
            "trade_date": [day for day, _ in pairs],
            "adj_factor": [value for _, value in pairs],
        }
    )


def _source_rows():
    """Three research quote rows where the source omits the last two days."""
    rows = []
    for offset in range(3):
        doc = _doc("quote", "sz000001", _day() + datetime.timedelta(days=offset))
        close, factor = 12.5, 1.5
        doc["close"] = close
        doc["open"], doc["high"], doc["low"] = 12.0, 13.0, 11.5
        doc["fq_factor"] = factor
        doc["close_hfq"] = round(close * factor, 4)
        doc["open_hfq"] = round(doc["open"] * factor, 4)
        doc["high_hfq"] = round(doc["high"] * factor, 4)
        doc["low_hfq"] = round(doc["low"] * factor, 4)
        rows.append(doc)
    return rows


def _source_db(rows):
    return _db("quote", rows, {"sz000001": "individual_stock"})


def _source_db_with_index():
    latest = _day() + datetime.timedelta(days=2)
    index_doc = _doc("quote", "sh000001", latest, row_class="stock_index")
    basics = {"sz000001": "individual_stock", "sh000001": "stock_index"}
    return _db("quote", _source_rows() + [index_doc], basics)


def _source_db_with_index_and_bse():
    latest = _day() + datetime.timedelta(days=2)
    index_doc = _doc("quote", "sh000001", latest, row_class="stock_index")
    bj_doc = _doc("quote", "bj830799", latest)
    basics = {
        "sz000001": "individual_stock",
        "sh000001": "stock_index",
        "bj830799": "individual_stock",
    }
    return _db("quote", _source_rows() + [index_doc, bj_doc], basics)


class _FakeJobRunContext:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


class _FakeJobRunHelper:
    JobRunContext = _FakeJobRunContext

    def __init__(self):
        self.created = 0
        self.finished = []
        self.cleaned = 0

    def mark_stale_running_job_runs_failed(self, job_family=None):
        self.cleaned += 1

    def utc_now_naive(self):
        return datetime.datetime(2026, 9, 17, 0, 0, 0)

    def create_job_run(self, context):
        self.created += 1
        return {"context": context}

    def finish_job_run(self, job_run, status=None, summary=None):
        self.finished.append({"status": status, "summary": summary})


def _stock_docs(db, alias):
    collection = wsv.COLLECTION_SPECS[alias]["collection"]
    return [d for d in db[collection].docs if d["_cls"] == "IndividualStock"]


def test_resolve_collections_rejects_unknown_alias():
    with pytest.raises(ValueError, match="unknown collection alias"):
        wsv._resolve_collections("quote,nope")


def test_resolve_collections_defaults_to_all():
    assert wsv._resolve_collections(None) == list(wsv.COLLECTION_SPECS)


@pytest.mark.parametrize("raw", ["", " ", ",", " , ", " , , "])
def test_resolve_collections_rejects_blank_tokens(raw):
    with pytest.raises(ValueError, match="no collections selected"):
        wsv._resolve_collections(raw)


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


def test_scope_classifies_every_declared_field_exactly_once():
    for alias, spec in wsv.COLLECTION_SPECS.items():
        assert spec["default_row_class"] in spec["row_classes"], alias
        for row_class, scope in spec["row_classes"].items():
            mapping = wsv._field_class_map(scope)
            assert mapping, (alias, row_class)
            for field, classes in mapping.items():
                assert len(classes) == 1, (alias, row_class, field, classes)


def test_validate_declared_scope_rejects_conflicting_classification(monkeypatch):
    scope = wsv.COLLECTION_SPECS["quote"]["row_classes"]["individual_stock"]
    monkeypatch.setitem(scope, "derived_fields", scope["derived_fields"] + ("close",))
    with pytest.raises(RuntimeError, match="declared scope conflict"):
        wsv._validate_declared_scope()


def test_validate_declared_scope_rejects_missing_provenance(monkeypatch):
    monkeypatch.delitem(wsv.RESEARCH_POPULATED_PROVENANCE, "isST")
    with pytest.raises(RuntimeError, match="without provenance"):
        wsv._validate_declared_scope()


def test_scope_version_is_stable_and_changes_with_the_declaration(monkeypatch):
    monkeypatch.setitem(
        wsv.COLLECTION_SPECS["quote"]["count_classes"]["stock_index"],
        "min_research_rows",
        wsv.DEFAULT_MIN_RESEARCH_INDEX_ROWS,
    )
    assert len(wsv.SCOPE_VERSION) == 64
    assert wsv.SCOPE_VERSION == wsv._compute_scope_version()
    original = wsv.COLLECTION_SPECS["quote"]["row_classes"]["stock_index"][
        "required_fields"
    ]
    monkeypatch.setitem(
        wsv.COLLECTION_SPECS["quote"]["row_classes"]["stock_index"],
        "required_fields",
        original + ("mystery_field",),
    )
    assert wsv._compute_scope_version() != wsv.SCOPE_VERSION


def test_scope_payload_covers_keys_and_deterministic_samples(monkeypatch):
    monkeypatch.setitem(
        wsv.COLLECTION_SPECS["quote"]["count_classes"]["stock_index"],
        "min_research_rows",
        wsv.DEFAULT_MIN_RESEARCH_INDEX_ROWS,
    )
    payload = wsv._scope_payload()
    assert payload["quote"]["keys"] == ["code", "date"]
    assert payload["quote"]["deterministic_samples"] is False
    assert payload["scoring"]["deterministic_samples"] is True
    assert (
        payload["quote"]["count_classes"]["stock_index"]["min_research_rows"]
        == wsv.DEFAULT_MIN_RESEARCH_INDEX_ROWS
    )


def test_doc_diffs_reports_compared_field_mismatch_and_missing():
    scope = wsv.COLLECTION_SPECS["quote"]["row_classes"]["individual_stock"]
    research = _doc("quote", "sz000001", _day())
    stable = dict(research)
    stable["close"] = 99.0
    stable.pop("high")
    comparison = wsv._compare_sample(research, stable, scope)
    assert any(d.startswith("close:") for d in comparison["diffs"])
    assert "high: missing on stable side" in comparison["diffs"]


def test_doc_diffs_ignores_excluded_bookkeeping_fields():
    scope = wsv.COLLECTION_SPECS["quote"]["row_classes"]["individual_stock"]
    research = _doc("quote", "sz000001", _day())
    stable = dict(research)
    stable["updated_at"] = _day(5)
    stable["_id"] = "different-object-id"
    stable["stock"] = {"$ref": "basic_stock", "$id": "other-env"}
    comparison = wsv._compare_sample(research, stable, scope)
    assert comparison["diffs"] == []
    assert comparison["undeclared"] == []


def test_freshness_sort_picks_max_date():
    """The freshness probe must honor sort, not fixture order."""
    earlier = _day(0)
    later = _day(1)
    docs = [_doc("quote", "sz000001", later), _doc("quote", "sz000002", earlier)]
    db = _db("quote", docs, {"sz000001": "individual_stock"})
    spec = wsv.COLLECTION_SPECS["quote"]
    assert wsv._latest_date(db, spec) == later.date()


def test_latest_completed_session_uses_calendar_and_close_cutoff():
    calendar = [
        datetime.datetime(2026, 9, 15),
        datetime.datetime(2026, 9, 16),
        datetime.datetime(2026, 9, 17),
    ]
    db = _FakeDb(
        {
            "finance_market": _FakeCollection(
                [{"name": "ChinaAStock", "trade_calendar": calendar}]
            )
        }
    )
    before_close = datetime.datetime(2026, 9, 17, 14, 30, tzinfo=wsv._CN_TZ)
    after_close = datetime.datetime(2026, 9, 17, 15, 30, tzinfo=wsv._CN_TZ)
    assert wsv._latest_completed_session(db, now=before_close) == datetime.date(
        2026, 9, 16
    )
    assert wsv._latest_completed_session(db, now=after_close) == datetime.date(
        2026, 9, 17
    )
    assert wsv._latest_completed_session(_FakeDb({}), now=after_close) is None


def test_check_collection_passes_on_identical_environments():
    research_db, stable_db = _make_pair("quote", count=40)
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 20, 0.005
    )
    assert result["status"] == "PASS", result
    assert result["checks"]["freshness"]["pass"] is True
    assert result["checks"]["count"]["classes"]["individual_stock"]["pass"] is True
    assert result["checks"]["count"]["total"]["used_for_verdict"] is False
    assert result["checks"]["samples"]["pass"] is True
    assert result["checks"]["samples"]["compared"] == 20


def test_check_collection_fails_on_freshness_gap():
    research_db, stable_db = _make_pair("quote", count=40)
    for doc in stable_db[wsv.COLLECTION_SPECS["quote"]["collection"]].docs:
        doc["date"] = _day(0)
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 20, 0.005
    )
    assert result["status"] == "FAIL"
    assert result["checks"]["freshness"]["pass"] is False
    assert result["checks"]["freshness"]["stable_in_progress"] is False


def test_freshness_flags_in_progress_session_and_uses_the_reference():
    research_db, stable_db = _make_pair("quote", count=40, in_progress=True)
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 10, 0.005
    )
    freshness = result["checks"]["freshness"]
    assert freshness["reference_date"] == str(_day().date())
    assert freshness["research_latest"] == str(
        (_day() + datetime.timedelta(days=1)).date()
    )
    assert freshness["research_in_progress"] is True
    assert freshness["stable_in_progress"] is True
    assert freshness["pass"] is True
    assert result["checks"]["count"]["trade_date"] == str(_day().date())


def test_mutually_stale_default_reference_fails(monkeypatch):
    stale_day = _day() - datetime.timedelta(days=7)
    research_db, stable_db = _make_pair("quote", count=10, day=stale_day)
    monkeypatch.setattr(
        wsv, "_latest_completed_session", lambda db, now=None: _day().date()
    )
    result = wsv._check_collection("quote", research_db, stable_db, None, 5, 0.005)
    assert result["checks"]["freshness"]["reference_source"] == "trade_calendar"
    assert result["checks"]["freshness"]["pass"] is False
    assert result["status"] == "FAIL"


def test_explicit_trade_date_pins_a_historical_acceptance(monkeypatch):
    pinned = _day() - datetime.timedelta(days=7)
    research_db, stable_db = _make_pair("quote", count=10, day=pinned)
    monkeypatch.setattr(
        wsv, "_latest_completed_session", lambda db, now=None: _day().date()
    )
    result = wsv._check_collection(
        "quote", research_db, stable_db, pinned.date(), 5, 0.005
    )
    assert result["checks"]["freshness"]["reference_source"] == "requested"
    assert result["checks"]["freshness"]["pass"] is True


def test_check_collection_fails_when_count_drift_above_tolerance():
    research_db, stable_db = _make_pair("quote", count=100, stable_count=90)
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 5, 0.005
    )
    assert result["checks"]["count"]["classes"]["individual_stock"][
        "relative_diff"
    ] == pytest.approx(0.1)
    assert result["status"] == "FAIL"


def test_count_class_zero_rows_fails_even_when_the_total_looks_fine():
    research_db, stable_db = _make_pair(
        "quote", count=10, index_count=5, stable_index_count=0
    )
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 5, 0.005
    )
    index_class = result["checks"]["count"]["classes"]["stock_index"]
    assert index_class["research"] == 5
    assert index_class["stable"] == 0
    assert index_class["pass"] is False
    assert result["checks"]["count"]["pass"] is False
    assert result["status"] == "FAIL"


def test_count_class_coverage_superset_passes():
    research_db, stable_db = _make_pair(
        "quote", count=10, index_count=10, stable_index_count=5
    )
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 5, 0.005
    )
    index_class = result["checks"]["count"]["classes"]["stock_index"]
    assert index_class["mode"] == "coverage_superset"
    assert index_class["pass"] is True
    assert result["checks"]["count"]["pass"] is True
    assert result["status"] == "PASS", result


def test_index_coverage_collapse_below_the_floor_fails(monkeypatch):
    monkeypatch.setitem(
        wsv.COLLECTION_SPECS["quote"]["count_classes"]["stock_index"],
        "min_research_rows",
        wsv.DEFAULT_MIN_RESEARCH_INDEX_ROWS,
    )
    research_db, stable_db = _make_pair(
        "quote", count=5, index_count=300, stable_index_count=222
    )
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 5, 0.005
    )
    index_class = result["checks"]["count"]["classes"]["stock_index"]
    assert index_class["research"] == 300
    assert index_class["stable"] == 222
    assert index_class["pass"] is False
    assert "min_research_rows" in index_class["error"]
    assert result["status"] == "FAIL"


def test_index_stable_code_missing_on_research_fails():
    day = _day()
    research_docs = [_doc("quote", f"sz{i:06d}", day) for i in range(5)]
    research_docs += [
        _doc("quote", f"sh{i:06d}", day, row_class="stock_index") for i in range(5)
    ]
    stable_docs = [_doc("quote", f"sz{i:06d}", day) for i in range(5)]
    stable_docs += [
        _doc("quote", f"sh{i:06d}", day, row_class="stock_index") for i in range(4)
    ]
    stable_docs += [_doc("quote", "sh009999", day, row_class="stock_index")]
    research_basic = {f"sz{i:06d}": "individual_stock" for i in range(5)}
    research_basic.update({f"sh{i:06d}": "stock_index" for i in range(5)})
    stable_basic = {f"sz{i:06d}": "individual_stock" for i in range(5)}
    stable_basic.update({f"sh{i:06d}": "stock_index" for i in range(4)})
    stable_basic["sh009999"] = "stock_index"
    research_db = _db("quote", research_docs, research_basic)
    stable_db = _db("quote", stable_docs, stable_basic)
    result = wsv._check_collection(
        "quote", research_db, stable_db, day.date(), 5, 0.005
    )
    index_class = result["checks"]["count"]["classes"]["stock_index"]
    assert index_class["research"] == 5
    assert index_class["stable"] == 5
    assert index_class["missing_stable_code_count"] == 1
    assert index_class["missing_stable_codes"] == ["sh009999"]
    assert index_class["pass"] is False
    assert result["status"] == "FAIL"


def test_unsupported_universe_is_a_declared_class_and_fails_when_research_leaks():
    research_db, stable_db = _make_pair("quote", count=5)
    collection = wsv.COLLECTION_SPECS["quote"]["collection"]
    research_db[collection].docs.append(_doc("quote", "bj830799", _day()))
    research_db["basic_stock"].docs.append(
        {"code": "bj830799", "object_type": "individual_stock"}
    )
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 5, 0.005
    )
    entry = result["checks"]["count"]["classes"]["unsupported_universe"]
    assert entry["mode"] == "research_excludes"
    assert entry["research"] == 1
    assert entry["pass"] is False
    assert result["status"] == "FAIL"


def test_unsupported_universe_on_stable_only_passes():
    research_db, stable_db = _make_pair("quote", count=5)
    collection = wsv.COLLECTION_SPECS["quote"]["collection"]
    stable_db[collection].docs.append(_doc("quote", "bj830799", _day()))
    stable_db["basic_stock"].docs.append(
        {"code": "bj830799", "object_type": "individual_stock"}
    )
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 5, 0.005
    )
    entry = result["checks"]["count"]["classes"]["unsupported_universe"]
    assert entry["research"] == 0
    assert entry["stable"] == 1
    assert entry["pass"] is True
    assert result["status"] == "PASS", result


def test_instrument_type_falls_back_to_cls_discriminator():
    research_db, stable_db = _make_pair("quote", count=5, index_count=5)
    for doc in research_db["basic_stock"].docs:
        if doc["object_type"] == "stock_index":
            doc.pop("object_type")
            doc["_cls"] = "StockIndex"
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 5, 0.005
    )
    assert result["checks"]["count"]["classes"]["stock_index"]["research"] == 5
    assert result["checks"]["count"]["pass"] is True


def test_count_fails_when_a_supported_row_is_unclassified():
    research_db, stable_db = _make_pair("quote", count=10, index_count=0)
    research_db["basic_stock"].docs = []
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 5, 0.005
    )
    assert result["checks"]["count"]["unclassified"]["research"] == 10
    assert result["checks"]["count"]["pass"] is False
    assert result["status"] == "FAIL"


def test_insufficient_samples_fails_closed():
    """§5.2 gate: fewer candidates than requested samples must FAIL."""
    research_db, stable_db = _make_pair("quote", count=5, index_count=0)
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 20, 0.005
    )
    assert result["status"] == "FAIL"
    assert "insufficient samples" in result["checks"]["samples"]["error"]
    assert result["checks"]["samples"]["compared"] == 5


def test_sample_pool_is_not_capped_and_covers_the_day():
    research_db, stable_db = _make_pair("quote", count=1200, index_count=0)
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 20, 0.005
    )
    samples = result["checks"]["samples"]
    assert samples["pool_size"] == 1200
    assert samples["day_rows"] == 1200
    assert samples["pool_complete"] is True
    assert samples["compared"] == 20


def test_reference_fields_skipped_across_environments():
    """stock DBRefs are minted per environment and must never false-FAIL."""

    def remint(docs):
        for i, doc in enumerate(docs):
            doc["stock"] = {"$ref": "basic_stock", "$id": f"env-specific-{i}"}

    research_db, stable_db = _make_pair("factor", count=40, mutate=remint)
    result = wsv._check_collection(
        "factor", research_db, stable_db, _day().date(), 20, 0.005
    )
    assert result["status"] == "PASS", result


def test_required_field_mismatch_fails():
    def bump_close(docs):
        for doc in docs:
            doc["close"] = 99.0

    research_db, stable_db = _make_pair("quote", count=40, mutate=bump_close)
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 20, 0.005
    )
    assert result["status"] == "FAIL"
    assert result["checks"]["samples"]["failures"]


def test_required_field_absent_on_both_sides_fails():
    research_db, stable_db = _make_pair("quote", count=20)
    for db in (research_db, stable_db):
        for doc in _stock_docs(db, "quote"):
            doc.pop("previous_close")
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 10, 0.005
    )
    assert result["status"] == "FAIL"
    assert any(
        "required field absent on both sides" in diff
        for failure in result["checks"]["samples"]["failures"]
        for diff in failure["diffs"]
    )


def test_derived_fields_excluded_by_default_with_reason_and_scope_version():
    def bump_derived(docs):
        for doc in docs:
            doc["fq_factor"] = 2.5
            doc["close_hfq"] = 999.0

    research_db, stable_db = _make_pair("quote", count=20, mutate=bump_derived)
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 10, 0.005
    )
    assert result["status"] == "PASS", result
    assert set(result["scope"]["derived_fields"]) >= {
        "fq_factor",
        "close_hfq",
        "open_hfq",
        "high_hfq",
        "low_hfq",
    }
    assert result["scope"]["version"] == wsv.SCOPE_VERSION
    assert "pre-fq-adj-factor-fix" in result["scope"]["reason"]
    assert "pre-fq-adj-factor-fix" in result["scope"]["derived_reason"]


def test_compare_derived_fails_on_mismatch():
    def bump_derived(docs):
        for doc in docs:
            doc["fq_factor"] = 2.5
            doc["close_hfq"] = 999.0

    research_db, stable_db = _make_pair("quote", count=20, mutate=bump_derived)
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 10, 0.005, compare_derived=True
    )
    assert result["status"] == "FAIL"
    assert result["checks"]["samples"]["compare_derived"] is True
    assert any(
        "fq_factor" in diff
        for failure in result["checks"]["samples"]["failures"]
        for diff in failure["diffs"]
    )


def test_factor_ma_fields_are_declared_derived():
    """MA factors are computed from close_hfq, so parity excludes them."""
    scope = wsv.COLLECTION_SPECS["factor"]["row_classes"]["individual_stock"]
    assert set(scope["derived_fields"]) == {
        "ma_10",
        "ma_20",
        "ma_30",
        "ma_60",
        "ma_120",
    }

    def bump_ma(docs):
        for doc in docs:
            doc["ma_10"] = 999.0

    research_db, stable_db = _make_pair("factor", count=20, mutate=bump_ma)
    result = wsv._check_collection(
        "factor", research_db, stable_db, _day().date(), 10, 0.005
    )
    assert result["status"] == "PASS", result


def test_research_populated_absence_on_stable_is_informational():
    def drop_new_fields(docs):
        for doc in docs:
            doc.pop("isST", None)
            doc.pop("peTTM", None)

    research_db, stable_db = _make_pair("quote", count=20, mutate=drop_new_fields)
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 10, 0.005
    )
    assert result["status"] == "PASS", result
    notes = " ".join(result["checks"]["samples"]["informational"])
    assert "isST" in notes
    assert "peTTM" in notes
    provenance = result["scope"]["research_populated_fields"]["isST"]
    assert provenance["writer"]
    assert provenance["source"]
    assert provenance["revision"]
    assert provenance["revision_source"]


def test_research_populated_presence_is_required_on_research():
    research_db, stable_db = _make_pair("quote", count=20)
    for doc in _stock_docs(research_db, "quote"):
        doc["isST"] = None
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 10, 0.005
    )
    assert result["status"] == "FAIL"
    failures = result["checks"]["samples"]["failures"]
    assert any(
        any("isST" in failure for failure in entry["presence_failures"])
        for entry in failures
    )


def test_research_populated_differing_value_on_both_sides_fails():
    def bump_pe(docs):
        for doc in docs:
            doc["peTTM"] = 42.0

    research_db, stable_db = _make_pair("quote", count=20, mutate=bump_pe)
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 10, 0.005
    )
    assert result["status"] == "FAIL"
    assert any(
        "non-null on both sides" in diff
        for failure in result["checks"]["samples"]["failures"]
        for diff in failure["diffs"]
    )


def test_undeclared_field_fails_on_each_side():
    research_db, stable_db = _make_pair("quote", count=20)
    collection = wsv.COLLECTION_SPECS["quote"]["collection"]
    for doc in research_db[collection].docs:
        doc["research_extra"] = 1
    for doc in stable_db[collection].docs:
        doc["stable_extra"] = 2
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 10, 0.005
    )
    assert result["status"] == "FAIL"
    assert set(result["checks"]["samples"]["undeclared_fields"]) == {
        "research_extra",
        "stable_extra",
    }
    assert "undeclared fields observed" in result["checks"]["samples"]["error"]


def test_undeclared_field_outside_the_sample_basket_still_fails():
    """Full-day key discovery must catch an undeclared key anywhere that day."""
    research_db, stable_db = _make_pair("quote", count=20)
    # Only one (likely unsampled) stock document carries the unknown key.
    _stock_docs(research_db, "quote")[-1]["unsampled_extra"] = 1
    result = wsv._check_collection(
        "quote", research_db, stable_db, _day().date(), 2, 0.005
    )
    assert result["status"] == "FAIL"
    assert "unsampled_extra" in result["checks"]["samples"]["undeclared_fields"]
    assert (
        "unsampled_extra"
        in result["checks"]["samples"]["day_discovery"]["individual_stock"]
    )


def test_scoring_uses_deterministic_stride_basket():
    research_db, stable_db = _make_pair("scoring", count=100)
    result = wsv._check_collection(
        "scoring", research_db, stable_db, _day().date(), 20, 0.005
    )
    assert result["status"] == "PASS", result
    stock_codes = [
        key["stock_code"] for key in result["checks"]["samples"]["sampled_keys"]
    ]
    assert stock_codes == [f"sz{i:06d}" for i in range(0, 100, 5)]


def test_source_check_passes_on_matching_factors_with_carry_forward():
    research_db = _source_db(_source_rows())
    # Source only provides the first day; the writer carries it forward.
    fake = _FakeTushare(_adj_frame([("20260920", 1.5)]))
    result = wsv._source_check(research_db, None, 1, tushare=fake)
    assert result["pass"] is True, result
    assert result["checked_codes"] == 1
    assert result["tolerances"]["close_hfq"] == 5e-4
    assert fake.calls and fake.calls[0][0] == "000001.SZ"


def test_source_check_fails_on_tushare_error():
    research_db = _source_db(_source_rows())
    fake = _FakeTushare(error=RuntimeError("network down"))
    result = wsv._source_check(research_db, None, 1, tushare=fake)
    assert result["pass"] is False
    assert "tushare adj_factor failed" in result["failures"][0]["error"]


def test_source_check_fails_on_empty_or_unusable_factor():
    research_db = _source_db(_source_rows())
    empty = wsv._source_check(
        research_db, None, 1, tushare=_FakeTushare(pd.DataFrame())
    )
    assert empty["pass"] is False
    assert "returned no rows" in empty["failures"][0]["error"]

    invalid = wsv._source_check(
        research_db, None, 1, tushare=_FakeTushare(_adj_frame([("20260920", 0.0)]))
    )
    assert invalid["pass"] is False
    assert "no usable factor values" in invalid["failures"][0]["error"]


def test_source_check_skips_invalid_source_rows_like_the_writer():
    research_db = _source_db(_source_rows())
    frame = _adj_frame([("20260920", 1.5), ("20260921", float("nan"))])
    result = wsv._source_check(research_db, None, 1, tushare=_FakeTushare(frame))
    assert result["pass"] is True, result
    assert result["codes"][0]["invalid_source_rows_skipped"] == 1


def test_source_check_fails_on_factor_mismatch():
    research_db = _source_db(_source_rows())
    fake = _FakeTushare(_adj_frame([("20260920", 1.4)]))
    result = wsv._source_check(research_db, None, 1, tushare=fake)
    assert result["pass"] is False
    assert any("fq_factor" in failure for failure in result["failures"][0]["failures"])


def test_source_check_fails_on_close_hfq_mismatch():
    rows = _source_rows()
    rows[0]["close_hfq"] = 999.0
    research_db = _source_db(rows)
    fake = _FakeTushare(_adj_frame([("20260920", 1.5)]))
    result = wsv._source_check(research_db, _day().date(), 1, tushare=fake)
    assert result["pass"] is False
    assert any("close_hfq" in failure for failure in result["failures"][0]["failures"])


def test_source_check_fails_on_ohlc_hfq_mismatch():
    rows = _source_rows()
    rows[0]["open_hfq"] = 999.0
    research_db = _source_db(rows)
    fake = _FakeTushare(_adj_frame([("20260920", 1.5)]))
    result = wsv._source_check(research_db, _day().date(), 1, tushare=fake)
    assert result["pass"] is False
    assert any("open_hfq" in failure for failure in result["failures"][0]["failures"])


def test_source_check_rejects_non_numeric_and_numeric_string_values():
    for bad in ("not-a-number", "1.5", True):
        rows = _source_rows()
        rows[0]["fq_factor"] = bad
        research_db = _source_db(rows)
        fake = _FakeTushare(_adj_frame([("20260920", 1.5)]))
        result = wsv._source_check(research_db, _day().date(), 1, tushare=fake)
        assert result["pass"] is False, bad
        assert "non-numeric" in result["failures"][0]["failures"][0], bad


def test_source_check_excludes_index_and_unsupported_codes():
    research_db = _source_db_with_index_and_bse()
    fake = _FakeTushare(_adj_frame([("20260920", 1.5)]))
    result = wsv._source_check(research_db, None, 1, tushare=fake)
    assert result["pass"] is True, result
    assert result["available_supported_codes"] == 1
    assert [entry["code"] for entry in result["codes"]] == ["sz000001"]


def test_source_check_fails_when_no_supported_stock_codes():
    index_doc = _doc("quote", "sh000001", _day(), row_class="stock_index")
    research_db = _db("quote", [index_doc], {"sh000001": "stock_index"})
    fake = _FakeTushare(_adj_frame([("20260920", 1.5)]))
    result = wsv._source_check(research_db, None, 1, tushare=fake)
    assert result["pass"] is False
    assert "insufficient supported individual-stock codes" in result["error"]


def test_source_check_fails_when_fewer_codes_than_samples():
    research_db = _source_db(_source_rows())
    fake = _FakeTushare(_adj_frame([("20260920", 1.5)]))
    result = wsv._source_check(research_db, None, 5, tushare=fake)
    assert result["pass"] is False
    assert "insufficient supported individual-stock codes" in result["error"]


def test_run_verify_source_check_on_by_default(monkeypatch):
    research_db = _source_db_with_index()
    monkeypatch.setattr(wsv, "_stable_db", lambda client: research_db, raising=False)
    fake = _FakeTushare(_adj_frame([("20260920", 1.5)]))
    summary = wsv.run_verify(
        aliases=["quote"],
        trade_date=None,
        samples=1,
        tolerance=0.005,
        research_db=research_db,
        stable_client=object(),
        tushare=fake,
    )
    assert summary["verify_source_tushare"] is True
    assert summary["source_check"]["pass"] is True
    assert summary["r1_acceptance"] is True
    assert summary["status"] == "PASS", summary
    assert len(summary["collections"]) == 1


def test_run_verify_skip_source_tushare_is_not_an_r1_acceptance(monkeypatch):
    research_db = _source_db_with_index()
    monkeypatch.setattr(wsv, "_stable_db", lambda client: research_db, raising=False)
    summary = wsv.run_verify(
        aliases=["quote"],
        trade_date=None,
        samples=1,
        tolerance=0.005,
        research_db=research_db,
        stable_client=object(),
        verify_source=False,
    )
    assert summary["source_check"] is None
    assert summary["source_check_skipped"] is True
    assert summary["r1_acceptance"] is False
    assert "NOT an R1 acceptance" in summary["warning"]
    assert summary["status"] == "PASS"


def test_run_verify_source_check_failure_fails_the_run(monkeypatch):
    research_db = _source_db_with_index()
    monkeypatch.setattr(wsv, "_stable_db", lambda client: research_db, raising=False)
    fake = _FakeTushare(error=RuntimeError("boom"))
    summary = wsv.run_verify(
        aliases=["quote"],
        trade_date=None,
        samples=1,
        tolerance=0.005,
        research_db=research_db,
        stable_client=object(),
        tushare=fake,
    )
    assert summary["source_check"]["pass"] is False
    assert summary["status"] == "FAIL"


def test_run_verify_rejects_empty_aliases():
    with pytest.raises(ValueError, match="vacuous verification"):
        wsv.run_verify(aliases=[], trade_date=None, samples=1, tolerance=0.005)


def test_run_verify_aggregates_statuses(monkeypatch):
    research_db, stable_db = _make_pair("quote", count=40)
    monkeypatch.setattr(wsv, "_stable_db", lambda client: stable_db, raising=False)

    summary = wsv.run_verify(
        aliases=["quote"],
        trade_date=_day().date(),
        samples=10,
        tolerance=0.005,
        research_db=research_db,
        stable_client=object(),
        verify_source=False,
    )
    assert summary["status"] == "PASS"
    assert summary["scope_version"] == wsv.SCOPE_VERSION
    assert len(summary["collections"]) == 1
    assert summary["collections"][0]["status"] == "PASS"


def _tracking_args():
    return wsv.parse_args(["--collections", "quote", "--skip-source-tushare"])


def test_run_with_tracking_records_success(monkeypatch):
    helper = _FakeJobRunHelper()
    monkeypatch.setattr(wsv, "job_run_helper", helper)
    monkeypatch.setattr(wsv, "_init_db_connection", lambda: None)
    monkeypatch.setattr(
        wsv, "run_verify", lambda **kwargs: {"status": "PASS", "trade_date": "x"}
    )
    summary = wsv._run_with_tracking(_tracking_args())
    assert summary["status"] == "PASS"
    assert helper.cleaned == 1
    assert helper.created == 1
    assert helper.finished[-1]["status"] == "SUCCESS"
    assert helper.finished[-1]["summary"]["status"] == "PASS"


def test_run_with_tracking_records_failure_status(monkeypatch):
    helper = _FakeJobRunHelper()
    monkeypatch.setattr(wsv, "job_run_helper", helper)
    monkeypatch.setattr(wsv, "_init_db_connection", lambda: None)
    monkeypatch.setattr(wsv, "run_verify", lambda **kwargs: {"status": "FAIL"})
    summary = wsv._run_with_tracking(_tracking_args())
    assert summary["status"] == "FAIL"
    assert helper.finished[-1]["status"] == "FAILED"


def test_run_with_tracking_records_failed_on_exception(monkeypatch):
    helper = _FakeJobRunHelper()
    monkeypatch.setattr(wsv, "job_run_helper", helper)
    monkeypatch.setattr(wsv, "_init_db_connection", lambda: None)

    def _boom(**kwargs):
        raise RuntimeError("comparison exploded")

    monkeypatch.setattr(wsv, "run_verify", _boom)
    with pytest.raises(RuntimeError, match="comparison exploded"):
        wsv._run_with_tracking(_tracking_args())
    assert helper.finished[-1]["status"] == "FAILED"
    assert "comparison exploded" in helper.finished[-1]["summary"]["error"]


def test_main_exit_codes(monkeypatch, capsys):
    monkeypatch.setattr(wsv, "_run_with_tracking", lambda args: {"status": "PASS"})
    with pytest.raises(SystemExit) as excinfo:
        wsv.main([])
    assert excinfo.value.code == 0

    monkeypatch.setattr(wsv, "_run_with_tracking", lambda args: {"status": "FAIL"})
    with pytest.raises(SystemExit) as excinfo:
        wsv.main([])
    assert excinfo.value.code == 1

    def _boom(args):
        raise RuntimeError("tracking exploded")

    monkeypatch.setattr(wsv, "_run_with_tracking", _boom)
    with pytest.raises(SystemExit) as excinfo:
        wsv.main([])
    assert excinfo.value.code == 1
    assert "tracking exploded" in capsys.readouterr().out


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


def test_parse_args_source_check_defaults_on_with_skip_opt_out():
    args = wsv.parse_args([])
    assert args.compare_derived is False
    assert args.verify_source_tushare is True
    assert wsv.parse_args(["--skip-source-tushare"]).verify_source_tushare is False
    assert wsv.parse_args(["--verify-source-tushare"]).verify_source_tushare is True
    assert wsv.parse_args(["--compare-derived"]).compare_derived is True


def test_scoring_spec_uses_model_version_key():
    assert wsv.COLLECTION_SPECS["scoring"]["keys"] == [
        "stock_code",
        "date",
        "horizon",
        "model_version",
    ]
