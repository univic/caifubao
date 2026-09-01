"""Tests for the prod→dev sync engine (collection resolution + upsert keys)."""

import datetime
from unittest.mock import MagicMock

import pytest

from app.lib.datahub.sync_engine import (
    SYNC_STATE_COLLECTION,
    SYNC_UPSERT_KEYS,
    _require_date_first_index,
    _resolve_collection_window,
    _resolve_sync_collections,
    _sync_collection,
    run_sync,
)


def test_resolve_sync_collections_keeps_signal_in_dev():
    assert _resolve_sync_collections(None, is_dev_environment=True) == [
        "stock_daily_quote",
        "stock_factor_daily",
        "stock_signal_daily",
        "finance_market",
        "stock_industry",
    ]


def test_resolve_sync_collections_skips_signal_outside_dev():
    assert _resolve_sync_collections(None, is_dev_environment=False) == [
        "stock_daily_quote",
        "stock_factor_daily",
        "finance_market",
        "stock_industry",
    ]


def test_resolve_sync_collections_skips_signal_alias_outside_dev():
    assert _resolve_sync_collections(
        ["quote", "signal"],
        is_dev_environment=False,
    ) == ["stock_daily_quote"]


class _FakeCursor:
    def __init__(self, docs):
        self._docs = iter(docs)
        self.sort_args = None

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._docs)

    def sort(self, *args, **kwargs):
        self.sort_args = (args, kwargs)
        return self


def _fake_collection(name, docs):
    col = MagicMock()
    col.name = name
    col.cursor = _FakeCursor(docs)
    col.find = MagicMock(return_value=col.cursor)
    col.list_indexes.return_value = [{"name": "date_-1", "key": {"date": -1}}]
    return col


def _doc(code, date, _id=None):
    return {"_id": _id or object(), "code": code, "date": date, "close": 10.0}


def test_quote_sync_matches_by_code_and_date():
    src = _fake_collection(
        "stock_daily_quote",
        [
            _doc(
                "sz000001",
                datetime.datetime(2026, 8, 27, tzinfo=datetime.timezone.utc),
                _id="prod-id",
            )
        ],
    )
    dst = MagicMock()
    dst.name = "stock_daily_quote"

    _sync_collection(
        src, dst, date_field="date", from_date=None, to_date=None, dry_run=False
    )

    assert dst.bulk_write.call_count == 1
    ops = dst.bulk_write.call_args.args[0]
    assert len(ops) == 1
    op = ops[0]
    # Filter is the business key, not _id.
    assert op._filter == {
        "code": "sz000001",
        "date": datetime.datetime(2026, 8, 27, tzinfo=datetime.timezone.utc),
    }
    assert op._doc["$set"]["close"] == 10.0
    assert op._doc["$set"]["code"] == "sz000001"


def test_sync_uses_business_key_mapping_for_all_synced_collections():
    assert SYNC_UPSERT_KEYS["stock_daily_quote"] == ["code", "date"]
    assert SYNC_UPSERT_KEYS["stock_factor_daily"] == ["stock_code", "date"]
    assert SYNC_UPSERT_KEYS["stock_signal_daily"] == [
        "stock_code",
        "date",
        "signal_name",
    ]
    assert SYNC_UPSERT_KEYS["stock_industry"] == ["stock_code"]


def test_snapshot_collection_falls_back_to_id():
    src = _fake_collection(
        "finance_market",
        [{"_id": "snap-id", "name": "ChinaAStock", "value": 1}],
    )
    dst = MagicMock()
    dst.name = "finance_market"

    _sync_collection(
        src, dst, date_field=None, from_date=None, to_date=None, dry_run=False
    )

    op = dst.bulk_write.call_args.args[0][0]
    assert op._filter == {"_id": "snap-id"}


def test_same_business_key_different_id_merges_not_inserts():
    """Regression: dev doc with same (code, date) but different _id must be
    updated via the business-key filter, not inserted (E11000)."""
    src = _fake_collection(
        "stock_daily_quote",
        [
            _doc(
                "sz000001",
                datetime.datetime(2026, 8, 27, tzinfo=datetime.timezone.utc),
                _id="prod-id",
            )
        ],
    )
    dst = MagicMock()
    dst.name = "stock_daily_quote"
    # dev already holds the same business key under a different _id
    dst.bulk_write.return_value = MagicMock(upserted_count=0, modified_count=1)

    _sync_collection(
        src, dst, date_field="date", from_date=None, to_date=None, dry_run=False
    )

    op = dst.bulk_write.call_args.args[0][0]
    assert op._filter == {
        "code": "sz000001",
        "date": datetime.datetime(2026, 8, 27, tzinfo=datetime.timezone.utc),
    }
    assert "_id" not in op._doc["$set"]
    # upsert=True with a business-key filter merges (no duplicate insert)
    assert op._upsert is True


def test_business_key_collection_falls_back_to_id_when_key_missing():
    src = _fake_collection(
        "stock_daily_quote",
        [{"_id": "odd-doc", "code": "sz000001", "close": 1.0}],  # no date field
    )
    dst = MagicMock()
    dst.name = "stock_daily_quote"

    _sync_collection(
        src, dst, date_field="date", from_date=None, to_date=None, dry_run=False
    )

    op = dst.bulk_write.call_args.args[0][0]
    assert op._filter == {"_id": "odd-doc"}


def test_collection_window_uses_destination_watermark_with_overlap():
    latest = datetime.datetime(2026, 8, 31, tzinfo=datetime.UTC)

    window = _resolve_collection_window(
        date_field="date",
        from_date=None,
        to_date=None,
        full_sync=False,
        overlap_days=3,
        sync_state={"bootstrap_complete": True, "watermark": latest},
    )

    assert window == (latest - datetime.timedelta(days=3), None, latest)


def test_collection_window_preserves_explicit_date_range():
    from_date = datetime.datetime(2026, 8, 31, tzinfo=datetime.UTC)
    to_date = datetime.datetime(2026, 9, 1, tzinfo=datetime.UTC)

    window = _resolve_collection_window(
        date_field="date",
        from_date=from_date,
        to_date=to_date,
        full_sync=False,
        overlap_days=3,
        sync_state=None,
    )

    assert window == (from_date, to_date, None)


def test_collection_window_without_completed_bootstrap_stays_full():
    assert _resolve_collection_window(
        date_field="date",
        from_date=None,
        to_date=None,
        full_sync=False,
        overlap_days=3,
        sync_state=None,
    ) == (None, None, None)


def test_partial_bootstrap_cannot_become_incremental_watermark():
    partial_latest = datetime.datetime(2026, 8, 31, tzinfo=datetime.UTC)

    assert _resolve_collection_window(
        date_field="date",
        from_date=None,
        to_date=None,
        full_sync=False,
        overlap_days=3,
        sync_state={
            "bootstrap_complete": False,
            "watermark": partial_latest,
        },
    ) == (None, None, None)


def test_collection_window_full_sync_ignores_watermark():
    assert _resolve_collection_window(
        date_field="date",
        from_date=None,
        to_date=None,
        full_sync=True,
        overlap_days=3,
        sync_state={
            "bootstrap_complete": True,
            "watermark": datetime.datetime(2026, 8, 31, tzinfo=datetime.UTC),
        },
    ) == (None, None, None)


def test_date_collection_reads_newest_business_date_first():
    src = _fake_collection("stock_daily_quote", [])
    dst = MagicMock()
    dst.name = "stock_daily_quote"

    _sync_collection(
        src,
        dst,
        date_field="date",
        from_date=datetime.datetime(2026, 8, 28, tzinfo=datetime.UTC),
        to_date=None,
        dry_run=True,
    )

    assert src.cursor.sort_args == (([("date", -1)],), {})


def test_run_sync_resolves_each_collection_from_its_own_watermark(monkeypatch):
    from app.lib.datahub import sync_engine

    quote_watermark = datetime.datetime(2026, 8, 31, tzinfo=datetime.UTC)
    factor_watermark = datetime.datetime(2026, 8, 30, tzinfo=datetime.UTC)
    src_db = {
        "stock_daily_quote": _fake_collection("stock_daily_quote", []),
        "stock_factor_daily": _fake_collection("stock_factor_daily", []),
    }
    src_db["stock_daily_quote"].find_one.return_value = {"date": quote_watermark}
    src_db["stock_factor_daily"].find_one.return_value = {"date": factor_watermark}
    dst_quote = MagicMock()
    dst_quote.name = "stock_daily_quote"
    dst_quote.list_indexes.return_value = [{"name": "date_-1", "key": {"date": -1}}]
    dst_quote.find_one.return_value = {"date": quote_watermark}
    dst_factor = MagicMock()
    dst_factor.name = "stock_factor_daily"
    dst_factor.list_indexes.return_value = [{"name": "date_-1", "key": {"date": -1}}]
    dst_factor.find_one.return_value = {"date": factor_watermark}
    state_collection = MagicMock()
    states = {
        "stock_daily_quote": {
            "bootstrap_complete": True,
            "watermark": quote_watermark,
        },
        "stock_factor_daily": {
            "bootstrap_complete": True,
            "watermark": factor_watermark,
        },
    }
    state_collection.find_one.side_effect = lambda query: states.get(query["_id"])
    dst_db = {
        "stock_daily_quote": dst_quote,
        "stock_factor_daily": dst_factor,
        SYNC_STATE_COLLECTION: state_collection,
    }
    calls = []

    monkeypatch.setattr(sync_engine, "_get_src_db", lambda *_: src_db)
    monkeypatch.setattr(sync_engine, "_get_dst_db", lambda *_: dst_db)
    monkeypatch.setattr(sync_engine, "_is_dev_environment", lambda: True)
    monkeypatch.setattr(
        sync_engine,
        "_sync_collection",
        lambda **kwargs: (
            calls.append(kwargs) or {"read": 0, "upserted": 0, "modified": 0}
        ),
    )

    result = run_sync(
        src_client=MagicMock(),
        collections=["quote", "factor"],
        overlap_days=3,
    )

    assert calls[0]["from_date"] == quote_watermark - datetime.timedelta(days=3)
    assert calls[1]["from_date"] == factor_watermark - datetime.timedelta(days=3)
    assert (
        result["collections"]["stock_daily_quote"]["destination_watermark"]
        == quote_watermark
    )
    assert (
        result["collections"]["stock_factor_daily"]["destination_watermark"]
        == factor_watermark
    )
    assert state_collection.update_one.call_count == 2


def test_failed_bootstrap_does_not_write_completion_marker(monkeypatch):
    from app.lib.datahub import sync_engine

    source_watermark = datetime.datetime(2026, 8, 31, tzinfo=datetime.UTC)
    src = _fake_collection("stock_daily_quote", [])
    src.find_one.return_value = {"date": source_watermark}
    dst = MagicMock()
    dst.name = "stock_daily_quote"
    dst.list_indexes.return_value = [{"name": "date_-1", "key": {"date": -1}}]
    state_collection = MagicMock()
    state_collection.find_one.return_value = None
    monkeypatch.setattr(
        sync_engine,
        "_get_src_db",
        lambda *_: {"stock_daily_quote": src},
    )
    monkeypatch.setattr(
        sync_engine,
        "_get_dst_db",
        lambda *_: {
            "stock_daily_quote": dst,
            SYNC_STATE_COLLECTION: state_collection,
        },
    )
    monkeypatch.setattr(sync_engine, "_is_dev_environment", lambda: True)
    monkeypatch.setattr(
        sync_engine,
        "_sync_collection",
        lambda **_: (_ for _ in ()).throw(RuntimeError("deadline")),
    )

    with pytest.raises(RuntimeError, match="deadline"):
        run_sync(src_client=MagicMock(), collections=["quote"])

    state_collection.update_one.assert_not_called()


def test_sync_models_define_date_first_indexes():
    from app.model.factor import StockFactorDaily
    from app.model.stock import StockDailyQuote

    assert any(
        spec["fields"] == [("date", -1)]
        for spec in StockFactorDaily._meta["index_specs"]
    )
    assert any(
        spec["fields"] == [("date", -1)]
        for spec in StockDailyQuote._meta["index_specs"]
    )


def test_sync_requires_date_first_index():
    collection = MagicMock()
    collection.name = "stock_factor_daily"
    collection.list_indexes.return_value = [
        {"name": "stock_code_1_date_1", "key": {"stock_code": 1, "date": 1}}
    ]

    with pytest.raises(RuntimeError, match="index beginning with date"):
        _require_date_first_index(collection, "date", role="source")


@pytest.mark.parametrize(
    "index",
    [
        {"name": "date_hashed", "key": {"date": "hashed"}},
        {"name": "date_sparse", "key": {"date": -1}, "sparse": True},
        {
            "name": "date_partial",
            "key": {"date": -1},
            "partialFilterExpression": {"date": {"$exists": True}},
        },
    ],
)
def test_sync_rejects_date_indexes_that_cannot_cover_all_sorted_docs(index):
    collection = MagicMock()
    collection.name = "stock_factor_daily"
    collection.list_indexes.return_value = [index]

    with pytest.raises(RuntimeError, match="requires a non-sparse, non-partial"):
        _require_date_first_index(collection, "date", role="source")


def test_run_sync_rejects_full_sync_with_explicit_window():
    with pytest.raises(ValueError, match="full_sync cannot be combined"):
        run_sync(
            src_client=MagicMock(),
            from_date=datetime.datetime(2026, 8, 31, tzinfo=datetime.UTC),
            full_sync=True,
        )


def test_run_sync_preflights_all_indexes_before_first_collection_write(monkeypatch):
    from app.lib.datahub import sync_engine

    src_quote = _fake_collection("stock_daily_quote", [])
    src_factor = _fake_collection("stock_factor_daily", [])
    dst_quote = _fake_collection("stock_daily_quote", [])
    dst_factor = _fake_collection("stock_factor_daily", [])
    dst_factor.list_indexes.return_value = [
        {"name": "stock_code_1_date_1", "key": {"stock_code": 1, "date": 1}}
    ]
    state_collection = MagicMock()
    monkeypatch.setattr(
        sync_engine,
        "_get_src_db",
        lambda *_: {
            "stock_daily_quote": src_quote,
            "stock_factor_daily": src_factor,
        },
    )
    monkeypatch.setattr(
        sync_engine,
        "_get_dst_db",
        lambda *_: {
            "stock_daily_quote": dst_quote,
            "stock_factor_daily": dst_factor,
            SYNC_STATE_COLLECTION: state_collection,
        },
    )
    monkeypatch.setattr(sync_engine, "_is_dev_environment", lambda: True)
    sync_collection = MagicMock()
    monkeypatch.setattr(sync_engine, "_sync_collection", sync_collection)

    with pytest.raises(RuntimeError, match="destination collection stock_factor_daily"):
        run_sync(src_client=MagicMock(), collections=["quote", "factor"])

    sync_collection.assert_not_called()
    state_collection.update_one.assert_not_called()
