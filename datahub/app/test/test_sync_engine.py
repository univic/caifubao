"""Tests for the prod→dev sync engine (collection resolution + upsert keys)."""

import datetime
from unittest.mock import MagicMock

from app.lib.datahub.sync_engine import (
    SYNC_UPSERT_KEYS,
    _resolve_sync_collections,
    _sync_collection,
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

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._docs)

    def sort(self, *args, **kwargs):
        return self


def _fake_collection(name, docs):
    col = MagicMock()
    col.name = name
    col.find = MagicMock(return_value=_FakeCursor(docs))
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
