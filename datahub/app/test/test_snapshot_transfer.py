# -*- coding: utf-8 -*-
"""Tests for the controlled snapshot export/import tooling.

Pure unit tests: pymongo handles are MagicMock'd (same style as
test_sync_engine.py) while the snapshot files/manifests on disk are real,
so the verify-then-apply mechanics are exercised end to end without a live
MongoDB.
"""

import datetime
import gzip
import hashlib
import json
import os
import socket
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from bson import ObjectId
from bson.decimal128 import Decimal128

from app.lib.datahub import snapshot_transfer as st

UTC = datetime.timezone.utc
QUOTE_DATE = datetime.datetime(2026, 9, 4)


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, docs):
        self._docs = iter(docs)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._docs)

    def sort(self, *args, **kwargs):
        return self


def _export_col(name, docs):
    col = MagicMock()
    col.name = name
    col.cursor = _FakeCursor(docs)
    col.find = MagicMock(return_value=col.cursor)
    return col


def _export_db(docs_by_name):
    """Mock Database for run_export: named docs, empty collections otherwise."""
    cols = {name: _export_col(name, docs) for name, docs in docs_by_name.items()}

    def _getitem(name):
        if name not in cols:
            cols[name] = _export_col(name, [])
        return cols[name]

    db = MagicMock()
    db.name = "caifubao_data"
    db.__getitem__.side_effect = _getitem
    return db, cols


def _import_db():
    """Mock Database for run_import with lazily created collection mocks."""
    cols = {}

    def _getitem(name):
        if name not in cols:
            col = MagicMock()
            col.name = name
            col.bulk_write.return_value = MagicMock(upserted_count=0, modified_count=0)
            col.count_documents.return_value = 0
            cols[name] = col
        return cols[name]

    db = MagicMock()
    db.name = "caifubao_dev"
    db.__getitem__.side_effect = _getitem
    return db, cols


def _quote_doc(code="sz000001", date=QUOTE_DATE, oid=None):
    return {
        "_id": oid or ObjectId("650a1b2c3d4e5f60718293a4"),
        "code": code,
        "date": date,
        "close": 10.5,
    }


def _industry_doc(code="sz000001", name="PingAn"):
    return {
        "_id": ObjectId("650a1b2c3d4e5f60718293a5"),
        "stock_code": code,
        "name": name,
    }


def _write_snapshot(
    base: Path,
    payloads: dict[str, list[dict]],
    *,
    snapshot_id: str = "snapshot-test00000000",
    producer_image: str = "registry/caifubao-datahub@sha256:abc",
    overrides: dict[str, dict] | None = None,
) -> tuple[Path, str]:
    """Hand-build a valid snapshot directory; returns (dir, manifest_sha256)."""
    base.mkdir(parents=True, exist_ok=True)
    snapshot_dir = base / snapshot_id
    snapshot_dir.mkdir(exist_ok=True)
    overrides = overrides or {}

    entries = []
    for name, docs in payloads.items():
        file_name = f"{name}.jsonl.gz"
        file_path = snapshot_dir / file_name
        content = "".join(st.encode_bson_json(doc) + "\n" for doc in docs).encode(
            "utf-8"
        )
        file_path.write_bytes(gzip.compress(content))
        entry = {
            "name": name,
            "file": file_name,
            "sha256": hashlib.sha256(file_path.read_bytes()).hexdigest(),
            "doc_count": len(docs),
            "data_as_of": None,
            "upsert_keys": st._expected_upsert_keys(name),
            "class": st._collection_class(name),
        }
        date_field = st._date_field(name)
        if date_field:
            dates = [
                st._as_naive_utc(doc[date_field])
                for doc in docs
                if isinstance(doc.get(date_field), datetime.datetime)
            ]
            entry["data_as_of"] = max(dates).isoformat() if dates else None
        entry.update(overrides.get(name, {}))
        entries.append(entry)

    manifest = {
        "manifest_version": st.SNAPSHOT_MANIFEST_VERSION,
        "snapshot_id": snapshot_id,
        "created_at": "2026-09-05T02:00:00+00:00",
        "producer_db": "caifubao_data",
        "producer_image": producer_image,
        "collections": entries,
    }
    payload = (
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    manifest_sha = hashlib.sha256(payload).hexdigest()
    (snapshot_dir / st.MANIFEST_NAME).write_bytes(payload)
    (snapshot_dir / st.MANIFEST_CHECKSUM_NAME).write_text(
        f"{manifest_sha}  {st.MANIFEST_NAME}\n", encoding="utf-8"
    )
    return snapshot_dir, manifest_sha


def _rewrite_manifest(snapshot_dir: Path, mutate) -> None:
    """Rewrite manifest.json with `mutate(dict)` applied and a fresh checksum."""
    manifest = json.loads((snapshot_dir / st.MANIFEST_NAME).read_text(encoding="utf-8"))
    mutate(manifest)
    payload = (
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    (snapshot_dir / st.MANIFEST_NAME).write_bytes(payload)
    (snapshot_dir / st.MANIFEST_CHECKSUM_NAME).write_text(
        f"{hashlib.sha256(payload).hexdigest()}  {st.MANIFEST_NAME}\n", encoding="utf-8"
    )


def _assert_no_writes(db, cols):
    for name, col in cols.items():
        col.insert_many.assert_not_called()
        col.replace_one.assert_not_called()
        if name.endswith(st.STAGING_SUFFIX):
            col.drop.assert_not_called()
            col.count_documents.assert_not_called()
        else:
            col.bulk_write.assert_not_called()
    db.client.admin.command.assert_not_called()


def _patch_job_tracking(monkeypatch, runner):
    """Stub the job-run tracking calls shared by both runners."""
    contexts = []

    monkeypatch.setattr(runner, "_init_db_connection", lambda: None)
    monkeypatch.setattr(
        runner.job_run_helper, "mark_stale_running_job_runs_failed", lambda **_: 0
    )
    monkeypatch.setattr(
        runner.job_run_helper,
        "create_job_run",
        lambda context: contexts.append(context) or object(),
    )
    monkeypatch.setattr(runner.job_run_helper, "finish_job_run", lambda *_, **__: None)
    return contexts


# ---------------------------------------------------------------------------
# BSON-safe JSONL encoding
# ---------------------------------------------------------------------------


def test_bson_json_round_trips_datetime_objectid_and_decimal128():
    doc = {
        "_id": ObjectId("650a1b2c3d4e5f60718293a4"),
        "date": datetime.datetime(2026, 9, 4, 15, 30, 59, 123000),
        "aware": datetime.datetime(2026, 9, 4, 15, 30, tzinfo=UTC),
        "amount": Decimal128("19.99"),
        "nested": {
            "turnover": Decimal128("1.5"),
            "oid": ObjectId("650a1b2c3d4e5f60718293a6"),
        },
        "tags": [datetime.datetime(2026, 9, 1)],
        "plain": "2026-09-04",
    }
    encoded = st.encode_bson_json(doc)
    assert '"__bson_date"' in encoded
    assert '"__bson_oid"' in encoded
    decoded = st.decode_bson_json(encoded)
    assert decoded["_id"] == doc["_id"]
    assert decoded["date"] == doc["date"]
    # aware datetimes decode to naive UTC (pymongo default representation)
    assert decoded["aware"] == datetime.datetime(2026, 9, 4, 15, 30)
    # documented lossy case: Decimal128 -> string
    assert decoded["amount"] == "19.99"
    assert decoded["nested"]["turnover"] == "1.5"
    assert decoded["nested"]["oid"] == ObjectId("650a1b2c3d4e5f60718293a6")
    assert decoded["tags"] == [datetime.datetime(2026, 9, 1)]
    # plain strings are never re-interpreted as BSON markers
    assert decoded["plain"] == "2026-09-04"


def test_bson_encoder_fails_closed_on_unsupported_types():
    with pytest.raises(st.SnapshotTransferError, match="unsupported type 'bytes'"):
        st.encode_bson_json({"blob": b"\x00\x01"})
    with pytest.raises(st.SnapshotTransferError, match="unsupported type 'date'"):
        st.encode_bson_json({"day": datetime.date(2026, 9, 4)})


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------


def test_export_writes_files_manifest_and_checksum(tmp_path):
    docs = [_quote_doc(), _quote_doc(code="sz000002")]
    db, _cols = _export_db(
        {"stock_daily_quote": docs, "stock_industry": [_industry_doc()]}
    )

    result = st.run_export(db=db, out_dir=tmp_path, producer_image="img@sha256:abc")

    snapshot_dir = Path(result["snapshot_dir"])
    assert result["status"] == "GOOD"
    assert snapshot_dir.name == result["snapshot_id"]
    assert snapshot_dir.is_dir()

    manifest_path = snapshot_dir / st.MANIFEST_NAME
    checksum_path = snapshot_dir / st.MANIFEST_CHECKSUM_NAME
    assert manifest_path.is_file() and checksum_path.is_file()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    declared_sha = checksum_path.read_text(encoding="utf-8").split()[0]
    assert declared_sha == hashlib.sha256(manifest_path.read_bytes()).hexdigest()

    assert manifest["manifest_version"] == 1
    assert manifest["snapshot_id"] == snapshot_dir.name
    assert manifest["producer_db"] == "caifubao_data"
    assert manifest["producer_image"] == "img@sha256:abc"
    datetime.datetime.fromisoformat(manifest["created_at"])

    by_name = {entry["name"]: entry for entry in manifest["collections"]}
    assert set(by_name) == set(st.SNAPSHOT_EXPORT_COLLECTIONS)

    quote_entry = by_name["stock_daily_quote"]
    assert quote_entry["file"] == "stock_daily_quote.jsonl.gz"
    assert quote_entry["doc_count"] == 2
    assert quote_entry["class"] == "date_partitioned"
    assert quote_entry["upsert_keys"] == ["code", "date"]
    assert quote_entry["data_as_of"] == QUOTE_DATE.isoformat()
    assert (
        quote_entry["sha256"]
        == hashlib.sha256((snapshot_dir / quote_entry["file"]).read_bytes()).hexdigest()
    )

    industry_entry = by_name["stock_industry"]
    assert industry_entry["class"] == "snapshot"
    assert industry_entry["upsert_keys"] is None
    assert industry_entry["data_as_of"] is None

    # the written file round-trips through the BSON-safe decoder
    with gzip.open(snapshot_dir / quote_entry["file"], "rt", encoding="utf-8") as fh:
        decoded = [st.decode_bson_json(line) for line in fh]
    assert decoded[0]["code"] == "sz000001"
    assert decoded[0]["date"] == QUOTE_DATE
    assert decoded[0]["_id"] == ObjectId("650a1b2c3d4e5f60718293a4")


def test_export_resolves_aliases_and_refuses_unknown_collections(tmp_path):
    db, _cols = _export_db({})
    result = st.run_export(
        db=db, out_dir=tmp_path, collections=["quote", "industry"], producer_image="img"
    )
    manifest = json.loads(
        (Path(result["snapshot_dir"]) / st.MANIFEST_NAME).read_text(encoding="utf-8")
    )
    assert [entry["name"] for entry in manifest["collections"]] == [
        "stock_daily_quote",
        "stock_industry",
    ]

    with pytest.raises(st.SnapshotTransferError, match="refused unknown"):
        st.run_export(
            db=db,
            out_dir=tmp_path,
            collections=["quote", "basic_stock"],
            producer_image="img",
        )


def test_export_applies_date_window_only_to_date_partitioned_collections(tmp_path):
    db, cols = _export_db({})
    from_date = datetime.datetime(2026, 9, 1)
    to_date = datetime.datetime(2026, 9, 4)

    st.run_export(
        db=db,
        out_dir=tmp_path,
        collections=["quote", "market"],
        from_date=from_date,
        to_date=to_date,
        producer_image="img",
    )

    assert cols["stock_daily_quote"].find.call_args.args[0] == {
        "date": {"$gte": from_date, "$lte": to_date}
    }
    # snapshot-class collections are always fully exported
    assert cols["finance_market"].find.call_args.args[0] == {}


def test_export_dry_run_writes_nothing(tmp_path):
    db, _cols = _export_db({"stock_daily_quote": [_quote_doc()]})
    result = st.run_export(
        db=db,
        out_dir=tmp_path,
        collections=["quote"],
        producer_image="img",
        dry_run=True,
    )
    assert result["status"] == "DRY_RUN"
    assert result["plan"]["stock_daily_quote"]["class"] == "date_partitioned"
    assert list(tmp_path.iterdir()) == []


def test_export_refuses_to_overwrite_an_existing_snapshot(tmp_path, monkeypatch):
    monkeypatch.setattr(st, "_new_snapshot_id", lambda now=None: "snapshot-fixed")
    (tmp_path / "snapshot-fixed").mkdir()
    db, _cols = _export_db({})

    with pytest.raises(st.SnapshotTransferError, match="refusing to overwrite"):
        st.run_export(
            db=db, out_dir=tmp_path, collections=["quote"], producer_image="img"
        )


def test_export_failure_leaves_no_partial_snapshot_dir(tmp_path):
    db, cols = _export_db({})

    def _explode(*_args, **_kwargs):
        raise RuntimeError("boom")

    db["finance_market"].find.side_effect = _explode

    with pytest.raises(RuntimeError, match="boom"):
        st.run_export(
            db=db,
            out_dir=tmp_path,
            collections=["quote", "market"],
            producer_image="img",
        )

    assert list(tmp_path.iterdir()) == []


# ---------------------------------------------------------------------------
# Import: verification (pass 1)
# ---------------------------------------------------------------------------


def test_import_happy_path_applies_upsert_and_replace(tmp_path, monkeypatch):
    payloads = {"stock_daily_quote": [_quote_doc(), _quote_doc(code="sz000002")]}
    snapshot_dir, manifest_sha = _write_snapshot(tmp_path, payloads)

    db, cols = _import_db()
    refresh = MagicMock(return_value={"refreshed_via": "initializer"})
    monkeypatch.setattr(st, "refresh_asset_status_after_import", refresh)

    summary = st.run_import(
        db=db, snapshot_dir=snapshot_dir, producer_image_note="dev-pod"
    )

    assert summary["status"] == "GOOD"
    assert summary["manifest_sha256"] == manifest_sha
    assert summary["snapshot_id"] == snapshot_dir.name

    # date-partitioned: business-key ReplaceOne upserts
    quote_ops = cols["stock_daily_quote"].bulk_write.call_args.args[0]
    assert len(quote_ops) == 2
    assert quote_ops[0]._filter == {"code": "sz000001", "date": QUOTE_DATE}
    assert quote_ops[0]._upsert is True
    assert "_id" not in quote_ops[0]._filter
    # the replacement doc excludes _id so a re-apply cannot change identity
    assert "_id" not in quote_ops[0]._doc
    assert quote_ops[0]._doc["code"] == "sz000001"
    assert quote_ops[0]._doc["close"] == 10.5

    refresh.assert_called_once_with(["stock_daily_quote"])

    record_call = cols[st.SNAPSHOT_IMPORT_STATE_COLLECTION].replace_one.call_args
    record = record_call.args[1]
    assert record_call.args[0] == {"_id": manifest_sha}
    assert record["snapshot_id"] == snapshot_dir.name
    assert record["manifest_sha256"] == manifest_sha
    assert record["producer_image"] == "registry/caifubao-datahub@sha256:abc"
    assert record["producer_image_note"] == "dev-pod"
    assert record["dry_run"] is False
    datetime.datetime.fromisoformat(record["applied_at"].isoformat())
    assert record["collections"] == [
        {
            "name": "stock_daily_quote",
            "doc_count": 2,
            "data_as_of": QUOTE_DATE.isoformat(),
            "sha256": summary["collections"]["stock_daily_quote"]["sha256"],
            "class": "date_partitioned",
            "applied_as": "upsert",
        }
    ]


def _stub_asset_status_refresh(monkeypatch):
    """Keep apply-phase tests off the real data_asset_status refresh path."""
    refresh = MagicMock(return_value={"refreshed_via": "stub", "asset_count": 0})
    monkeypatch.setattr(st, "refresh_asset_status_after_import", refresh)
    return refresh


def test_import_replaces_snapshot_class_collections_atomically(tmp_path, monkeypatch):
    docs = [_industry_doc(), _industry_doc(code="sz000002")]
    snapshot_dir, _sha = _write_snapshot(tmp_path, {"stock_industry": docs})
    _stub_asset_status_refresh(monkeypatch)

    db, cols = _import_db()
    staging = db["stock_industry__snapshot_staging"]
    staging.count_documents.return_value = len(docs)
    order = []
    staging.drop.side_effect = lambda: order.append("drop")
    staging.insert_many.side_effect = lambda batch: order.append("insert")
    staging.count_documents.side_effect = lambda _q: (order.append("count"), len(docs))[
        1
    ]
    db.client.admin.command.side_effect = lambda cmd, **k: order.append("rename")

    summary = st.run_import(db=db, snapshot_dir=snapshot_dir)

    assert summary["status"] == "GOOD"
    assert order == ["drop", "insert", "count", "rename"]
    db.client.admin.command.assert_called_once_with(
        {
            "renameCollection": f"caifubao_dev.stock_industry{st.STAGING_SUFFIX}",
            "to": "caifubao_dev.stock_industry",
            "dropTarget": True,
        }
    )
    # the target is replaced BY the atomic rename (dropTarget=True), never
    # pre-dropped — a failed rename must leave the previous state intact
    db["stock_industry"].drop.assert_not_called()
    entry = summary["collections"]["stock_industry"]
    assert entry["applied_as"] == "replace"
    assert entry["read"] == 2
    assert entry["staged"] == 2


def test_import_verifies_manifest_checksum_and_aborts_before_writes(tmp_path):
    snapshot_dir, _sha = _write_snapshot(
        tmp_path, {"stock_daily_quote": [_quote_doc()]}
    )
    # corrupt the manifest content without updating its checksum file
    manifest = json.loads((snapshot_dir / st.MANIFEST_NAME).read_text(encoding="utf-8"))
    manifest["producer_image"] = "tampered"
    (snapshot_dir / st.MANIFEST_NAME).write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )

    db, cols = _import_db()
    with pytest.raises(st.SnapshotTransferError, match="manifest checksum mismatch"):
        st.run_import(db=db, snapshot_dir=snapshot_dir)
    _assert_no_writes(db, cols)


def test_import_rejects_data_file_checksum_mismatch_before_any_write(tmp_path):
    snapshot_dir, _sha = _write_snapshot(
        tmp_path,
        {"stock_daily_quote": [_quote_doc()], "stock_industry": [_industry_doc()]},
    )
    # tamper AFTER a verified manifest exists: append another document
    file_path = snapshot_dir / "stock_daily_quote.jsonl.gz"
    original = gzip.decompress(file_path.read_bytes())
    extra = st.encode_bson_json(_quote_doc(code="sz999999")) + "\n"
    file_path.write_bytes(gzip.compress(original + extra.encode("utf-8")))

    db, cols = _import_db()
    with pytest.raises(
        st.SnapshotTransferError,
        match="checksum mismatch for collection stock_daily_quote",
    ):
        st.run_import(db=db, snapshot_dir=snapshot_dir)
    _assert_no_writes(db, cols)


def test_import_rejects_doc_count_mismatch_before_any_write(tmp_path):
    snapshot_dir, _sha = _write_snapshot(
        tmp_path,
        {"stock_daily_quote": [_quote_doc()]},
        overrides={"stock_daily_quote": {"doc_count": 5}},
    )

    db, cols = _import_db()
    with pytest.raises(st.SnapshotTransferError, match="document count mismatch"):
        st.run_import(db=db, snapshot_dir=snapshot_dir)
    _assert_no_writes(db, cols)


def test_import_rejects_data_as_of_watermark_mismatch(tmp_path):
    snapshot_dir, _sha = _write_snapshot(
        tmp_path,
        {"stock_daily_quote": [_quote_doc()]},
        overrides={"stock_daily_quote": {"data_as_of": "2026-12-31T00:00:00"}},
    )

    db, cols = _import_db()
    with pytest.raises(st.SnapshotTransferError, match="data_as_of watermark mismatch"):
        st.run_import(db=db, snapshot_dir=snapshot_dir)
    _assert_no_writes(db, cols)


def test_import_rejects_unsupported_manifest_version(tmp_path):
    snapshot_dir, _sha = _write_snapshot(
        tmp_path, {"stock_daily_quote": [_quote_doc()]}
    )
    _rewrite_manifest(snapshot_dir, lambda m: m.update(manifest_version=2))

    db, cols = _import_db()
    with pytest.raises(
        st.SnapshotTransferError, match="unsupported snapshot manifest_version 2"
    ):
        st.run_import(db=db, snapshot_dir=snapshot_dir)
    _assert_no_writes(db, cols)


def test_import_rejects_missing_manifest(tmp_path):
    db, cols = _import_db()
    with pytest.raises(st.SnapshotTransferError, match="manifest not found"):
        st.run_import(db=db, snapshot_dir=tmp_path / "does-not-exist")
    _assert_no_writes(db, cols)


def test_import_rejects_collections_outside_the_allow_list_by_name(tmp_path):
    # basic_stock is a dev-side mongoengine collection, NOT on the allow-list
    snapshot_dir, _sha = _write_snapshot(
        tmp_path,
        {"stock_daily_quote": [_quote_doc()], "basic_stock": [{"code": "sz000001"}]},
    )

    db, cols = _import_db()
    with pytest.raises(st.SnapshotTransferError, match="allow-list.*basic_stock"):
        st.run_import(db=db, snapshot_dir=snapshot_dir)
    _assert_no_writes(db, cols)


def test_import_rejects_manifest_class_mismatch(tmp_path):
    snapshot_dir, _sha = _write_snapshot(
        tmp_path,
        {"stock_industry": [_industry_doc()]},
        overrides={"stock_industry": {"class": "date_partitioned"}},
    )

    db, cols = _import_db()
    with pytest.raises(
        st.SnapshotTransferError, match="does not match the importer class"
    ):
        st.run_import(db=db, snapshot_dir=snapshot_dir)
    _assert_no_writes(db, cols)


def test_import_rejects_upsert_keys_mismatch(tmp_path):
    snapshot_dir, _sha = _write_snapshot(
        tmp_path,
        {"stock_daily_quote": [_quote_doc()]},
        overrides={"stock_daily_quote": {"upsert_keys": ["stock_code"]}},
    )

    db, cols = _import_db()
    with pytest.raises(
        st.SnapshotTransferError, match="does not match the importer business keys"
    ):
        st.run_import(db=db, snapshot_dir=snapshot_dir)
    _assert_no_writes(db, cols)


def test_import_rejects_path_escape_in_manifest_file_names(tmp_path):
    snapshot_dir, _sha = _write_snapshot(
        tmp_path, {"stock_industry": [_industry_doc()]}
    )
    _rewrite_manifest(
        snapshot_dir,
        lambda m: m["collections"][0].update(file="../../etc/passwd"),
    )

    db, cols = _import_db()
    with pytest.raises(
        st.SnapshotTransferError, match="not a plain .jsonl.gz file name"
    ):
        st.run_import(db=db, snapshot_dir=snapshot_dir)
    _assert_no_writes(db, cols)


# ---------------------------------------------------------------------------
# Import: apply (pass 2)
# ---------------------------------------------------------------------------


def test_import_upsert_is_idempotent_across_repeated_runs(tmp_path, monkeypatch):
    docs = [
        _quote_doc(),
        _quote_doc(code="sz000002", date=datetime.datetime(2026, 9, 3)),
    ]
    snapshot_dir, _sha = _write_snapshot(tmp_path, {"stock_daily_quote": docs})
    db, cols = _import_db()
    _stub_asset_status_refresh(monkeypatch)

    first_filters, second_filters = [], []

    def _capture_first(ops, **_kwargs):
        first_filters.extend(op._filter for op in ops)
        return MagicMock(upserted_count=len(ops), modified_count=0)

    def _capture_second(ops, **_kwargs):
        second_filters.extend(op._filter for op in ops)
        return MagicMock(upserted_count=0, modified_count=len(ops))

    db["stock_daily_quote"].bulk_write.side_effect = _capture_first
    st.run_import(db=db, snapshot_dir=snapshot_dir)
    db["stock_daily_quote"].bulk_write.side_effect = _capture_second
    st.run_import(db=db, snapshot_dir=snapshot_dir)

    expected = [
        {"code": "sz000001", "date": QUOTE_DATE},
        {"code": "sz000002", "date": datetime.datetime(2026, 9, 3)},
    ]
    assert first_filters == expected
    # re-importing the same snapshot targets exactly the same business keys,
    # so the second pass updates instead of duplicating documents
    assert second_filters == first_filters


def test_import_upsert_falls_back_to_id_when_business_key_missing(
    tmp_path, monkeypatch
):
    doc = {"_id": "odd-doc", "code": "sz000001"}  # no date field
    snapshot_dir, _sha = _write_snapshot(tmp_path, {"stock_daily_quote": [doc]})
    db, cols = _import_db()
    _stub_asset_status_refresh(monkeypatch)

    st.run_import(db=db, snapshot_dir=snapshot_dir)

    op = cols["stock_daily_quote"].bulk_write.call_args.args[0][0]
    assert op._filter == {"_id": "odd-doc"}
    assert op._upsert is True


def test_import_staging_count_mismatch_leaves_target_untouched(tmp_path):
    docs = [_industry_doc(), _industry_doc(code="sz000002")]
    snapshot_dir, _sha = _write_snapshot(tmp_path, {"stock_industry": docs})
    db, cols = _import_db()
    db["stock_industry__snapshot_staging"].count_documents.return_value = 1

    with pytest.raises(
        st.SnapshotTransferError, match="target collection was NOT modified"
    ):
        st.run_import(db=db, snapshot_dir=snapshot_dir)

    db["stock_industry"].drop.assert_not_called()
    db.client.admin.command.assert_not_called()
    db[st.SNAPSHOT_IMPORT_STATE_COLLECTION].replace_one.assert_not_called()


def test_import_dry_run_verifies_but_writes_nothing(tmp_path):
    snapshot_dir, manifest_sha = _write_snapshot(
        tmp_path,
        {"stock_daily_quote": [_quote_doc()], "stock_industry": [_industry_doc()]},
    )
    db, cols = _import_db()

    summary = st.run_import(db=db, snapshot_dir=snapshot_dir, dry_run=True)

    assert summary["status"] == "DRY_RUN"
    assert summary["collections_verified"] == 2
    assert summary["manifest_sha256"] == manifest_sha
    _assert_no_writes(db, cols)


def test_import_refresh_failure_is_reported_as_a_failed_import(tmp_path, monkeypatch):
    snapshot_dir, _sha = _write_snapshot(
        tmp_path, {"stock_daily_quote": [_quote_doc()]}
    )
    db, cols = _import_db()

    def _boom(_names):
        raise RuntimeError("status backend down")

    monkeypatch.setattr(st, "refresh_asset_status_after_import", _boom)

    with pytest.raises(
        st.SnapshotTransferError, match="data_asset_status refresh failed"
    ):
        st.run_import(db=db, snapshot_dir=snapshot_dir)

    # the data was applied and the import state recorded before the refresh ran
    cols["stock_daily_quote"].bulk_write.assert_called()
    cols[st.SNAPSHOT_IMPORT_STATE_COLLECTION].replace_one.assert_called()


def test_refresh_asset_status_after_import_reuses_the_initializer(monkeypatch):
    from app.jobs import data_asset_status_initializer

    run = MagicMock(
        return_value={
            "asset_count": 5,
            "written_count": 5,
            "status_counts": {"OK": 5},
        }
    )
    monkeypatch.setattr(
        data_asset_status_initializer,
        "_load_default_initializer",
        lambda: MagicMock(run=run),
    )

    summary = st.refresh_asset_status_after_import(["stock_daily_quote"])

    run.assert_called_once()
    assert summary["refreshed_via"] == "data_asset_status_initializer.recompute"
    assert summary["collections_imported"] == ["stock_daily_quote"]
    assert summary["asset_count"] == 5


# ---------------------------------------------------------------------------
# Snapshot directory resolution
# ---------------------------------------------------------------------------


def _age_manifest(snapshot_dir: Path, when: datetime.datetime) -> None:
    stamp = when.timestamp()
    os.utime(snapshot_dir / st.MANIFEST_NAME, (stamp, stamp))


def test_resolve_snapshot_dir_latest_picks_newest_manifest(tmp_path):
    old_dir, _ = _write_snapshot(
        tmp_path, {"stock_industry": [_industry_doc()]}, snapshot_id="snapshot-old"
    )
    new_dir, _ = _write_snapshot(
        tmp_path, {"stock_industry": [_industry_doc()]}, snapshot_id="snapshot-new"
    )
    _age_manifest(old_dir, datetime.datetime(2026, 9, 1, tzinfo=UTC))
    _age_manifest(new_dir, datetime.datetime(2026, 9, 5, tzinfo=UTC))

    assert st.resolve_snapshot_dir(tmp_path, "latest") == new_dir
    assert st.resolve_snapshot_dir(tmp_path, "snapshot-old") == old_dir
    assert st.resolve_snapshot_dir(tmp_path, None) == new_dir


def test_resolve_snapshot_dir_latest_without_any_snapshot_raises(tmp_path):
    with pytest.raises(st.SnapshotTransferError, match="no snapshot"):
        st.resolve_snapshot_dir(tmp_path, "latest")


def test_default_snapshot_dir_env_override(monkeypatch):
    monkeypatch.delenv("SNAPSHOT_DIR", raising=False)
    assert st.default_snapshot_dir() == Path("/work/snapshot")
    monkeypatch.setenv("SNAPSHOT_DIR", "/tmp/snaps")
    assert st.default_snapshot_dir() == Path("/tmp/snaps")


def test_resolve_producer_image_env_and_hostname_fallback(monkeypatch):
    monkeypatch.setenv("IMAGE_SHA", "sha256:deadbeef")
    assert st.resolve_producer_image() == "sha256:deadbeef"
    monkeypatch.delenv("IMAGE_SHA", raising=False)
    assert st.resolve_producer_image() == socket.gethostname()


# ---------------------------------------------------------------------------
# Runners: argument parsing and wiring (no live MongoDB)
# ---------------------------------------------------------------------------


def test_snapshot_export_runner_without_command_prints_help(capsys):
    from app.jobs import snapshot_export_runner

    snapshot_export_runner.main([])
    assert "Snapshot Export Runner" in capsys.readouterr().out


def test_snapshot_import_runner_without_command_prints_help(capsys):
    from app.jobs import snapshot_import_runner

    snapshot_import_runner.main([])
    assert "Snapshot Import Runner" in capsys.readouterr().out


def test_snapshot_export_runner_passes_run_arguments_to_engine(monkeypatch, tmp_path):
    from app.jobs import snapshot_export_runner
    from app.lib.datahub import snapshot_transfer

    contexts = []
    engine_calls = []
    monkeypatch.setattr(
        snapshot_transfer, "_get_local_db", lambda cfg=None: MagicMock()
    )
    monkeypatch.setattr(
        snapshot_transfer,
        "run_export",
        lambda **kwargs: (
            engine_calls.append(kwargs)
            or {
                "status": "GOOD",
                "snapshot_id": "snapshot-x",
                "snapshot_dir": str(tmp_path / "snapshot-x"),
                "total_docs": 0,
                "collections_exported": 0,
                "collections": {},
            }
        ),
    )
    contexts = _patch_job_tracking(monkeypatch, snapshot_export_runner)

    snapshot_export_runner.main(
        [
            "run",
            "--collections",
            "quote,daily_basic",
            "--from-date",
            "2026-09-01",
            "--to-date",
            "2026-09-04",
            "--out-dir",
            str(tmp_path),
            "--dry-run",
        ]
    )

    assert len(engine_calls) == 1
    call = engine_calls[0]
    assert call["collections"] == ["quote", "daily_basic"]
    assert call["from_date"] == datetime.datetime(2026, 9, 1)
    assert call["to_date"] == datetime.datetime(2026, 9, 4)
    assert call["out_dir"] == tmp_path
    assert call["dry_run"] is True
    assert call["producer_image"]
    assert contexts[0].job_family == "snapshot_transfer"
    assert contexts[0].job_name == "datahub_snapshot_export"
    assert contexts[0].trigger == "cli"
    assert contexts[0].source == "cli"
    assert contexts[0].extra["dry_run"] is True
    assert contexts[0].extra["collections"] == "quote,daily_basic"


def test_snapshot_export_runner_defaults_to_snapshot_dir_env_and_image_sha(
    monkeypatch,
):
    from app.jobs import snapshot_export_runner
    from app.lib.datahub import snapshot_transfer

    engine_calls = []
    monkeypatch.setattr(
        snapshot_transfer, "_get_local_db", lambda cfg=None: MagicMock()
    )
    monkeypatch.setattr(
        snapshot_transfer,
        "run_export",
        lambda **kwargs: (
            engine_calls.append(kwargs)
            or {"status": "GOOD", "snapshot_id": "s", "snapshot_dir": "/x"}
        ),
    )
    _patch_job_tracking(monkeypatch, snapshot_export_runner)
    monkeypatch.setenv("SNAPSHOT_DIR", "/tmp/snapshot-base")
    monkeypatch.setenv("IMAGE_SHA", "sha256:deadbeef")

    snapshot_export_runner.main(["run"])

    assert len(engine_calls) == 1
    assert engine_calls[0]["out_dir"] == Path("/tmp/snapshot-base")
    assert engine_calls[0]["collections"] is None
    assert engine_calls[0]["producer_image"] == "sha256:deadbeef"


def test_snapshot_import_runner_resolves_latest_snapshot_dir(monkeypatch, tmp_path):
    from app.jobs import snapshot_import_runner
    from app.lib.datahub import snapshot_transfer

    old_dir, _ = _write_snapshot(
        tmp_path, {"stock_industry": [_industry_doc()]}, snapshot_id="snapshot-old"
    )
    new_dir, _ = _write_snapshot(
        tmp_path, {"stock_industry": [_industry_doc()]}, snapshot_id="snapshot-new"
    )
    _age_manifest(old_dir, datetime.datetime(2026, 9, 1, tzinfo=UTC))
    _age_manifest(new_dir, datetime.datetime(2026, 9, 5, tzinfo=UTC))

    contexts = []
    engine_calls = []
    monkeypatch.setattr(
        snapshot_transfer, "_get_local_db", lambda cfg=None: MagicMock()
    )
    monkeypatch.setattr(
        snapshot_transfer,
        "run_import",
        lambda **kwargs: (
            engine_calls.append(kwargs)
            or {
                "status": "GOOD",
                "snapshot_id": "snapshot-new",
                "snapshot_dir": str(new_dir),
            }
        ),
    )
    contexts = _patch_job_tracking(monkeypatch, snapshot_import_runner)

    snapshot_import_runner.main(["run", "--snapshot-dir", str(tmp_path)])

    assert len(engine_calls) == 1
    assert engine_calls[0]["snapshot_dir"] == new_dir
    assert engine_calls[0]["dry_run"] is False
    assert engine_calls[0]["producer_image_note"]
    assert contexts[0].job_name == "datahub_snapshot_import"
    assert contexts[0].job_family == "snapshot_transfer"
    assert contexts[0].extra["snapshot_id"] == "latest"


def test_snapshot_import_runner_explicit_snapshot_id_and_dry_run(monkeypatch, tmp_path):
    from app.jobs import snapshot_import_runner
    from app.lib.datahub import snapshot_transfer

    old_dir, _ = _write_snapshot(
        tmp_path, {"stock_industry": [_industry_doc()]}, snapshot_id="snapshot-old"
    )
    engine_calls = []
    monkeypatch.setattr(
        snapshot_transfer, "_get_local_db", lambda cfg=None: MagicMock()
    )
    monkeypatch.setattr(
        snapshot_transfer,
        "run_import",
        lambda **kwargs: (
            engine_calls.append(kwargs)
            or {
                "status": "DRY_RUN",
                "snapshot_id": "snapshot-old",
                "snapshot_dir": str(old_dir),
            }
        ),
    )
    _patch_job_tracking(monkeypatch, snapshot_import_runner)

    snapshot_import_runner.main(
        [
            "run",
            "--snapshot-dir",
            str(tmp_path),
            "--snapshot-id",
            "snapshot-old",
            "--dry-run",
        ]
    )

    assert engine_calls[0]["snapshot_dir"] == old_dir
    assert engine_calls[0]["dry_run"] is True


def test_snapshot_import_runner_sigterm_handler_marks_run_failed(monkeypatch):
    from app.jobs import snapshot_import_runner

    finished = []
    job_run = object()
    monkeypatch.setattr(
        snapshot_import_runner.job_run_helper,
        "finish_job_run",
        lambda *args, **kwargs: finished.append((args, kwargs)),
    )

    handler = snapshot_import_runner._make_termination_handler(job_run)
    with pytest.raises(SystemExit) as exc_info:
        handler(snapshot_import_runner.signal.SIGTERM, None)

    assert exc_info.value.code == 128 + snapshot_import_runner.signal.SIGTERM
    assert finished[0][1]["status"] == "FAILED"
    assert "SIGTERM" in finished[0][1]["error_message"]


def test_snapshot_export_runner_sigterm_handler_marks_run_failed(monkeypatch):
    from app.jobs import snapshot_export_runner

    finished = []
    monkeypatch.setattr(
        snapshot_export_runner.job_run_helper,
        "finish_job_run",
        lambda *args, **kwargs: finished.append((args, kwargs)),
    )

    handler = snapshot_export_runner._make_termination_handler(object())
    with pytest.raises(SystemExit) as exc_info:
        handler(snapshot_export_runner.signal.SIGTERM, None)

    assert exc_info.value.code == 128 + snapshot_export_runner.signal.SIGTERM
    assert finished[0][1]["summary"] == {"failed_phase": "snapshot_export"}


def test_snapshot_import_runner_records_failed_run_for_engine_error(monkeypatch):
    """R1: an allow-list rejection is visible as a FAILED job-run record."""
    from app.jobs import snapshot_import_runner
    from app.lib.datahub import snapshot_transfer

    monkeypatch.setattr(
        snapshot_transfer, "_get_local_db", lambda cfg=None: MagicMock()
    )

    def _boom(**_kwargs):
        raise st.SnapshotTransferError(
            "snapshot manifest proposes collections outside the dev import "
            "allow-list: ['user_credentials']; allowed: [...]"
        )

    monkeypatch.setattr(snapshot_transfer, "run_import", _boom)
    _patch_job_tracking(monkeypatch, snapshot_import_runner)

    finished = []
    monkeypatch.setattr(
        snapshot_import_runner.job_run_helper,
        "finish_job_run",
        lambda job_run, **kwargs: finished.append(kwargs),
    )

    with pytest.raises(st.SnapshotTransferError):
        snapshot_import_runner.main(
            [
                "run",
                "--snapshot-dir",
                "/tmp/does-not-matter",
                "--snapshot-id",
                "my-snapshot",
            ]
        )

    assert len(finished) == 1
    assert finished[0]["status"] == "FAILED"
    assert "user_credentials" in finished[0]["error_message"]


def test_snapshot_id_rejects_path_traversal():
    with pytest.raises(st.SnapshotTransferError, match="plain directory name"):
        st.resolve_snapshot_dir(Path("/tmp/snap"), "../escape")
    with pytest.raises(st.SnapshotTransferError, match="plain directory name"):
        st.resolve_snapshot_dir(Path("/tmp/snap"), "sub/dir")


def test_decode_bson_json_wraps_corrupt_markers_as_snapshot_transfer_error():
    with pytest.raises(st.SnapshotTransferError, match="undecodable"):
        st.decode_bson_json('{"__bson_oid": "not-a-hex-oid"}')
    with pytest.raises(st.SnapshotTransferError, match="undecodable"):
        st.decode_bson_json('{"__bson_date": "not-a-number"}')


# ---------------------------------------------------------------------------
# Optional S3 object-storage transport
# ---------------------------------------------------------------------------


def _clear_s3_env(monkeypatch):
    """Keep endpoint/region defaults deterministic regardless of ambient env."""
    for name in (
        "DATA_LAKE_ENDPOINT_URL",
        "DATA_LAKE_REGION",
        "AWS_ENDPOINT_URL",
        "AWS_DEFAULT_REGION",
    ):
        monkeypatch.delenv(name, raising=False)


class _FakeS3Client:
    """MagicMock-based S3 client stub serving/recording object transfers."""

    def __init__(
        self,
        *,
        manifest_bytes: bytes = b"{}",
        sidecar_text: str = "",
        data_files: dict[str, bytes] | None = None,
        fail_uploads: tuple[str, ...] = (),
        fail_downloads: tuple[str, ...] = (),
    ):
        self.client = MagicMock(name="s3client")
        self.uploaded: list[tuple[str, str, str]] = []
        self.downloaded: list[tuple[str, str, str]] = []
        self._manifest_bytes = manifest_bytes
        self._sidecar_text = sidecar_text
        self._data_files = data_files or {}
        self._fail_uploads = set(fail_uploads)
        self._fail_downloads = set(fail_downloads)
        self.client.upload_file.side_effect = self._upload
        self.client.download_file.side_effect = self._download

    def _upload(self, local_path, bucket, key):
        if key in self._fail_uploads:
            raise RuntimeError(f"simulated upload failure for {key}")
        self.uploaded.append((local_path, bucket, key))

    def _download(self, bucket, key, local_path):
        if key in self._fail_downloads:
            raise RuntimeError(f"simulated download failure for {key}")
        name = key.rpartition("/")[2]
        if name == st.MANIFEST_NAME:
            payload = self._manifest_bytes
        elif name == st.MANIFEST_CHECKSUM_NAME:
            payload = self._sidecar_text.encode("utf-8")
        else:
            if name not in self._data_files:
                raise RuntimeError(f"unexpected object requested: {key}")
            payload = self._data_files[name]
        Path(local_path).write_bytes(payload)
        self.downloaded.append((bucket, key, local_path))


def _patch_s3_client(monkeypatch, fake: _FakeS3Client) -> list[tuple[str, dict]]:
    """Replace boto3.client (the lazy import used by the engine) with a stub."""
    import boto3

    constructed: list[tuple[str, dict]] = []

    def _client(service, **kwargs):
        constructed.append((service, kwargs))
        return fake.client

    monkeypatch.setattr(boto3, "client", _client)
    return constructed


def _snapshot_payload_bytes(snapshot_dir: Path) -> tuple[bytes, str, dict[str, bytes]]:
    """Extract manifest bytes, sidecar text and data file bytes from a snapshot."""
    manifest_bytes = (snapshot_dir / st.MANIFEST_NAME).read_bytes()
    sidecar = (snapshot_dir / st.MANIFEST_CHECKSUM_NAME).read_text(encoding="utf-8")
    manifest = json.loads(manifest_bytes.decode("utf-8"))
    data_files = {
        entry["file"]: (snapshot_dir / entry["file"]).read_bytes()
        for entry in manifest["collections"]
    }
    return manifest_bytes, sidecar, data_files


def test_parse_s3_uri_accepts_bucket_and_prefix_variants():
    assert st.parse_s3_uri("s3://bucket/prefix/deeper") == ("bucket", "prefix/deeper")
    assert st.parse_s3_uri("s3://bucket/snapshot-x") == ("bucket", "snapshot-x")
    # the prefix may be empty; leading/trailing slashes normalize away
    assert st.parse_s3_uri("s3://bucket") == ("bucket", "")
    assert st.parse_s3_uri("s3://bucket/") == ("bucket", "")
    assert st.parse_s3_uri("s3://bucket//slashed//") == ("bucket", "slashed")


def test_parse_s3_uri_rejects_degenerate_values():
    for bad in (
        "s3://",
        "s3:///only-prefix",
        "http://bucket/key",
        "bucket/key",
        "s3://bucket with space/key",
        "s3://../escape",
        "",
    ):
        with pytest.raises(st.SnapshotTransferError, match="invalid S3 URI"):
            st.parse_s3_uri(bad)


def test_download_dir_for_uri_derives_base_downloads_basename(tmp_path):
    assert (
        st.download_dir_for_uri(tmp_path, "s3://b/snapshots/snapshot-x")
        == tmp_path / "downloads" / "snapshot-x"
    )
    assert st.download_dir_for_uri(tmp_path, "s3://b/snapshot-x") == (
        tmp_path / "downloads" / "snapshot-x"
    )
    # bucket-root URIs fall back to the bucket name
    assert st.download_dir_for_uri(tmp_path, "s3://b") == tmp_path / "downloads" / "b"
    with pytest.raises(st.SnapshotTransferError, match="safe local download directory"):
        st.download_dir_for_uri(tmp_path, "s3://b/snapshots/..")


def test_upload_snapshot_uploads_manifest_sidecar_then_data_files(
    tmp_path, monkeypatch
):
    _clear_s3_env(monkeypatch)
    snapshot_dir, _sha = _write_snapshot(
        tmp_path,
        {"stock_daily_quote": [_quote_doc()], "stock_industry": [_industry_doc()]},
    )
    fake = _FakeS3Client()
    constructed = _patch_s3_client(monkeypatch, fake)

    result = st.upload_snapshot(snapshot_dir, "s3://snap-bucket/prefix/snapshots")

    assert len(constructed) == 1
    service, kwargs = constructed[0]
    assert service == "s3"
    assert kwargs["endpoint_url"] is None
    assert kwargs["region_name"] is None

    expected_files = [
        st.MANIFEST_NAME,
        st.MANIFEST_CHECKSUM_NAME,
        "stock_daily_quote.jsonl.gz",
        "stock_industry.jsonl.gz",
    ]
    expected_keys = [f"prefix/snapshots/{name}" for name in expected_files]
    assert [key for _path, _bucket, key in fake.uploaded] == expected_keys
    # every local file name maps onto its derived object key
    assert {path for path, _bucket, _key in fake.uploaded} == {
        str(snapshot_dir / name) for name in expected_files
    }
    assert result == {
        "bucket": "snap-bucket",
        "prefix": "prefix/snapshots",
        "objects": expected_keys,
    }


def test_upload_snapshot_empty_prefix_uses_bare_object_keys(tmp_path, monkeypatch):
    _clear_s3_env(monkeypatch)
    snapshot_dir, _sha = _write_snapshot(
        tmp_path, {"stock_daily_quote": [_quote_doc()]}
    )
    fake = _FakeS3Client()
    _patch_s3_client(monkeypatch, fake)

    result = st.upload_snapshot(snapshot_dir, "s3://snap-bucket/")

    assert result["prefix"] == ""
    assert [key for _path, _bucket, key in fake.uploaded] == [
        st.MANIFEST_NAME,
        st.MANIFEST_CHECKSUM_NAME,
        "stock_daily_quote.jsonl.gz",
    ]


def test_upload_snapshot_defaults_endpoint_and_region_from_env(tmp_path, monkeypatch):
    snapshot_dir, _sha = _write_snapshot(
        tmp_path, {"stock_daily_quote": [_quote_doc()]}
    )
    fake = _FakeS3Client()
    constructed = _patch_s3_client(monkeypatch, fake)
    monkeypatch.setenv("DATA_LAKE_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("DATA_LAKE_REGION", "cn-north-1")

    st.upload_snapshot(snapshot_dir, "s3://b/p")

    assert constructed[0][1]["endpoint_url"] == "http://minio:9000"
    assert constructed[0][1]["region_name"] == "cn-north-1"

    # explicit arguments override the env defaults
    st.upload_snapshot(
        snapshot_dir,
        "s3://b/p",
        endpoint_url="http://other:9000",
        region_name="us-east-1",
    )
    assert constructed[1][1]["endpoint_url"] == "http://other:9000"
    assert constructed[1][1]["region_name"] == "us-east-1"


def test_upload_snapshot_refuses_a_directory_without_a_manifest(tmp_path, monkeypatch):
    _clear_s3_env(monkeypatch)
    fake = _FakeS3Client()
    constructed = _patch_s3_client(monkeypatch, fake)
    empty = tmp_path / "empty"
    empty.mkdir()

    with pytest.raises(st.SnapshotTransferError, match="manifest not found"):
        st.upload_snapshot(empty, "s3://b/p")

    assert constructed == []
    assert fake.uploaded == []


def test_upload_snapshot_failure_names_the_key_and_keeps_local_files(
    tmp_path, monkeypatch
):
    snapshot_dir, _sha = _write_snapshot(
        tmp_path, {"stock_daily_quote": [_quote_doc()]}
    )
    fake = _FakeS3Client(fail_uploads={"p/stock_daily_quote.jsonl.gz"})
    _patch_s3_client(monkeypatch, fake)

    with pytest.raises(
        st.SnapshotTransferError,
        match="s3://snap-bucket/p/stock_daily_quote.jsonl.gz",
    ):
        st.upload_snapshot(snapshot_dir, "s3://snap-bucket/p")

    # fail-closed: manifest+sidecar uploaded, then the transfer aborted, and
    # every local file is still on disk for the retry (nothing is deleted)
    assert [key for _path, _bucket, key in fake.uploaded] == [
        "p/manifest.json",
        "p/manifest.json.sha256",
    ]
    assert (snapshot_dir / st.MANIFEST_NAME).is_file()
    assert (snapshot_dir / st.MANIFEST_CHECKSUM_NAME).is_file()
    assert (snapshot_dir / "stock_daily_quote.jsonl.gz").is_file()


def test_download_snapshot_fetches_manifest_pair_first_then_data_files(
    tmp_path, monkeypatch
):
    _clear_s3_env(monkeypatch)
    source_dir, _ = _write_snapshot(tmp_path, {"stock_daily_quote": [_quote_doc()]})
    manifest_bytes, sidecar, data_files = _snapshot_payload_bytes(source_dir)
    fake = _FakeS3Client(
        manifest_bytes=manifest_bytes, sidecar_text=sidecar, data_files=data_files
    )
    _patch_s3_client(monkeypatch, fake)
    dest = tmp_path / "out" / "downloads" / "snapshot-test00000000"

    result = st.download_snapshot("s3://snap-bucket/prefix/snapshot-test00000000", dest)

    assert result == dest
    assert [key for _bucket, key, _path in fake.downloaded] == [
        "prefix/snapshot-test00000000/manifest.json",
        "prefix/snapshot-test00000000/manifest.json.sha256",
        "prefix/snapshot-test00000000/stock_daily_quote.jsonl.gz",
    ]
    assert (dest / st.MANIFEST_NAME).read_bytes() == manifest_bytes
    assert (dest / st.MANIFEST_CHECKSUM_NAME).read_text(encoding="utf-8") == sidecar
    assert (dest / "stock_daily_quote.jsonl.gz").read_bytes() == (
        data_files["stock_daily_quote.jsonl.gz"]
    )


def test_download_snapshot_sidecar_mismatch_fails_closed_without_data_files(
    tmp_path, monkeypatch
):
    source_dir, _ = _write_snapshot(
        tmp_path,
        {"stock_daily_quote": [_quote_doc()], "stock_industry": [_industry_doc()]},
    )
    manifest_bytes, _sidecar, data_files = _snapshot_payload_bytes(source_dir)
    bad_sidecar = f"{'0' * 64}  {st.MANIFEST_NAME}\n"
    fake = _FakeS3Client(
        manifest_bytes=manifest_bytes, sidecar_text=bad_sidecar, data_files=data_files
    )
    _patch_s3_client(monkeypatch, fake)
    dest = tmp_path / "downloads" / "snapshot-test00000000"

    with pytest.raises(st.SnapshotTransferError, match="manifest checksum mismatch"):
        st.download_snapshot("s3://snap-bucket/snaps/snapshot-test00000000", dest)

    # fail-closed: the untrusted manifest pair is removed and NO data file landed
    assert list(dest.iterdir()) == []


def test_download_snapshot_propagates_object_failures_as_transfer_error(
    tmp_path, monkeypatch
):
    source_dir, _ = _write_snapshot(tmp_path, {"stock_daily_quote": [_quote_doc()]})
    manifest_bytes, sidecar, data_files = _snapshot_payload_bytes(source_dir)
    fake = _FakeS3Client(
        manifest_bytes=manifest_bytes,
        sidecar_text=sidecar,
        data_files=data_files,
        fail_downloads={"snaps/stock_daily_quote.jsonl.gz"},
    )
    _patch_s3_client(monkeypatch, fake)
    dest = tmp_path / "downloads" / "snap"

    with pytest.raises(
        st.SnapshotTransferError,
        match="s3://snap-bucket/snaps/stock_daily_quote.jsonl.gz",
    ):
        st.download_snapshot("s3://snap-bucket/snaps", dest)


def test_upload_then_download_round_trip_preserves_manifest_and_files(
    tmp_path, monkeypatch
):
    _clear_s3_env(monkeypatch)
    snapshot_dir, manifest_sha = _write_snapshot(
        tmp_path / "src",
        {"stock_daily_quote": [_quote_doc(), _quote_doc(code="sz000002")]},
    )

    objects: dict[str, bytes] = {}

    def _upload(local_path, _bucket, key):
        objects[key] = Path(local_path).read_bytes()

    def _download(_bucket, key, local_path):
        Path(local_path).write_bytes(objects[key])

    import boto3

    client = MagicMock(name="s3client")
    client.upload_file.side_effect = _upload
    client.download_file.side_effect = _download
    monkeypatch.setattr(boto3, "client", lambda service, **kwargs: client)

    st.upload_snapshot(snapshot_dir, "s3://snap-bucket/snaps")
    dest = tmp_path / "dst"
    st.download_snapshot("s3://snap-bucket/snaps", dest)

    assert (dest / st.MANIFEST_NAME).read_bytes() == (
        snapshot_dir / st.MANIFEST_NAME
    ).read_bytes()
    assert manifest_sha in (dest / st.MANIFEST_CHECKSUM_NAME).read_text(
        encoding="utf-8"
    )
    assert sorted(p.name for p in dest.iterdir()) == [
        "manifest.json",
        "manifest.json.sha256",
        "stock_daily_quote.jsonl.gz",
    ]
    # the round-tripped snapshot still passes the importer's full pass-1 verify
    db, _cols = _import_db()
    summary = st.run_import(db=db, snapshot_dir=dest, dry_run=True)
    assert summary["status"] == "DRY_RUN"
    assert summary["manifest_sha256"] == manifest_sha


def test_snapshot_export_runner_uploads_when_upload_uri_given(monkeypatch, tmp_path):
    from app.jobs import snapshot_export_runner
    from app.lib.datahub import snapshot_transfer

    engine_calls = []
    upload_calls = []
    finished = []
    snapshot_dir = tmp_path / "snapshot-x"

    def _fake_run_export(**kwargs):
        engine_calls.append(kwargs)
        return {
            "status": "GOOD",
            "dry_run": False,
            "snapshot_id": "snapshot-x",
            "snapshot_dir": str(snapshot_dir),
            "manifest_sha256": "a" * 64,
            "total_docs": 1,
            "collections_exported": 1,
            "collections": {},
        }

    def _fake_upload(snapshot_dir_arg, uri, **kwargs):
        upload_calls.append((snapshot_dir_arg, uri, kwargs))
        return {
            "bucket": "snap-bucket",
            "prefix": "prefix",
            "objects": ["prefix/manifest.json"],
        }

    monkeypatch.setattr(
        snapshot_transfer, "_get_local_db", lambda cfg=None: MagicMock()
    )
    monkeypatch.setattr(snapshot_transfer, "run_export", _fake_run_export)
    monkeypatch.setattr(snapshot_transfer, "upload_snapshot", _fake_upload)
    contexts = _patch_job_tracking(monkeypatch, snapshot_export_runner)
    monkeypatch.setattr(
        snapshot_export_runner.job_run_helper,
        "finish_job_run",
        lambda job_run, **kwargs: finished.append(kwargs),
    )

    snapshot_export_runner.main(
        ["run", "--out-dir", str(tmp_path), "--upload-uri", "s3://snap-bucket/prefix"]
    )

    assert upload_calls == [(snapshot_dir, "s3://snap-bucket/prefix", {})]
    assert finished[0]["status"] == "SUCCESS"
    assert finished[0]["summary"]["upload"] == {
        "bucket": "snap-bucket",
        "prefix": "prefix",
        "objects": ["prefix/manifest.json"],
    }
    assert contexts[0].extra["upload_uri"] == "s3://snap-bucket/prefix"


def test_snapshot_export_runner_dry_run_does_not_upload(monkeypatch, tmp_path):
    from app.jobs import snapshot_export_runner
    from app.lib.datahub import snapshot_transfer

    upload_calls = []
    monkeypatch.setattr(
        snapshot_transfer, "_get_local_db", lambda cfg=None: MagicMock()
    )
    monkeypatch.setattr(
        snapshot_transfer,
        "run_export",
        lambda **kwargs: {
            "status": "DRY_RUN",
            "dry_run": True,
            "snapshot_id": "snapshot-x",
            "snapshot_dir": str(tmp_path / "snapshot-x"),
            "total_docs": 0,
            "collections_exported": 0,
            "collections": {},
        },
    )
    monkeypatch.setattr(
        snapshot_transfer,
        "upload_snapshot",
        lambda *args, **kwargs: upload_calls.append((args, kwargs)),
    )
    _patch_job_tracking(monkeypatch, snapshot_export_runner)

    snapshot_export_runner.main(
        [
            "run",
            "--out-dir",
            str(tmp_path),
            "--upload-uri",
            "s3://snap-bucket/prefix",
            "--dry-run",
        ]
    )

    assert upload_calls == []


def test_snapshot_export_runner_upload_failure_marks_run_failed(monkeypatch, tmp_path):
    from app.jobs import snapshot_export_runner
    from app.lib.datahub import snapshot_transfer

    monkeypatch.setattr(
        snapshot_transfer, "_get_local_db", lambda cfg=None: MagicMock()
    )
    monkeypatch.setattr(
        snapshot_transfer,
        "run_export",
        lambda **kwargs: {
            "status": "GOOD",
            "dry_run": False,
            "snapshot_id": "snapshot-x",
            "snapshot_dir": str(tmp_path / "snapshot-x"),
        },
    )

    def _boom(_snapshot_dir, _uri):
        raise st.SnapshotTransferError(
            "snapshot upload failed for s3://snap-bucket/p/manifest.json: boom"
        )

    monkeypatch.setattr(snapshot_transfer, "upload_snapshot", _boom)
    _patch_job_tracking(monkeypatch, snapshot_export_runner)
    finished = []
    monkeypatch.setattr(
        snapshot_export_runner.job_run_helper,
        "finish_job_run",
        lambda job_run, **kwargs: finished.append(kwargs),
    )

    with pytest.raises(st.SnapshotTransferError, match="snapshot upload failed"):
        snapshot_export_runner.main(
            ["run", "--out-dir", str(tmp_path), "--upload-uri", "s3://snap-bucket/p"]
        )

    assert finished[0]["status"] == "FAILED"


def test_snapshot_import_runner_snapshot_uri_downloads_then_imports(
    monkeypatch, tmp_path
):
    from app.jobs import snapshot_import_runner
    from app.lib.datahub import snapshot_transfer

    download_calls = []
    engine_calls = []

    def _fake_download(uri, dest_dir, **kwargs):
        download_calls.append((uri, dest_dir, kwargs))
        dest_dir.mkdir(parents=True, exist_ok=True)
        (dest_dir / st.MANIFEST_NAME).write_bytes(b"{}")

    monkeypatch.setattr(
        snapshot_transfer, "_get_local_db", lambda cfg=None: MagicMock()
    )
    monkeypatch.setattr(snapshot_transfer, "download_snapshot", _fake_download)
    monkeypatch.setattr(
        snapshot_transfer,
        "run_import",
        lambda **kwargs: (
            engine_calls.append(kwargs)
            or {
                "status": "GOOD",
                "snapshot_id": "snapshot-x",
                "snapshot_dir": str(kwargs["snapshot_dir"]),
            }
        ),
    )
    contexts = _patch_job_tracking(monkeypatch, snapshot_import_runner)
    monkeypatch.setenv("SNAPSHOT_DIR", str(tmp_path / "snaps"))

    snapshot_import_runner.main(
        ["run", "--snapshot-uri", "s3://snap-bucket/snapshots/snapshot-x"]
    )

    expected_dest = tmp_path / "snaps" / "downloads" / "snapshot-x"
    assert download_calls == [
        ("s3://snap-bucket/snapshots/snapshot-x", expected_dest, {})
    ]
    assert engine_calls[0]["snapshot_dir"] == expected_dest
    assert contexts[0].extra["snapshot_uri"] == "s3://snap-bucket/snapshots/snapshot-x"


def test_snapshot_import_runner_prefers_snapshot_dir_over_snapshot_uri(
    monkeypatch, tmp_path
):
    from app.jobs import snapshot_import_runner
    from app.lib.datahub import snapshot_transfer

    old_dir, _ = _write_snapshot(
        tmp_path, {"stock_industry": [_industry_doc()]}, snapshot_id="snapshot-old"
    )
    download_calls = []
    engine_calls = []
    monkeypatch.setattr(
        snapshot_transfer, "_get_local_db", lambda cfg=None: MagicMock()
    )
    monkeypatch.setattr(
        snapshot_transfer,
        "download_snapshot",
        lambda *args, **kwargs: download_calls.append((args, kwargs)),
    )
    monkeypatch.setattr(
        snapshot_transfer,
        "run_import",
        lambda **kwargs: (
            engine_calls.append(kwargs)
            or {
                "status": "GOOD",
                "snapshot_id": "snapshot-old",
                "snapshot_dir": str(old_dir),
            }
        ),
    )
    _patch_job_tracking(monkeypatch, snapshot_import_runner)

    snapshot_import_runner.main(
        [
            "run",
            "--snapshot-dir",
            str(tmp_path),
            "--snapshot-id",
            "snapshot-old",
            "--snapshot-uri",
            "s3://snap-bucket/snapshots/snapshot-old",
        ]
    )

    assert download_calls == []
    assert engine_calls[0]["snapshot_dir"] == old_dir
