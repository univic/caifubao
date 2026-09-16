# -*- coding: utf-8 -*-
"""Controlled snapshot export/import for the dev data plane.

Implements the tooling side of the ``dev-snapshot-import`` contract: a
research/data deployment exports a checksummed snapshot, and dev imports it
fail-closed without ever holding an online MongoDB credential for the
producing environment.

Snapshot layout (``<out_dir>/<snapshot_id>/``)::

    manifest.json            manifest v1 (SNAPSHOT_MANIFEST_VERSION)
    manifest.json.sha256     sha256 of manifest.json, ``sha256sum`` format
    <collection>.jsonl.gz    newline-delimited JSON, BSON-safe encoding

Manifest v1 schema::

    {
      "manifest_version": 1,
      "snapshot_id": "<dir name>",
      "created_at": "<ISO8601 UTC>",
      "producer_db": "<exporting database name>",
      "producer_image": "<image SHA or hostname>",
      "collections": [
        {
          "name": "stock_daily_quote",
          "file": "stock_daily_quote.jsonl.gz",
          "sha256": "<hex of the .jsonl.gz file>",
          "doc_count": 1234,
          "data_as_of": "<max date ISO8601>" | null,
          "upsert_keys": ["code", "date"] | null,
          "class": "date_partitioned" | "snapshot"
        },
        ...
      ]
    }

BSON-safe JSON line encoding (strict, fail-closed):

* ``datetime.datetime`` -> ``{"__bson_date": <epoch milliseconds>}``.
  Naive datetimes are treated as UTC (pymongo's default representation) and
  decode back to naive UTC datetimes.
* ``bson.ObjectId`` -> ``{"__bson_oid": "<24-char hex>"}``.
* ``bson.decimal128.Decimal128`` -> plain string (the one documented lossy
  case: it decodes back as ``str``).
* every other non-JSON-native BSON type (bytes, ``datetime.date``, ...) makes
  the encoder raise ``SnapshotTransferError`` — export never writes a
  snapshot it could not restore faithfully.

Collection classes and idempotency semantics:

* ``date_partitioned`` (``stock_daily_quote``, ``stock_daily_basic``,
  ``stock_factor_daily``, ``stock_signal_daily``): upsert by the same
  business keys the online sync used (``SYNC_UPSERT_KEYS``) with bulk
  ``ReplaceOne(..., upsert=True)`` batches, so re-importing the same
  snapshot neither duplicates documents nor changes business-key identity.
* ``snapshot`` (``finance_market``, ``stock_industry``): state-equivalent
  replacement. Documents are restored into ``<name>__snapshot_staging``,
  the staged count is verified against the manifest, and only then is the
  target atomically replaced via ``renameCollection(..., dropTarget=True)``.
  Dev-side rows absent from the snapshot are deleted with the target. For
  ``stock_industry`` this deliberately supersedes the online sync's
  upsert-by-``stock_code`` behaviour.
* ``snapshot_import_state`` (dev-side import traceability record, replacing
  ``data_sync_state``) is on the import allow-list as a snapshot-class
  collection but is never exported.

Usage:
    python -m app.jobs.snapshot_export_runner run
    python -m app.jobs.snapshot_import_runner run
"""

import calendar
import datetime
import gzip
import hashlib
import io
import json
import logging
import os
import re
import shutil
import socket
import uuid
from pathlib import Path
from typing import Any

from bson import ObjectId
from bson.decimal128 import Decimal128
from bson.errors import BSONError
from pymongo import ReplaceOne
from pymongo.collection import Collection as MongoCollection
from pymongo.database import Database as MongoDatabase

from app.lib.datahub.sync_engine import (
    BATCH_SIZE as SYNC_BATCH_SIZE,
    COLLECTION_ALIASES,
    SYNC_UPSERT_KEYS,
    SYNCABLE_COLLECTIONS,
)

logger = logging.getLogger(__name__)


class SnapshotTransferError(ValueError):
    """Fail-closed snapshot export/import failure.

    Raised for manifest, allow-list, checksum, document-count or encoding
    mismatches; the message names the collection/file/reason and the runner
    records it as the job-run error.
    """


SNAPSHOT_MANIFEST_VERSION = 1
SNAPSHOT_IMPORT_STATE_COLLECTION = "snapshot_import_state"

# Snapshot-class collections use explicit replace/drop semantics instead of
# business-key upserts (see module docstring).
SNAPSHOT_SNAPSHOT_CLASS_COLLECTIONS = {"finance_market", "stock_industry"}

# Import allow-list: exactly the online sync surface plus the dev-side
# import-state metadata collection that replaces data_sync_state. Anything
# else is rejected before any data is applied.
SNAPSHOT_IMPORT_ALLOWED_COLLECTIONS = frozenset(SYNCABLE_COLLECTIONS) | {
    SNAPSHOT_IMPORT_STATE_COLLECTION,
}

# Export allow-list: the online sync surface. The import-state collection is
# dev-side metadata and never leaves the exporting deployment.
SNAPSHOT_EXPORT_COLLECTIONS = frozenset(SYNCABLE_COLLECTIONS)

BATCH_SIZE = SYNC_BATCH_SIZE

MANIFEST_NAME = "manifest.json"
MANIFEST_CHECKSUM_NAME = "manifest.json.sha256"
SNAPSHOT_FILE_SUFFIX = ".jsonl.gz"
STAGING_SUFFIX = "__snapshot_staging"
DEFAULT_SNAPSHOT_DIR = "/work/snapshot"
_MANIFEST_CHECKSUM_LINE_RE = re.compile(r"^([0-9a-f]{64})\s")
_DATA_FILE_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+\.jsonl\.gz$")


class _HashingWriter:
    """Binary file wrapper hashing compressed bytes as they are written."""

    def __init__(self, stream: Any) -> None:
        self._stream = stream
        self.hasher = hashlib.sha256()
        self.bytes_written = 0

    def write(self, data: bytes) -> int:
        self.hasher.update(data)
        self.bytes_written += len(data)
        return self._stream.write(data)

    def tell(self) -> int:
        return self._stream.tell()

    def flush(self) -> None:
        self._stream.flush()

    def close(self) -> None:
        self._stream.close()

    def writable(self) -> bool:
        return True

    def readable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False


class _HashingReader:
    """Binary file wrapper hashing compressed bytes as they are read."""

    def __init__(self, stream: Any) -> None:
        self._stream = stream
        self.hasher = hashlib.sha256()
        self.bytes_read = 0

    def read(self, size: int = -1) -> bytes:
        data = self._stream.read(size)
        self._absorb(data)
        return data

    def readinto(self, b: Any) -> int:
        data = self._stream.read(len(b))
        b[: len(data)] = data
        self._absorb(data)
        return len(data)

    def _absorb(self, data: bytes) -> None:
        self.hasher.update(data)
        self.bytes_read += len(data)

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def write(self, data: bytes) -> int:
        raise io.UnsupportedOperation("reader is read-only")

    def close(self) -> None:
        self._stream.close()


def _datetime_to_epoch_ms(value: datetime.datetime) -> int:
    if value.tzinfo is None:
        return calendar.timegm(value.utctimetuple()) * 1000 + value.microsecond // 1000
    return int(value.timestamp() * 1000)


def _epoch_ms_to_datetime(value: int | float) -> datetime.datetime:
    """Decode epoch milliseconds to a naive UTC datetime (pymongo default)."""
    epoch = datetime.datetime(1970, 1, 1)
    return epoch + datetime.timedelta(milliseconds=value)


def _as_naive_utc(value: datetime.datetime) -> datetime.datetime:
    """Normalize a datetime to naive UTC (pymongo's default representation)."""
    if value.tzinfo is None:
        return value
    return value.astimezone(datetime.timezone.utc).replace(tzinfo=None)


def _bson_json_default(obj: Any) -> Any:
    """json.dumps default() implementing the strict BSON-safe encoding."""
    if isinstance(obj, datetime.datetime):
        return {"__bson_date": _datetime_to_epoch_ms(obj)}
    if isinstance(obj, ObjectId):
        return {"__bson_oid": str(obj)}
    if isinstance(obj, Decimal128):
        return str(obj)
    raise SnapshotTransferError(
        "cannot encode BSON value of unsupported type "
        f"{type(obj).__name__!r} to the snapshot JSONL format: {obj!r}"
    )


def _bson_json_object_hook(obj: dict[str, Any]) -> Any:
    """json.loads object_hook() restoring the BSON-safe markers."""
    if len(obj) == 1:
        if "__bson_date" in obj:
            return _epoch_ms_to_datetime(obj["__bson_date"])
        if "__bson_oid" in obj:
            return ObjectId(obj["__bson_oid"])
    return obj


def encode_bson_json(obj: Any) -> str:
    """Encode one BSON document as a JSON line string (strict)."""
    return json.dumps(obj, default=_bson_json_default, ensure_ascii=False)


def decode_bson_json(line: str) -> Any:
    """Decode one JSON line restoring BSON-safe markers.

    Raises SnapshotTransferError (not raw json/bson errors) so callers keep a
    single fail-closed error contract.
    """
    try:
        return json.loads(line, object_hook=_bson_json_object_hook)
    except SnapshotTransferError:
        raise
    except (json.JSONDecodeError, ValueError, TypeError, BSONError) as exc:
        # includes bson.errors.BSONError (e.g. InvalidId for corrupt
        # __bson_oid markers) and TypeError/ValueError for non-numeric
        # __bson_date markers
        raise SnapshotTransferError(
            f"undecodable snapshot JSONL document ({type(exc).__name__}): "
            f"{line[:200]!r}"
        ) from exc


def _new_snapshot_id(now: datetime.datetime | None = None) -> str:
    now = now or datetime.datetime.now(datetime.timezone.utc)
    return f"snapshot-{now.strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


def default_snapshot_dir() -> Path:
    return Path(os.getenv("SNAPSHOT_DIR") or DEFAULT_SNAPSHOT_DIR)


def resolve_latest_snapshot_dir(base_dir: Path) -> Path:
    """Pick the snapshot directory whose manifest.json was written last."""
    base_dir = Path(base_dir)
    candidates = [
        path.parent for path in base_dir.glob(f"*/{MANIFEST_NAME}") if path.is_file()
    ]
    if not candidates:
        raise SnapshotTransferError(
            f"no snapshot with a {MANIFEST_NAME} found under {base_dir}"
        )
    return max(candidates, key=lambda path: (path / MANIFEST_NAME).stat().st_mtime)


def resolve_snapshot_dir(base_dir: Path, snapshot_id: str | None) -> Path:
    """Resolve a snapshot id (or 'latest') to its snapshot directory."""
    if snapshot_id in (None, "latest"):
        return resolve_latest_snapshot_dir(base_dir)
    if "/" in snapshot_id or "\\" in snapshot_id or snapshot_id in (".", ".."):
        raise SnapshotTransferError(
            f"invalid snapshot id {snapshot_id!r}: must be a plain directory "
            "name under the snapshot base dir"
        )
    return Path(base_dir) / snapshot_id


def resolve_producer_image() -> str:
    """Producer image SHA from IMAGE_SHA, falling back to the hostname."""
    return os.getenv("IMAGE_SHA") or socket.gethostname()


def _get_local_db(cfg: Any = None) -> MongoDatabase:
    """Return the local database handle via mongoengine's connection."""
    from mongoengine import get_connection

    if cfg is None:
        from app.conf import app_config as cfg

    conn = get_connection()
    return conn[cfg.MONGODB_DB]


def _collection_class(name: str) -> str:
    if name in SNAPSHOT_SNAPSHOT_CLASS_COLLECTIONS:
        return "snapshot"
    if name == SNAPSHOT_IMPORT_STATE_COLLECTION:
        return "snapshot"
    return "date_partitioned"


def _expected_upsert_keys(name: str) -> list[str] | None:
    if _collection_class(name) == "snapshot":
        return None
    return list(SYNC_UPSERT_KEYS.get(name, []))


def _date_field(name: str) -> str | None:
    return SYNCABLE_COLLECTIONS.get(name, {}).get("date_field")


def _resolve_export_collections(collections: list[str] | None) -> list[str]:
    """Resolve aliases against the export allow-list (fail-closed)."""
    selected = collections or list(SYNCABLE_COLLECTIONS.keys())
    resolved = list(
        dict.fromkeys(COLLECTION_ALIASES.get(c.strip(), c.strip()) for c in selected)
    )
    unknown = [name for name in resolved if name not in SNAPSHOT_EXPORT_COLLECTIONS]
    if unknown:
        raise SnapshotTransferError(
            "snapshot export refused unknown/non-exportable collections: "
            f"{sorted(unknown)}; allowed: {sorted(SNAPSHOT_EXPORT_COLLECTIONS)}"
        )
    return resolved


def _export_query(
    name: str,
    from_date: datetime.datetime | None,
    to_date: datetime.datetime | None,
) -> dict[str, Any]:
    """Build the export filter (mirrors sync_engine._iter_docs).

    Snapshot-class collections have no business date and are always fully
    exported regardless of any requested window.
    """
    query: dict[str, Any] = {}
    date_field = _date_field(name)
    if date_field and (from_date or to_date):
        date_filter: dict[str, datetime.datetime] = {}
        if from_date:
            date_filter["$gte"] = from_date
        if to_date:
            date_filter["$lte"] = to_date
        if date_filter:
            query[date_field] = date_filter
    return query


def _export_collection(
    db: MongoDatabase,
    name: str,
    snapshot_dir: Path,
    from_date: datetime.datetime | None,
    to_date: datetime.datetime | None,
) -> dict[str, Any]:
    """Stream one collection to <name>.jsonl.gz and return its manifest entry."""
    col: MongoCollection = db[name]
    date_field = _date_field(name)
    query = _export_query(name, from_date, to_date)
    sort = [(date_field, 1), ("_id", 1)] if date_field else [("_id", 1)]

    logger.info(
        "Exporting %s with filter=%s (date_field=%s, class=%s)",
        name,
        query,
        date_field,
        _collection_class(name),
    )

    file_name = f"{name}{SNAPSHOT_FILE_SUFFIX}"
    file_path = snapshot_dir / file_name
    doc_count = 0
    data_as_of: datetime.datetime | None = None

    with open(file_path, "wb") as raw:
        hashing = _HashingWriter(raw)
        with (
            gzip.GzipFile(fileobj=hashing, mode="wb", compresslevel=6) as gz,
            io.TextIOWrapper(gz, encoding="utf-8", newline="") as text,
        ):
            for doc in col.find(query).sort(sort):
                doc_count += 1
                if date_field:
                    value = doc.get(date_field)
                    if isinstance(value, datetime.datetime):
                        value = _as_naive_utc(value)
                        if data_as_of is None or value > data_as_of:
                            data_as_of = value
                text.write(encode_bson_json(doc) + "\n")

    entry = {
        "name": name,
        "file": file_name,
        "sha256": hashing.hasher.hexdigest(),
        "doc_count": doc_count,
        "data_as_of": data_as_of.isoformat() if data_as_of else None,
        "upsert_keys": _expected_upsert_keys(name),
        "class": _collection_class(name),
    }
    logger.info("Exported %s: doc_count=%d sha256=%s", name, doc_count, entry["sha256"])
    return entry


def _write_manifest(snapshot_dir: Path, manifest: dict[str, Any]) -> str:
    """Write manifest.json plus its sha256 checksum; returns the manifest sha."""
    payload = (
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    manifest_sha256 = hashlib.sha256(payload).hexdigest()
    (snapshot_dir / MANIFEST_NAME).write_bytes(payload)
    (snapshot_dir / MANIFEST_CHECKSUM_NAME).write_text(
        f"{manifest_sha256}  {MANIFEST_NAME}\n", encoding="utf-8"
    )
    return manifest_sha256


def run_export(
    db: MongoDatabase,
    out_dir: Path,
    collections: list[str] | None = None,
    from_date: datetime.datetime | None = None,
    to_date: datetime.datetime | None = None,
    producer_image: str = "",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Export allow-list collections into a checksummed snapshot directory.

    Args:
        db: Local (exporting) database handle; plain pymongo collection
            handles are used, mirroring sync_engine._get_dst_db.
        out_dir: Parent directory; the snapshot is written to
            ``out_dir/<snapshot_id>``.
        collections: Subset of SNAPSHOT_EXPORT_COLLECTIONS (aliases allowed).
            None exports every exportable collection.
        from_date: For date-partitioned collections only export documents
            with date >= this value.
        to_date: For date-partitioned collections only export documents
            with date <= this value.
        producer_image: Producing image SHA (or hostname fallback).
        dry_run: Print the plan without writing anything.

    Returns:
        Summary dict with the snapshot id, directory and per-collection stats.

    Raises:
        SnapshotTransferError: On unknown collections or filesystem conflicts.
    """
    resolved = _resolve_export_collections(collections)
    snapshot_id = _new_snapshot_id()
    snapshot_dir = out_dir / snapshot_id
    plan = {
        name: {
            "class": _collection_class(name),
            "date_field": _date_field(name),
            "filter": _export_query(name, from_date, to_date),
        }
        for name in resolved
    }
    if dry_run:
        logger.info(
            "Snapshot export (dry-run) plan: snapshot_id=%s out_dir=%s plan=%s",
            snapshot_id,
            out_dir,
            plan,
        )
        return {
            "status": "DRY_RUN",
            "dry_run": True,
            "snapshot_id": snapshot_id,
            "snapshot_dir": str(snapshot_dir),
            "collections_exported": 0,
            "total_docs": 0,
            "collections": {},
            "plan": plan,
            "producer_image": producer_image,
        }

    if snapshot_dir.exists():
        raise SnapshotTransferError(
            f"snapshot directory already exists: {snapshot_dir}; refusing to overwrite"
        )
    snapshot_dir.mkdir(parents=True)

    entries: list[dict[str, Any]] = []
    try:
        for name in resolved:
            entries.append(
                _export_collection(db, name, snapshot_dir, from_date, to_date)
            )
    except Exception:
        # An incomplete snapshot must never be mistakable for a good one:
        # the manifest is only written after every file completed, and a
        # failed export leaves no partial directory behind.
        manifest_path = snapshot_dir / MANIFEST_NAME
        if not manifest_path.exists():
            shutil.rmtree(snapshot_dir, ignore_errors=True)
            logger.warning("Removed partial snapshot directory %s", snapshot_dir)
        raise

    manifest = {
        "manifest_version": SNAPSHOT_MANIFEST_VERSION,
        "snapshot_id": snapshot_id,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "producer_db": db.name,
        "producer_image": producer_image,
        "collections": entries,
    }
    manifest_sha256 = _write_manifest(snapshot_dir, manifest)
    logger.info(
        "Snapshot manifest written: %s (sha256=%s)",
        snapshot_dir / MANIFEST_NAME,
        manifest_sha256,
    )

    results = {entry["name"]: entry for entry in entries}
    total_docs = sum(entry["doc_count"] for entry in entries)
    summary = {
        "status": "GOOD",
        "dry_run": False,
        "snapshot_id": snapshot_id,
        "snapshot_dir": str(snapshot_dir),
        "manifest": str(snapshot_dir / MANIFEST_NAME),
        "manifest_sha256": manifest_sha256,
        "producer_image": producer_image,
        "producer_db": db.name,
        "collections_exported": len(entries),
        "total_docs": total_docs,
        "collections": results,
    }
    logger.info("Snapshot export complete: %s", summary)
    return summary


def _load_manifest(snapshot_dir: Path) -> tuple[dict[str, Any], str]:
    """Load manifest.json, verify its own checksum, and fail closed.

    Returns (manifest, manifest_sha256). Raises SnapshotTransferError when the
    manifest is missing, unreadable, mis-structured, checksum-corrupted or
    written by an unsupported manifest_version.
    """
    manifest_path = snapshot_dir / MANIFEST_NAME
    checksum_path = snapshot_dir / MANIFEST_CHECKSUM_NAME
    if not manifest_path.is_file():
        raise SnapshotTransferError(f"snapshot manifest not found: {manifest_path}")
    if not checksum_path.is_file():
        raise SnapshotTransferError(
            f"snapshot manifest checksum not found: {checksum_path}; "
            "an unchecksummed manifest is treated as corrupted"
        )

    payload = manifest_path.read_bytes()
    manifest_sha256 = hashlib.sha256(payload).hexdigest()
    declared = checksum_path.read_text(encoding="utf-8").strip()
    match = _MANIFEST_CHECKSUM_LINE_RE.match(declared)
    if not match or match.group(1) != manifest_sha256:
        raise SnapshotTransferError(
            f"snapshot manifest checksum mismatch for {manifest_path}: "
            f"declared={declared!r} actual={manifest_sha256}"
        )

    try:
        manifest = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotTransferError(
            f"snapshot manifest is not valid JSON: {manifest_path}: {exc}"
        ) from exc
    if not isinstance(manifest, dict):
        raise SnapshotTransferError(
            f"snapshot manifest must be a JSON object: {manifest_path}"
        )

    manifest_version = manifest.get("manifest_version")
    if manifest_version != SNAPSHOT_MANIFEST_VERSION:
        raise SnapshotTransferError(
            "unsupported snapshot manifest_version "
            f"{manifest_version!r} in {manifest_path}; "
            f"supported version: {SNAPSHOT_MANIFEST_VERSION}"
        )
    for key in ("snapshot_id", "created_at", "producer_db", "producer_image"):
        if not isinstance(manifest.get(key), str) or not manifest.get(key):
            raise SnapshotTransferError(
                f"snapshot manifest field {key!r} must be a non-empty string "
                f"in {manifest_path}"
            )
    entries = manifest.get("collections")
    if not isinstance(entries, list) or not entries:
        raise SnapshotTransferError(
            f"snapshot manifest 'collections' must be a non-empty list in {manifest_path}"
        )
    for entry in entries:
        _validate_manifest_entry(entry, snapshot_dir)
    return manifest, manifest_sha256


def _validate_manifest_entry(entry: Any, snapshot_dir: Path) -> None:
    """Structurally validate one manifest collection entry (fail-closed)."""
    if not isinstance(entry, dict):
        raise SnapshotTransferError(
            f"snapshot manifest collection entry must be an object in {snapshot_dir}: "
            f"{entry!r}"
        )
    name = entry.get("name")
    if not isinstance(name, str) or not name:
        raise SnapshotTransferError(
            f"snapshot manifest collection entry without a valid 'name' in {snapshot_dir}"
        )
    file_name = entry.get("file")
    if not isinstance(file_name, str) or not _DATA_FILE_NAME_RE.match(file_name):
        raise SnapshotTransferError(
            f"snapshot manifest file name {file_name!r} for collection {name} "
            f"is not a plain {SNAPSHOT_FILE_SUFFIX} file name in {snapshot_dir}"
        )
    sha256 = entry.get("sha256")
    if not isinstance(sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", sha256):
        raise SnapshotTransferError(
            f"snapshot manifest sha256 for collection {name} is not a hex sha256 "
            f"in {snapshot_dir}: {sha256!r}"
        )
    doc_count = entry.get("doc_count")
    if not isinstance(doc_count, int) or isinstance(doc_count, bool) or doc_count < 0:
        raise SnapshotTransferError(
            f"snapshot manifest doc_count for collection {name} must be a "
            f"non-negative integer in {snapshot_dir}: {doc_count!r}"
        )
    data_as_of = entry.get("data_as_of")
    if data_as_of is not None and not isinstance(data_as_of, str):
        raise SnapshotTransferError(
            f"snapshot manifest data_as_of for collection {name} must be an "
            f"ISO8601 string or null in {snapshot_dir}: {data_as_of!r}"
        )
    if data_as_of is not None:
        try:
            datetime.datetime.fromisoformat(data_as_of)
        except ValueError as exc:
            raise SnapshotTransferError(
                f"snapshot manifest data_as_of for collection {name} is not "
                f"ISO8601 in {snapshot_dir}: {data_as_of!r}: {exc}"
            ) from exc
    collection_class = entry.get("class")
    if collection_class != _collection_class(name):
        raise SnapshotTransferError(
            f"snapshot manifest class {collection_class!r} for collection {name} "
            f"does not match the importer class {_collection_class(name)!r} "
            f"in {snapshot_dir}"
        )
    upsert_keys = entry.get("upsert_keys")
    expected_keys = _expected_upsert_keys(name)
    if upsert_keys != expected_keys:
        raise SnapshotTransferError(
            f"snapshot manifest upsert_keys {upsert_keys!r} for collection {name} "
            f"does not match the importer business keys {expected_keys!r} "
            f"in {snapshot_dir}"
        )


def _reject_collections_outside_allow_list(manifest: dict[str, Any]) -> None:
    """Raise naming every manifest collection outside the import allow-list."""
    rejections = sorted(
        entry.get("name", "<unnamed>")
        for entry in manifest["collections"]
        if entry.get("name") not in SNAPSHOT_IMPORT_ALLOWED_COLLECTIONS
    )
    if rejections:
        raise SnapshotTransferError(
            "snapshot manifest proposes collections outside the dev import "
            f"allow-list: {rejections}; allowed: "
            f"{sorted(SNAPSHOT_IMPORT_ALLOWED_COLLECTIONS)}"
        )


def _verify_snapshot_files(
    snapshot_dir: Path, manifest: dict[str, Any]
) -> list[dict[str, Any]]:
    """Pass 1: verify sha256, doc count, content and watermark — no writes.

    Streams every manifest file once, hashing the compressed bytes while
    decoding and validating every JSON line, then compares against the
    manifest. Raises SnapshotTransferError on the first mismatch.
    """
    verified: list[dict[str, Any]] = []
    for entry in manifest["collections"]:
        name = entry["name"]
        file_path = snapshot_dir / entry["file"]
        if not file_path.is_file():
            raise SnapshotTransferError(
                f"snapshot data file missing for collection {name}: {file_path}"
            )

        doc_count = 0
        data_as_of: datetime.datetime | None = None
        date_field = _date_field(name)
        with open(file_path, "rb") as raw:
            hashing = _HashingReader(raw)
            with (
                gzip.GzipFile(fileobj=hashing, mode="rb") as gz,
                io.TextIOWrapper(gz, encoding="utf-8", newline="") as text,
            ):
                for line_number, line in enumerate(text, start=1):
                    if not line.endswith("\n"):
                        raise SnapshotTransferError(
                            f"snapshot data file {file_path} for collection {name} "
                            f"ends mid-line at line {line_number}"
                        )
                    stripped = line.strip()
                    if not stripped:
                        raise SnapshotTransferError(
                            f"snapshot data file {file_path} for collection {name} "
                            f"has a blank line at {line_number}"
                        )
                    try:
                        doc = decode_bson_json(stripped)
                    except (json.JSONDecodeError, ValueError) as exc:
                        raise SnapshotTransferError(
                            f"snapshot data file {file_path} for collection {name} "
                            f"has an undecodable document at line {line_number}: {exc}"
                        ) from exc
                    if not isinstance(doc, dict):
                        raise SnapshotTransferError(
                            f"snapshot data file {file_path} for collection {name} "
                            f"has a non-object document at line {line_number}"
                        )
                    doc_count += 1
                    if date_field:
                        value = doc.get(date_field)
                        if isinstance(value, datetime.datetime):
                            value = _as_naive_utc(value)
                            if data_as_of is None or value > data_as_of:
                                data_as_of = value

        computed_sha256 = hashing.hasher.hexdigest()
        if computed_sha256 != entry["sha256"]:
            raise SnapshotTransferError(
                f"snapshot checksum mismatch for collection {name} "
                f"({file_path}): manifest={entry['sha256']} actual={computed_sha256}"
            )
        if doc_count != entry["doc_count"]:
            raise SnapshotTransferError(
                f"snapshot document count mismatch for collection {name} "
                f"({file_path}): manifest={entry['doc_count']} actual={doc_count}"
            )
        declared_as_of = (
            _as_naive_utc(datetime.datetime.fromisoformat(entry["data_as_of"]))
            if entry["data_as_of"]
            else None
        )
        if declared_as_of != data_as_of:
            raise SnapshotTransferError(
                f"snapshot data_as_of watermark mismatch for collection {name} "
                f"({file_path}): manifest={entry['data_as_of']!r} "
                f"content_max={data_as_of.isoformat() if data_as_of else None!r}"
            )
        logger.info(
            "Verified %s: doc_count=%d sha256=%s data_as_of=%s",
            name,
            doc_count,
            computed_sha256,
            entry["data_as_of"],
        )
        verified.append(
            {
                "name": name,
                "file": entry["file"],
                "sha256": computed_sha256,
                "doc_count": doc_count,
                "data_as_of": entry["data_as_of"],
                "class": entry["class"],
                "upsert_keys": entry["upsert_keys"],
            }
        )
    return verified


def _iter_decoded_docs(file_path: Path, name: str) -> Any:
    """Yield decoded documents from a snapshot data file (streaming)."""
    with (
        open(file_path, "rb") as raw,
        gzip.GzipFile(fileobj=raw, mode="rb") as gz,
        io.TextIOWrapper(gz, encoding="utf-8", newline="") as text,
    ):
        for line_number, line in enumerate(text, start=1):
            stripped = line.strip()
            if not stripped:
                raise SnapshotTransferError(
                    f"snapshot data file {file_path} for collection {name} "
                    f"has a blank line at {line_number}"
                )
            try:
                yield decode_bson_json(stripped)
            except (json.JSONDecodeError, ValueError) as exc:
                raise SnapshotTransferError(
                    f"snapshot data file {file_path} for collection {name} has an "
                    f"undecodable document at line {line_number}: {exc}"
                ) from exc


def _apply_upsert_collection(
    db: MongoDatabase, snapshot_dir: Path, entry: dict[str, Any]
) -> dict[str, Any]:
    """Apply a date-partitioned collection via business-key ReplaceOne upserts."""
    name = entry["name"]
    file_path = snapshot_dir / entry["file"]
    target: MongoCollection = db[name]
    upsert_keys = entry["upsert_keys"] or SYNC_UPSERT_KEYS.get(name) or ["_id"]

    stats = {"read": 0, "upserted": 0, "modified": 0}
    batch: list[ReplaceOne] = []

    for doc in _iter_decoded_docs(file_path, name):
        stats["read"] += 1
        doc_id = doc.get("_id")
        doc_copy = {k: v for k, v in doc.items() if k != "_id"}
        doc_filter = {k: doc.get(k) for k in upsert_keys}
        if any(v is None for v in doc_filter.values()):
            # Mirror sync_engine: a document missing part of its business key
            # falls back to the _id filter instead of matching on nulls.
            doc_filter = {"_id": doc_id}
        batch.append(ReplaceOne(doc_filter, doc_copy, upsert=True))
        if len(batch) >= BATCH_SIZE:
            result = target.bulk_write(batch, ordered=False)
            stats["upserted"] += result.upserted_count
            stats["modified"] += result.modified_count
            batch.clear()
            logger.info(
                "  %s: %d docs applied so far (snapshot %s)",
                name,
                stats["read"],
                snapshot_dir.name,
            )

    if batch:
        result = target.bulk_write(batch, ordered=False)
        stats["upserted"] += result.upserted_count
        stats["modified"] += result.modified_count

    logger.info(
        "Applied %s (upsert by %s): read=%d upserted=%d modified=%d",
        name,
        upsert_keys,
        stats["read"],
        stats["upserted"],
        stats["modified"],
    )
    return {"applied_as": "upsert", "upsert_keys": list(upsert_keys), **stats}


def _apply_snapshot_replace_collection(
    db: MongoDatabase, snapshot_dir: Path, entry: dict[str, Any]
) -> dict[str, Any]:
    """Apply a snapshot-class collection via staging + atomic rename.

    The staging collection is restored and count-verified first; only after
    the staged count matches the manifest is the target atomically replaced
    by renameCollection(..., dropTarget=True) — a failure at any earlier
    point leaves the previous dev state untouched.
    """
    name = entry["name"]
    file_path = snapshot_dir / entry["file"]
    staging_name = f"{name}{STAGING_SUFFIX}"
    staging: MongoCollection = db[staging_name]

    # Drop any leftover staging collection from an aborted previous attempt.
    staging.drop()

    inserted = 0
    batch: list[dict[str, Any]] = []
    for doc in _iter_decoded_docs(file_path, name):
        batch.append(doc)
        if len(batch) >= BATCH_SIZE:
            staging.insert_many(batch)
            inserted += len(batch)
            batch.clear()
    if batch:
        staging.insert_many(batch)
        inserted += len(batch)

    staged_count = staging.count_documents({})
    if staged_count != entry["doc_count"]:
        raise SnapshotTransferError(
            f"snapshot staging verification failed for collection {name}: staged "
            f"{staged_count} documents but the manifest declares "
            f"{entry['doc_count']}; the target collection was NOT modified"
        )

    # Atomically replace the target with the verified staging collection. The
    # previous target state is dropped by the rename itself (dropTarget=True)
    # in the same atomic step, so no failure window can leave dev without
    # either the old or the new state.
    #
    # MongoDB requires the renameCollection command to be issued against the
    # admin database (pymongo's own Collection.rename helper routes it via
    # conn.admin), so run it on the admin handle with fully-qualified
    # namespaces. It also needs the renameCollectionSameDB privilege
    # (dbAdmin/dbOwner) on the dev database — verified against the real dev
    # mongod in the slice-3 acceptance checklist before the Stage-1 cutover.
    source_ns = f"{db.name}.{staging_name}"
    target_ns = f"{db.name}.{name}"
    db.client.admin.command(
        {"renameCollection": source_ns, "to": target_ns, "dropTarget": True}
    )
    logger.info(
        "Applied %s (replace): staged=%d renamed %s -> %s (dropTarget=True)",
        name,
        staged_count,
        source_ns,
        target_ns,
    )
    return {"applied_as": "replace", "read": inserted, "staged": staged_count}


def _record_import_state(
    db: MongoDatabase,
    manifest: dict[str, Any],
    manifest_sha256: str,
    verified: list[dict[str, Any]],
    producer_image_note: str | None,
) -> dict[str, Any]:
    """Persist the traceability record into snapshot_import_state.

    One record per applied manifest (keyed by the manifest sha256, mirroring
    data_sync_state's idempotent upsert): re-importing the same snapshot
    refreshes applied_at instead of duplicating the record.
    """
    record = {
        "_id": manifest_sha256,
        "manifest_version": manifest["manifest_version"],
        "snapshot_id": manifest["snapshot_id"],
        "manifest_sha256": manifest_sha256,
        "created_at": manifest["created_at"],
        "producer_db": manifest["producer_db"],
        "producer_image": manifest["producer_image"],
        "applied_at": datetime.datetime.now(datetime.timezone.utc),
        "dry_run": False,
        "collections": [
            {
                "name": entry["name"],
                "doc_count": entry["doc_count"],
                "data_as_of": entry["data_as_of"],
                "sha256": entry["sha256"],
                "class": entry["class"],
                "applied_as": entry["applied_as"],
            }
            for entry in verified
        ],
    }
    if producer_image_note:
        record["producer_image_note"] = producer_image_note
    state_col: MongoCollection = db[SNAPSHOT_IMPORT_STATE_COLLECTION]
    state_col.replace_one({"_id": manifest_sha256}, record, upsert=True)
    logger.info(
        "Recorded snapshot import state: snapshot_id=%s manifest_sha256=%s",
        manifest["snapshot_id"],
        manifest_sha256,
    )
    return record


def refresh_asset_status_after_import(applied_collections: list[str]) -> dict[str, Any]:
    """Propagate imported coverage into dev's data_asset_status metadata.

    Reuses the dev ecosystem's canonical asset-status refresh — the
    data_asset_status initializer's recompute (the same path
    ``./scripts/caifubao data refresh-status`` runs) — instead of building a
    parallel status system. The recompute derives latest_data_date/status
    from the freshly imported stock_daily_quote / stock_factor_daily content,
    so the snapshot's actual coverage (its data_as_of) becomes what the
    data-quality page and health watcher report, and a stale snapshot shows
    as stale.
    """
    from app.jobs import data_asset_status_initializer

    initializer = data_asset_status_initializer._load_default_initializer()
    result = initializer.run()
    summary = {
        "refreshed_via": "data_asset_status_initializer.recompute",
        "collections_imported": sorted(applied_collections),
        "asset_count": result.get("asset_count", 0),
        "written_count": result.get("written_count", 0),
        "status_counts": result.get("status_counts", {}),
    }
    logger.info("data_asset_status refresh after snapshot import: %s", summary)
    return summary


def run_import(
    db: MongoDatabase,
    snapshot_dir: Path,
    dry_run: bool = False,
    producer_image_note: str | None = None,
) -> dict[str, Any]:
    """Import a verified snapshot into the local database (fail-closed).

    PASS 1 (never writes): load and checksum-verify manifest.json, reject
    every collection outside the import allow-list, verify every data file's
    sha256 while streaming, count documents and decode-validate every
    document against the manifest.

    PASS 2 (skipped on dry-run): apply per collection class — business-key
    upserts for date-partitioned collections, staging + verified atomic
    rename for snapshot-class collections — then record the import into
    ``snapshot_import_state`` and refresh data_asset_status.

    Args:
        db: Local (dev) database handle.
        snapshot_dir: Directory holding manifest.json and the data files.
        dry_run: Verify only; write nothing.
        producer_image_note: Optional importing-environment annotation stored
            alongside the manifest's producer_image.

    Returns:
        Summary dict with verified/applied per-collection stats.

    Raises:
        SnapshotTransferError: On any manifest/allow-list/checksum/count/
            watermark mismatch — before any dev collection is mutated.
    """
    start_time = datetime.datetime.now(datetime.timezone.utc)
    snapshot_dir = Path(snapshot_dir)
    manifest, manifest_sha256 = _load_manifest(snapshot_dir)
    _reject_collections_outside_allow_list(manifest)

    verified = _verify_snapshot_files(snapshot_dir, manifest)
    total_docs = sum(entry["doc_count"] for entry in verified)
    logger.info(
        "Snapshot %s verified: %d collections, %d documents, manifest_sha256=%s",
        manifest["snapshot_id"],
        len(verified),
        total_docs,
        manifest_sha256,
    )

    if dry_run:
        summary = {
            "status": "DRY_RUN",
            "dry_run": True,
            "snapshot_id": manifest["snapshot_id"],
            "snapshot_dir": str(snapshot_dir),
            "manifest_sha256": manifest_sha256,
            "producer_image": manifest["producer_image"],
            "collections_verified": len(verified),
            "collections_applied": 0,
            "total_docs": total_docs,
            "collections": {entry["name"]: entry for entry in verified},
        }
        logger.info("Snapshot import (dry-run): no data applied. %s", summary)
        return summary

    applied: list[dict[str, Any]] = []
    try:
        for entry in verified:
            if entry["class"] == "snapshot":
                result = _apply_snapshot_replace_collection(db, snapshot_dir, entry)
            else:
                result = _apply_upsert_collection(db, snapshot_dir, entry)
            entry.update(result)
            applied.append(entry)
    except SnapshotTransferError as exc:
        raise SnapshotTransferError(
            f"snapshot apply failed after {len(applied)} collections "
            f"({[e['name'] for e in applied]}); earlier applies are idempotent "
            f"on re-run: {exc}"
        ) from exc

    _record_import_state(db, manifest, manifest_sha256, verified, producer_image_note)

    applied_names = [entry["name"] for entry in applied]
    try:
        asset_status_refresh = refresh_asset_status_after_import(applied_names)
    except Exception as exc:
        raise SnapshotTransferError(
            f"snapshot {manifest['snapshot_id']} was applied but the "
            f"data_asset_status refresh failed; freshness metadata was NOT "
            f"updated for {applied_names}: {exc}"
        ) from exc

    elapsed = (
        datetime.datetime.now(datetime.timezone.utc) - start_time
    ).total_seconds()
    summary = {
        "status": "GOOD",
        "dry_run": False,
        "snapshot_id": manifest["snapshot_id"],
        "snapshot_dir": str(snapshot_dir),
        "manifest_sha256": manifest_sha256,
        "producer_image": manifest["producer_image"],
        "collections_verified": len(verified),
        "collections_applied": len(applied),
        "total_docs": total_docs,
        "collections": {entry["name"]: entry for entry in applied},
        "asset_status_refresh": asset_status_refresh,
        "elapsed_seconds": round(elapsed, 2),
    }
    logger.info("Snapshot import complete: %s", summary)
    return summary
