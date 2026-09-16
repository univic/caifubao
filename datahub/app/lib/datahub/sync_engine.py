"""MongoDB-to-MongoDB data sync engine.

Copies external/read-only data collections from a source (prod) MongoDB to
the current (dev) MongoDB destination. Only syncs upstream data that the
scoring pipeline depends on: quotes, factors, signals, industry
classifications, and market info. Never syncs computed results that are
environment-specific (scoring predictions, backtest results, etc.).

In dev environments, syncing signal data avoids the chicken-and-egg problem
where signal computation requires factors and scoring requires signals.
In prod, signals are computed locally by the signal runner.

Usage:
    python -m app.jobs.data_sync_runner run
"""

import datetime
import logging
import os
from collections.abc import Generator
from typing import Any

from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection as MongoCollection
from pymongo.database import Database as MongoDatabase

logger = logging.getLogger(__name__)

# Collections that are safe to sync (upstream data for scoring pipeline).
# signal data is included for dev environments to avoid re-running the full
# quote→factor→signal pipeline locally; in prod, signals are computed locally.
# stock_daily_basic is tushare valuation snapshots (dev-first research data;
# once prod backfills it, dev keeps in sync here like the other collections).
SYNCABLE_COLLECTIONS = {
    "stock_daily_quote": {"date_field": "date"},
    "stock_daily_basic": {"date_field": "date"},
    "stock_factor_daily": {"date_field": "date"},
    "stock_signal_daily": {"date_field": "date", "dev_only": True},
    "finance_market": {"date_field": None},  # small, always full sync
    "stock_industry": {"date_field": None},  # small snapshot, always full sync
}

# User-friendly aliases for --collections argument
COLLECTION_ALIASES = {
    "quote": "stock_daily_quote",
    "daily_basic": "stock_daily_basic",
    "factor": "stock_factor_daily",
    "signal": "stock_signal_daily",
    "market": "finance_market",
    "industry": "stock_industry",
}

# Number of documents per bulk-write batch
BATCH_SIZE = 500
DEFAULT_OVERLAP_DAYS = 3
SYNC_STATE_COLLECTION = "data_sync_state"
DEV_ENV_VALUES = {"dev", "development", "local", "test"}

# Business keys for idempotent upsert. Syncing merges by these fields instead
# of _id so documents produced independently in the target (e.g. a dev quote
# job writing the same (code, date) with a different _id) do not collide on
# the unique (code, date) indexes with E11000. Collections without a unique
# business key (snapshots) keep the _id-based upsert.
SYNC_UPSERT_KEYS = {
    "stock_daily_quote": ["code", "date"],
    "stock_daily_basic": ["code", "date"],
    "stock_factor_daily": ["stock_code", "date"],
    "stock_signal_daily": ["stock_code", "date", "signal_name"],
    # stock_industry has a unique stock_code (model/industry.py) — a dev-side
    # doc with the same stock_code but a different _id would otherwise E11000.
    "stock_industry": ["stock_code"],
}


def _is_dev_environment() -> bool:
    """Return True when signal sync is allowed for this runtime."""
    env = os.getenv("APP_ENV") or os.getenv("ENVIRONMENT") or "DEV"
    return env.strip().lower() in DEV_ENV_VALUES


def _resolve_sync_collections(
    target_collections: list[str] | None,
    is_dev_environment: bool,
) -> list[str]:
    """Resolve aliases and remove collections that are unsafe for this env."""
    selected = target_collections or list(SYNCABLE_COLLECTIONS.keys())
    resolved = list(
        dict.fromkeys(COLLECTION_ALIASES.get(c.strip(), c.strip()) for c in selected)
    )
    unknown = set(resolved) - set(SYNCABLE_COLLECTIONS.keys())
    if unknown:
        logger.warning("Skipping unknown collections: %s", unknown)
        resolved = [c for c in resolved if c in SYNCABLE_COLLECTIONS]

    if not is_dev_environment:
        dev_only = [
            name for name in resolved if SYNCABLE_COLLECTIONS[name].get("dev_only")
        ]
        if dev_only:
            logger.warning(
                "Skipping dev-only sync collections outside dev environment: %s",
                dev_only,
            )
            resolved = [name for name in resolved if name not in dev_only]

    return resolved


def _build_src_client() -> MongoClient:
    """Create a PyMongo client for the source (prod) database."""
    from app.conf import app_config as cfg

    host = cfg.MONGODB_SRC_HOST
    port = cfg.MONGODB_SRC_PORT
    user = cfg.MONGODB_SRC_USERNAME
    pwd = cfg.MONGODB_SRC_PASSWORD
    # Build client from individual parameters (not a connection string)
    # to avoid triggering gitleaks pattern detection.
    # Explicit authSource="admin" to match existing mongoengine config.
    client = MongoClient(
        host=host,
        port=port,
        username=user,
        password=pwd,
        authSource="admin",
        serverSelectionTimeoutMS=10000,
    )
    return client


def _get_src_db(client: MongoClient, cfg) -> MongoDatabase:
    """Return the source database handle."""
    return client[cfg.MONGODB_SRC_NAME]


def _get_dst_db(cfg) -> MongoDatabase:
    """Return the local (dev) database handle via mongoengine's connection."""
    from mongoengine import get_connection

    conn = get_connection()
    return conn[cfg.MONGODB_DB]


def _iter_docs(
    col: MongoCollection,
    date_field: str | None,
    from_date: datetime.datetime | None,
    to_date: datetime.datetime | None,
) -> Generator[dict[str, Any]]:
    """Yield documents from the source collection, optionally filtered by date."""
    query: dict[str, Any] = {}
    if date_field and (from_date or to_date):
        date_filter: dict[str, datetime.datetime] = {}
        if from_date:
            date_filter["$gte"] = from_date
        if to_date:
            date_filter["$lte"] = to_date
        if date_filter:
            query[date_field] = date_filter

    logger.info(
        "Reading from %s with filter=%s (date_field=%s)",
        col.name,
        query,
        date_field,
    )

    # Prioritize the newest business data. If a job reaches its deadline after
    # partially processing a window, dev still receives the latest trading day
    # first. Snapshot collections have no business date and fall back to _id.
    sort = (date_field, -1) if date_field else ("_id", -1)
    cursor = col.find(query).sort([sort])
    yield from cursor


def _resolve_collection_window(
    *,
    date_field: str | None,
    from_date: datetime.datetime | None,
    to_date: datetime.datetime | None,
    full_sync: bool,
    overlap_days: int,
    sync_state: dict[str, Any] | None,
) -> tuple[
    datetime.datetime | None,
    datetime.datetime | None,
    datetime.datetime | None,
]:
    """Resolve an explicit or destination-watermark sync window."""
    if from_date or to_date:
        return from_date, to_date, None
    if full_sync or not date_field:
        return None, None, None
    if overlap_days < 0:
        raise ValueError("overlap_days must be a non-negative integer")

    state = sync_state or {}
    if state.get("bootstrap_complete") is not True:
        # A partially written bootstrap must never become the incremental
        # watermark. Only a collection-level completion marker can switch
        # this collection out of full bootstrap mode.
        return None, None, None
    watermark = state.get("watermark")
    if watermark is None:
        return None, None, None
    return watermark - datetime.timedelta(days=overlap_days), None, watermark


def _latest_collection_date(
    collection: MongoCollection,
    date_field: str,
) -> datetime.datetime | None:
    latest = collection.find_one(
        {},
        {date_field: 1, "_id": 0},
        sort=[(date_field, -1)],
    )
    return latest.get(date_field) if latest else None


def _require_date_first_index(
    collection: MongoCollection,
    date_field: str,
    *,
    role: str,
) -> None:
    for index in collection.list_indexes():
        keys = index.get("key", {})
        first_key = next(iter(keys.items()), None)
        if (
            first_key
            and first_key[0] == date_field
            and first_key[1] in (1, -1)
            and not index.get("sparse")
            and not index.get("partialFilterExpression")
        ):
            return
    raise RuntimeError(
        f"{role} collection {collection.name} requires a non-sparse, "
        f"non-partial ascending or descending index beginning with {date_field} "
        "before incremental sync"
    )


def _save_sync_state(
    state_collection: MongoCollection,
    *,
    collection_name: str,
    watermark: datetime.datetime | None,
    mode: str,
) -> None:
    state_collection.update_one(
        {"_id": collection_name},
        {
            "$set": {
                "bootstrap_complete": True,
                "watermark": watermark,
                "last_mode": mode,
                "updated_at": datetime.datetime.now(datetime.UTC),
            }
        },
        upsert=True,
    )


def _sync_collection(
    src_col: MongoCollection,
    dst_col: MongoCollection,
    date_field: str | None,
    from_date: datetime.datetime | None,
    to_date: datetime.datetime | None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Sync one collection from source to destination using upsert.

    Returns counts of {read, upserted, modified}.
    """
    stats = {"read": 0, "upserted": 0, "modified": 0}
    batch: list[UpdateOne] = []

    for doc in _iter_docs(src_col, date_field, from_date, to_date):
        stats["read"] += 1
        doc_id = doc["_id"]
        # Remove _id from the update but keep it for the filter
        doc_copy = {k: v for k, v in doc.items() if k != "_id"}

        # Match by business key when the collection has one (see
        # SYNC_UPSERT_KEYS) — documents with the same business key but a
        # different _id (produced independently in dev) would otherwise hit
        # E11000 on the unique (code, date) index.
        upsert_keys = SYNC_UPSERT_KEYS.get(src_col.name)
        if upsert_keys:
            doc_filter = {k: doc.get(k) for k in upsert_keys}
            if any(v is None for v in doc_filter.values()):
                doc_filter = {"_id": doc_id}
        else:
            doc_filter = {"_id": doc_id}

        batch.append(
            UpdateOne(
                doc_filter,
                {"$set": doc_copy},
                upsert=True,
            )
        )

        if len(batch) >= BATCH_SIZE:
            if not dry_run:
                result = dst_col.bulk_write(batch, ordered=False)
                stats["upserted"] += result.upserted_count
                stats["modified"] += result.modified_count
            batch.clear()
            logger.info(
                "  %s: %d docs processed so far...",
                src_col.name,
                stats["read"],
            )

    # Flush remaining
    if batch:
        if not dry_run:
            result = dst_col.bulk_write(batch, ordered=False)
            stats["upserted"] += result.upserted_count
            stats["modified"] += result.modified_count

    logger.info(
        "%s: completed — read=%d upserted=%d modified=%d",
        src_col.name,
        stats["read"],
        stats["upserted"],
        stats["modified"],
    )
    return stats


def run_sync(
    src_client: MongoClient | None = None,
    dry_run: bool = False,
    collections: list[str] | None = None,
    from_date: datetime.datetime | None = None,
    to_date: datetime.datetime | None = None,
    full_sync: bool = False,
    overlap_days: int = DEFAULT_OVERLAP_DAYS,
) -> dict[str, Any]:
    """Sync source MongoDB data into the local database.

    Args:
        src_client: Optional pre-existing MongoClient for the source.
        dry_run: If True, read but do not write.
        collections: Subset of SYNCABLE_COLLECTIONS to sync. None = all.
        from_date: Only sync documents with date >= this value.
        to_date: Only sync documents with date <= this value.
        full_sync: Disable destination-watermark incremental selection.
        overlap_days: Calendar days before each destination watermark to replay.

    Returns:
        Summary dict with per-collection stats.
    """
    if full_sync and (from_date is not None or to_date is not None):
        raise ValueError("full_sync cannot be combined with from_date or to_date")
    if overlap_days < 0:
        raise ValueError("overlap_days must be a non-negative integer")

    close_client = False
    if src_client is None:
        src_client = _build_src_client()
        close_client = True

    try:
        from app.conf import app_config as cfg

        src_db = _get_src_db(src_client, cfg)
        dst_db = _get_dst_db(cfg)

        resolved = _resolve_sync_collections(
            collections,
            is_dev_environment=_is_dev_environment(),
        )

        results = {}
        state_collection = dst_db[SYNC_STATE_COLLECTION]
        # Validate every dated collection before the first write so a missing
        # index cannot leave a multi-collection run partially applied.
        for name in resolved:
            date_field = SYNCABLE_COLLECTIONS[name]["date_field"]
            if date_field:
                _require_date_first_index(src_db[name], date_field, role="source")
                _require_date_first_index(dst_db[name], date_field, role="destination")

        start_time = datetime.datetime.now(datetime.UTC)
        for name in resolved:
            config = SYNCABLE_COLLECTIONS[name]
            src_col = src_db[name]
            dst_col = dst_db[name]
            date_field = config["date_field"]
            sync_state = None
            source_watermark = None
            if date_field:
                sync_state = state_collection.find_one({"_id": name})
                source_watermark = _latest_collection_date(src_col, date_field)
            collection_from, collection_to, watermark = _resolve_collection_window(
                date_field=date_field,
                from_date=from_date,
                to_date=to_date,
                full_sync=full_sync,
                overlap_days=overlap_days,
                sync_state=sync_state,
            )
            stats = _sync_collection(
                src_col=src_col,
                dst_col=dst_col,
                date_field=date_field,
                from_date=collection_from,
                to_date=collection_to,
                dry_run=dry_run,
            )
            stats["from_date"] = collection_from
            stats["to_date"] = collection_to
            stats["destination_watermark"] = watermark
            results[name] = stats
            explicit_window = from_date is not None or to_date is not None
            bootstrap_required = bool(
                date_field
                and not explicit_window
                and (not sync_state or sync_state.get("bootstrap_complete") is not True)
            )
            if date_field and not dry_run and (not explicit_window or full_sync):
                _save_sync_state(
                    state_collection,
                    collection_name=name,
                    watermark=source_watermark,
                    mode="full" if full_sync or bootstrap_required else "incremental",
                )

        elapsed = (datetime.datetime.now(datetime.UTC) - start_time).total_seconds()
        total_read = sum(s["read"] for s in results.values())
        total_upserted = sum(s["upserted"] for s in results.values())
        total_modified = sum(s["modified"] for s in results.values())

        summary = {
            "status": "GOOD" if not dry_run else "DRY_RUN",
            "dry_run": dry_run,
            "full_sync": full_sync,
            "overlap_days": overlap_days,
            "elapsed_seconds": round(elapsed, 2),
            "collections_synced": len(resolved),
            "collections": results,
            "total_read": total_read,
            "total_upserted": total_upserted,
            "total_modified": total_modified,
        }
        logger.info("Sync complete: %s", summary)
        return summary
    finally:
        if close_client:
            src_client.close()
