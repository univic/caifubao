# -*- coding: utf-8 -*-
"""MongoDB-to-MongoDB data sync engine.

Copies external/read-only data collections from a source (prod) MongoDB to
the current (dev) MongoDB destination. Only syncs "source-of-truth" data:
quotes, factors, industry classifications, and market info. Never syncs
computed results (scoring predictions, signals, backtest results, etc.).

Usage:
    python -m app.jobs.data_sync_runner run
"""

import datetime
import logging
from collections.abc import Generator
from typing import Any

from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection as MongoCollection
from pymongo.database import Database as MongoDatabase

logger = logging.getLogger(__name__)

# Collections that are safe to sync (external data only, no computed results)
SYNCABLE_COLLECTIONS = {
    "stock_daily_quote": {"date_field": "date"},
    "stock_factor_daily": {"date_field": "date"},
    "finance_market": {"date_field": None},  # small, always full sync
    "stock_industry": {"date_field": None},  # small snapshot, always full sync
}

# User-friendly aliases for --collections argument
COLLECTION_ALIASES = {
    "quote": "stock_daily_quote",
    "factor": "stock_factor_daily",
    "market": "finance_market",
    "industry": "stock_industry",
}

# Number of documents per bulk-write batch
BATCH_SIZE = 500


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
    return conn[cfg.MONGODB_NAME]


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

    # Sort by _id for stable paging
    cursor = col.find(query).sort("_id", 1)
    yield from cursor


def _sync_collection(
    src_col: MongoCollection,
    dst_col: MongoCollection,
    date_field: str | None,
    from_date: datetime.datetime | None,
    to_date: datetime.datetime | None,
    dry_run: bool = False,
) -> dict[str, int]:
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

        batch.append(
            UpdateOne(
                {"_id": doc_id},
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
) -> dict[str, Any]:
    """Execute a full data sync from the source MongoDB to the local one.

    Args:
        src_client: Optional pre-existing MongoClient for the source.
        dry_run: If True, read but do not write.
        collections: Subset of SYNCABLE_COLLECTIONS to sync. None = all.
        from_date: Only sync documents with date >= this value.
        to_date: Only sync documents with date <= this value.

    Returns:
        Summary dict with per-collection stats.
    """
    close_client = False
    if src_client is None:
        src_client = _build_src_client()
        close_client = True

    try:
        from app.conf import app_config as cfg

        src_db = _get_src_db(src_client, cfg)
        dst_db = _get_dst_db(cfg)

        target_collections = collections or list(SYNCABLE_COLLECTIONS.keys())
        # Strip whitespace, resolve aliases, deduplicate
        resolved = list(
            dict.fromkeys(
                COLLECTION_ALIASES.get(c.strip(), c.strip()) for c in target_collections
            )
        )
        unknown = set(resolved) - set(SYNCABLE_COLLECTIONS.keys())
        if unknown:
            logger.warning("Skipping unknown collections: %s", unknown)
            resolved = [c for c in resolved if c in SYNCABLE_COLLECTIONS]

        results = {}
        start_time = datetime.datetime.now(datetime.UTC)
        for name in resolved:
            config = SYNCABLE_COLLECTIONS[name]
            src_col = src_db[name]
            dst_col = dst_db[name]
            stats = _sync_collection(
                src_col=src_col,
                dst_col=dst_col,
                date_field=config["date_field"],
                from_date=from_date,
                to_date=to_date,
                dry_run=dry_run,
            )
            results[name] = stats

        elapsed = (datetime.datetime.now(datetime.UTC) - start_time).total_seconds()
        total_read = sum(s["read"] for s in results.values())
        total_upserted = sum(s["upserted"] for s in results.values())
        total_modified = sum(s["modified"] for s in results.values())

        summary = {
            "status": "GOOD" if not dry_run else "DRY_RUN",
            "dry_run": dry_run,
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
