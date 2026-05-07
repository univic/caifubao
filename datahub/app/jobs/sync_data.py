#!/usr/bin/env python3
"""
Sync data between MongoDB instances for development/testing.

Reads collections from a source (prod) MongoDB and writes them to a target (dev) MongoDB.
Supports filtering by date and selecting specific collections.

Usage:
    python -m app.jobs.sync_data \\
        --from-uri "mongodb://user:pass@prod-host:27017/caifubao" \\
        --to-uri "mongodb://user:pass@dev-host:27017/caifubao" \\
        --date 2026-05-06

    python -m app.jobs.sync_data \\
        --from-uri "mongodb://..." --to-uri "mongodb://..." \\
        --date 2026-05-01 --date 2026-05-07 \\
        --collections stock_daily_quotes,stock_signal_daily \\
        --dry-run
"""

from __future__ import annotations

import argparse
import datetime
import logging
import sys
from collections.abc import Sequence
from typing import Any

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

logger = logging.getLogger(__name__)

DEFAULT_COLLECTIONS = [
    "stock_daily_quotes",
    "stock_factor_daily",
    "stock_signal_daily",
    "stock_score_predictions",
]

# Map collections to their date field names for filtering
COLLECTION_DATE_FIELDS: dict[str, list[str]] = {
    "stock_daily_quotes": ["date"],
    "stock_factor_daily": ["date", "trade_date"],
    "stock_signal_daily": ["date", "signal_date"],
    "stock_score_predictions": ["prediction_date", "evaluation_date"],
}


def parse_date(value: str) -> datetime.datetime:
    return datetime.datetime.strptime(value, "%Y-%m-%d")


def connect(uri: str) -> Database:
    """Connect to MongoDB and return the database object."""
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    # Extract database name from URI path or default to 'caifubao'
    db_name = "caifubao"
    if "/" in uri.split("@")[-1] if "@" in uri else "/" in uri:
        parts = uri.split("/")
        if len(parts) > 1 and parts[-1]:
            db_name = parts[-1].split("?")[0]
    db = client.get_database(db_name)
    # Verify connection
    db.command("ping")
    logger.info(
        "Connected to MongoDB: %s/%s", uri.split("@")[-1].split("/")[0], db_name
    )
    return db


def _find_date_field(collection: Collection, date_fields: list[str]) -> str | None:
    """Find which date field exists on a document in the collection."""
    doc = collection.find_one(projection={f: 1 for f in date_fields})
    if doc is None:
        return None
    for field in date_fields:
        if field in doc:
            return field
    return None


def _build_date_filter(
    date_field: str | None,
    dates: list[datetime.datetime] | None,
) -> dict[str, Any]:
    """Build a MongoDB query filter for the given dates.

    Returns an empty dict if no dates are provided.
    Raises ValueError if dates are given but date_field is None
    (caller should guard against this before calling).
    """
    if not dates:
        return {}
    if date_field is None:
        raise ValueError(
            "date_field is None but dates were provided. "
            "This should not happen — caller must validate."
        )
    if len(dates) == 1:
        d = dates[0]
        return {
            "$or": [
                {date_field: d},
                {date_field: d.strftime("%Y-%m-%d")},
            ]
        }
    return {
        "$or": [
            {
                date_field: {
                    "$gte": min(dates),
                    "$lte": max(dates),
                }
            },
            {
                date_field: {
                    "$gte": min(dates).strftime("%Y-%m-%d"),
                    "$lte": max(dates).strftime("%Y-%m-%d"),
                }
            },
        ]
    }


def _document_size(doc: dict[str, Any]) -> int:
    """Estimate the size of a document in bytes."""
    return len(str(doc))


def sync_collection(
    source_db: Database,
    target_db: Database,
    collection_name: str,
    date_filter: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
    batch_size: int = 500,
) -> dict[str, Any]:
    """
    Sync a single collection from source to target.

    Returns a summary dict with counts and status.
    """
    source_coll = source_db[collection_name]
    target_coll = target_db[collection_name]

    query: dict[str, Any] = date_filter or {}

    total_source = source_coll.count_documents(query)
    logger.info(
        "Collection '%s': %d documents match query%s",
        collection_name,
        total_source,
        " (dry run)" if dry_run else "",
    )

    if dry_run or total_source == 0:
        return {
            "collection": collection_name,
            "matched": total_source,
            "inserted": 0,
            "total_size_bytes": 0,
        }

    # Create unique index if not exists on target (based on source indexes)
    _ensure_indexes(source_coll, target_coll)

    # Batch insert
    inserted = 0
    total_size = 0
    cursor = source_coll.find(query).batch_size(batch_size)

    batch: list[dict[str, Any]] = []
    for doc in cursor:
        doc["_sync_ts"] = datetime.datetime.utcnow()
        batch.append(doc)
        total_size += _document_size(doc)

        if len(batch) >= batch_size:
            try:
                target_coll.insert_many(batch, ordered=False)
            except Exception:
                # Fall back to individual inserts on duplicate key errors
                for d in batch:
                    try:
                        target_coll.update_one(
                            {"_id": d["_id"]}, {"$set": d}, upsert=True
                        )
                    except Exception as exc:
                        logger.warning(
                            "Failed to upsert doc %s in %s: %s",
                            d.get("_id"),
                            collection_name,
                            exc,
                        )
            inserted += len(batch)
            batch = []

    # Flush remaining batch
    if batch:
        try:
            target_coll.insert_many(batch, ordered=False)
        except Exception:
            for d in batch:
                try:
                    target_coll.update_one({"_id": d["_id"]}, {"$set": d}, upsert=True)
                except Exception as exc:
                    logger.warning(
                        "Failed to upsert doc %s in %s: %s",
                        d.get("_id"),
                        collection_name,
                        exc,
                    )
        inserted += len(batch)

    return {
        "collection": collection_name,
        "matched": total_source,
        "inserted": inserted,
        "total_size_bytes": total_size,
    }


def _ensure_indexes(source_coll: Collection, target_coll: Collection) -> None:
    """Copy indexes from source to target (skip _id_ which is automatic)."""
    try:
        source_indexes = list(source_coll.list_indexes())
        existing_target = set(idx["name"] for idx in target_coll.list_indexes())
        for idx in source_indexes:
            if idx.get("name") == "_id_":
                continue
            if idx.get("name") in existing_target:
                continue
            keys = list(idx["key"].items())
            kwargs = {
                k: v for k, v in idx.items() if k not in ("key", "v", "ns", "name")
            }
            target_coll.create_index(keys, name=idx.get("name"), **kwargs)
    except Exception as exc:
        logger.warning("Failed to copy indexes for %s: %s", source_coll.name, exc)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync datahub collections between MongoDB instances for dev/testing."
    )
    parser.add_argument(
        "--from-uri",
        required=True,
        help="Source MongoDB URI (e.g., mongodb://user:pass@prod-host:27017/caifubao)",
    )
    parser.add_argument(
        "--to-uri",
        required=True,
        help="Target MongoDB URI (e.g., mongodb://user:pass@dev-host:27017/caifubao)",
    )
    parser.add_argument(
        "--date",
        action="append",
        dest="dates",
        default=[],
        help="Date filter (YYYY-MM-DD). Can be specified multiple times for a range.",
    )
    parser.add_argument(
        "--collections",
        default=",".join(DEFAULT_COLLECTIONS),
        help=f"Comma-separated collection names to sync (default: all). "
        f"Available: {', '.join(DEFAULT_COLLECTIONS)}",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be synced without writing any data.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of documents per batch (default: 500).",
    )
    parser.add_argument(
        "--allow-full-sync",
        action="store_true",
        help="Allow full collection sync when --date is given but no date field is detected. "
        "Without this flag, the sync will abort if --date is specified but the date field "
        "cannot be determined for a collection.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Parse dates
    dates: list[datetime.datetime] = []
    for d in args.dates:
        dates.append(parse_date(d))
    dates.sort()

    # Parse collections
    collections = [c.strip() for c in args.collections.split(",") if c.strip()]

    # Connect
    logger.info("Connecting to source...")
    source_db = connect(args.from_uri)
    logger.info("Connecting to target...")
    target_db = connect(args.to_uri)

    # Build date filter per collection (auto-detect date field)
    parsed_dates = dates  # Already sorted

    results: list[dict[str, Any]] = []
    for collection_name in collections:
        source_coll = source_db[collection_name]

        date_fields = COLLECTION_DATE_FIELDS.get(collection_name, ["date"])
        date_field = _find_date_field(source_coll, date_fields)

        if parsed_dates and not date_field:
            if args.allow_full_sync:
                logger.warning(
                    "Collection '%s': could not determine date field from %s, "
                    "syncing all documents (--allow-full-sync is set).",
                    collection_name,
                    date_fields,
                )
                date_filter = {}
            else:
                logger.error(
                    "Collection '%s': cannot determine date field from fields %s. "
                    "Aborting to prevent unintentional full sync. "
                    "Use --allow-full-sync to override.",
                    collection_name,
                    date_fields,
                )
                sys.exit(1)
        elif parsed_dates and date_field:
            date_filter = _build_date_filter(date_field, parsed_dates)
        else:
            date_filter = {}

        summary = sync_collection(
            source_db,
            target_db,
            collection_name,
            date_filter,
            dry_run=args.dry_run,
            batch_size=args.batch_size,
        )
        results.append(summary)

    # Print summary
    import json

    print("\n=== Sync Summary ===")
    print(json.dumps(results, indent=2, ensure_ascii=False, default=str))

    total_matched = sum(r["matched"] for r in results)
    total_inserted = sum(r["inserted"] for r in results)
    total_bytes = sum(r["total_size_bytes"] for r in results)
    print(
        f"Total: {total_matched} matched, {total_inserted} inserted"
        f"{' (dry run)' if args.dry_run else ''}"
    )
    if total_bytes:
        size_mb = total_bytes / (1024 * 1024)
        print(f"Total data: {size_mb:.2f} MB")

    if args.dry_run:
        sys.exit(0)

    logger.info("Sync completed successfully.")


if __name__ == "__main__":
    main()
