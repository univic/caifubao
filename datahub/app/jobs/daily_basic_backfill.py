"""Backfill stock_daily_basic from tushare pro.daily_basic, one date at a time.

Research/dev ingestion for valuation factors (option A of the fundamental
factors design). Pulls each trade date's full-market daily_basic snapshot,
normalizes to canonical rows, and bulk-upserts into stock_daily_basic keyed by
(code, date).

Resume safety: a per-date completion marker is written to
stock_daily_basic_backfill_state only after every batch of that date's upserts
succeeds. A date is skipped on resume only when its marker exists, so an
interrupted run heals partially-written dates (idempotent upserts) instead of
silently leaving them gap. Dates that come back empty or fail after retries
are recorded with their status and retried on the next run — a single bad date
never blocks the rest of the window.

Usage:
    python -m app.jobs.daily_basic_backfill --from-date 2019-01-01
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging

from app.lib.db_watcher.mongoengine_tool import mongo_watcher
from app.lib.datahub.data_source.handler import zh_a_daily_basic

logger = logging.getLogger(__name__)

BATCH_SIZE = 500
BACKFILL_STATE_COLLECTION = "stock_daily_basic_backfill_state"


def _init_db_connection() -> None:
    mongo_watcher.get_db_connection()


def _database():
    from mongoengine import get_connection

    from app.conf import app_config as cfg

    return get_connection()[cfg.MONGODB_DB]


def _daily_basic_collection():
    return _database()["stock_daily_basic"]


def _state_collection():
    return _database()[BACKFILL_STATE_COLLECTION]


def _quote_dates(
    from_date: datetime.date, to_date: datetime.date
) -> list[datetime.datetime]:
    """Trade dates that actually have quote data in the window (sorted)."""
    db = _database()
    dates = db["stock_daily_quote"].distinct(
        "date",
        {
            "date": {
                "$gte": datetime.datetime.combine(from_date, datetime.time.min),
                "$lte": datetime.datetime.combine(to_date, datetime.time.max),
            }
        },
    )
    return sorted(dates)


def _max_quote_date() -> datetime.datetime | None:
    db = _database()
    return (
        db["stock_daily_quote"]
        .find_one({}, {"date": 1, "_id": 0}, sort=[("date", -1)])
        .get("date")
    )


def _done_marker_dates(
    from_date: datetime.date, to_date: datetime.date
) -> set[datetime.datetime]:
    """Dates whose upserts completed (every batch of that date succeeded)."""
    state = _state_collection()
    return set(
        state.distinct(
            "_id",
            {
                "_id": {
                    "$gte": from_date.isoformat(),
                    "$lte": to_date.isoformat(),
                },
                "status": "done",
            },
        )
    )


def _record_date_state(
    trade_date: datetime.datetime, status: str, error: str | None = None
) -> None:
    _state_collection().update_one(
        {"_id": trade_date.strftime("%Y-%m-%d")},
        {
            "$set": {
                "status": status,
                "updated_at": datetime.datetime.now(datetime.UTC),
                **({"error": error[:500]} if error else {}),
            }
        },
        upsert=True,
    )


def backfill(from_date: datetime.date, to_date: datetime.date) -> dict:
    from pymongo import UpdateOne

    collection = _daily_basic_collection()
    collection.create_index([("code", 1), ("date", 1)], unique=True)
    # data-sync (sync_engine) requires a date-first index on source and
    # destination before incremental sync of this collection.
    collection.create_index([("date", -1)])

    trade_dates = _quote_dates(from_date, to_date)
    done = _done_marker_dates(from_date, to_date)
    stats = {
        "trade_dates_found": len(trade_dates),
        "dates_skipped_done": 0,
        "dates_fetched": 0,
        "rows_upserted": 0,
        "dates_empty": 0,
        "dates_failed": 0,
    }

    for trade_date in trade_dates:
        if trade_date.strftime("%Y-%m-%d") in done:
            stats["dates_skipped_done"] += 1
            continue
        compact = trade_date.strftime("%Y%m%d")
        try:
            rows = zh_a_daily_basic.fetch_and_normalize(compact)
        except RuntimeError as exc:
            message = str(exc)
            status = "empty" if "returned no rows" in message else "failed"
            _record_date_state(trade_date, status, message)
            stats[f"dates_{status}"] += 1
            logger.warning("daily_basic %s: %s (%s)", compact, status, message)
            continue
        except Exception as exc:  # noqa: BLE001 - isolate per-date failures
            _record_date_state(trade_date, "failed", str(exc))
            stats["dates_failed"] += 1
            logger.exception("daily_basic %s: failed", compact)
            continue
        if not rows:
            _record_date_state(trade_date, "empty")
            stats["dates_empty"] += 1
            logger.warning("no daily_basic rows for %s", compact)
            continue

        batch = [
            UpdateOne(
                {"code": row["code"], "date": row["date"]},
                {"$set": row},
                upsert=True,
            )
            for row in rows
        ]
        for offset in range(0, len(batch), BATCH_SIZE):
            result = collection.bulk_write(
                batch[offset : offset + BATCH_SIZE], ordered=False
            )
            stats["rows_upserted"] += result.upserted_count + result.modified_count
        _record_date_state(trade_date, "done")
        stats["dates_fetched"] += 1
        if stats["dates_fetched"] % 50 == 0:
            logger.info(
                "backfill progress: %s fetched / %s skipped / %s total, %s rows upserted",
                stats["dates_fetched"],
                stats["dates_skipped_done"],
                stats["trade_dates_found"],
                stats["rows_upserted"],
            )
    return stats


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill stock_daily_basic")
    parser.add_argument("--from-date", default="2019-01-01", help="YYYY-MM-DD")
    parser.add_argument(
        "--to-date",
        default=None,
        help="YYYY-MM-DD (default: latest date present in stock_daily_quote)",
    )
    return parser


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO)
    args = build_parser().parse_args(argv)
    _init_db_connection()
    from_date = datetime.date.fromisoformat(args.from_date)
    max_quote = _max_quote_date()
    to_date = (
        datetime.date.fromisoformat(args.to_date)
        if args.to_date
        else max_quote.date()
        if max_quote
        else from_date
    )
    stats = backfill(from_date, to_date)
    print(json.dumps(stats, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
