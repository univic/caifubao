"""Backfill stock_daily_basic from tushare pro.daily_basic, one date at a time.

Research/dev ingestion for valuation factors (option A of the fundamental
factors design). Pulls each trade date's full-market daily_basic snapshot,
normalizes to canonical rows, and bulk-upserts into stock_daily_basic keyed by
(code, date), so re-runs are idempotent and safe to interrupt.

Usage:
    python -m app.jobs.daily_basic_backfill --from-date 2019-01-01 --to-date 2026-07-31
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


def _init_db_connection() -> None:
    mongo_watcher.get_db_connection()


def _database_daily_basic_collection():
    from mongoengine import get_connection

    from app.conf import app_config as cfg

    return get_connection()[cfg.MONGODB_DB]["stock_daily_basic"]


def _quote_dates(
    from_date: datetime.date, to_date: datetime.date
) -> list[datetime.datetime]:
    """Trade dates that actually have quote data in the window (sorted)."""
    db = _database_daily_basic_collection().database
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


def backfill(from_date: datetime.date, to_date: datetime.date) -> dict:
    from pymongo import UpdateOne

    collection = _database_daily_basic_collection()
    collection.create_index([("code", 1), ("date", 1)], unique=True)

    trade_dates = _quote_dates(from_date, to_date)
    stats = {
        "trade_dates_found": len(trade_dates),
        "dates_fetched": 0,
        "rows_upserted": 0,
    }

    for trade_date in trade_dates:
        compact = trade_date.strftime("%Y%m%d")
        rows = zh_a_daily_basic.fetch_and_normalize(compact)
        if not rows:
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
        stats["dates_fetched"] += 1
        if stats["dates_fetched"] % 50 == 0:
            logger.info(
                "backfill progress: %s/%s dates, %s rows upserted",
                stats["dates_fetched"],
                stats["trade_dates_found"],
                stats["rows_upserted"],
            )
    return stats


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill stock_daily_basic")
    parser.add_argument("--from-date", default="2019-01-01", help="YYYY-MM-DD")
    parser.add_argument("--to-date", default="2026-07-31", help="YYYY-MM-DD")
    return parser


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO)
    args = build_parser().parse_args(argv)
    _init_db_connection()
    from_date = datetime.date.fromisoformat(args.from_date)
    to_date = datetime.date.fromisoformat(args.to_date)
    stats = backfill(from_date, to_date)
    print(json.dumps(stats, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
