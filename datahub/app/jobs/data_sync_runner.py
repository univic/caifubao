# -*- coding: utf-8 -*-
"""Data sync runner — pull external data from prod MongoDB into dev.

Usage:
    python -m app.jobs.data_sync_runner run [--collections quote,industry]
                                            [--from-date 2026-04-01]
                                            [--dry-run]

Designed for deployment as a K8s CronJob (optional). In dev environments,
this can be triggered on-demand or scheduled (e.g. nightly) to keep
external data up-to-date without re-pulling from baostock.
"""

import argparse
import datetime
import json
import logging

from app.lib.utilities import job_run_helper

logger = logging.getLogger(__name__)

SYNC_JOB_FAMILY = "data_sync"
SYNC_JOB_NAME = "datahub_data_sync"
SYNC_JOB_TRIGGER = "startup"
SYNC_JOB_SOURCE = "cli"


def _init_db_connection() -> None:
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher

    mongo_watcher.get_db_connection()


def parse_date(value: str | None) -> datetime.datetime | None:
    if not value:
        return None
    return datetime.datetime.strptime(value, "%Y-%m-%d")


def run_sync(args) -> dict:
    from app.lib.datahub.sync_engine import run_sync as engine_run

    collections = args.collections.split(",") if args.collections else None
    result = engine_run(
        dry_run=args.dry_run,
        collections=collections,
        from_date=parse_date(args.from_date),
        to_date=parse_date(args.to_date),
    )
    return result


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Data Sync Runner")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    p_run = subparsers.add_parser("run", help="Sync data from prod to dev")
    p_run.add_argument(
        "--collections",
        default=None,
        help="Comma-separated collection names (default: all)",
    )
    p_run.add_argument(
        "--from-date",
        default=None,
        help="Sync only records on or after this date (YYYY-MM-DD)",
    )
    p_run.add_argument(
        "--to-date",
        default=None,
        help="Sync only records on or before this date (YYYY-MM-DD)",
    )
    p_run.add_argument("--dry-run", action="store_true", help="Read only, no write")
    p_run.add_argument(
        "--job-name", default=SYNC_JOB_NAME, help="Job name for run tracking"
    )
    p_run.add_argument(
        "--job-family", default=SYNC_JOB_FAMILY, help="Job family for run tracking"
    )
    p_run.add_argument("--trigger", default=SYNC_JOB_TRIGGER)
    p_run.add_argument("--source", default=SYNC_JOB_SOURCE)

    args = parser.parse_args(argv)

    if args.command == "run":
        _run_with_tracking(args)
    else:
        parser.print_help()


def _run_with_tracking(args) -> None:
    _init_db_connection()
    scheduled_at = job_run_helper.utc_now_naive()

    context = job_run_helper.JobRunContext(
        job_name=args.job_name,
        job_family=args.job_family,
        trigger=args.trigger,
        source=args.source,
        scheduled_at=scheduled_at,
        extra={
            "collections": args.collections,
            "from_date": args.from_date,
            "to_date": args.to_date,
            "dry_run": args.dry_run,
        },
    )
    job_run = job_run_helper.create_job_run(context)

    try:
        result = run_sync(args)
        status = "SUCCESS" if result.get("status") == "GOOD" else "FAILED"

        summary = {
            "total_read": result.get("total_read", 0),
            "total_upserted": result.get("total_upserted", 0),
            "collections_synced": result.get("collections_synced", 0),
            "elapsed_seconds": result.get("elapsed_seconds", 0),
        }
        job_run_helper.finish_job_run(job_run, status=status, summary=summary)
        print(json.dumps(result, default=str, ensure_ascii=False, indent=2))

    except Exception as exc:
        logger.exception("Data sync run failed")
        job_run_helper.finish_job_run(
            job_run, status="FAILED", summary={}, error_message=str(exc)
        )
        raise


if __name__ == "__main__":
    main()
