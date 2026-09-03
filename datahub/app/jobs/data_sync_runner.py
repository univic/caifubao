# -*- coding: utf-8 -*-
"""Data sync runner — pull external data from prod MongoDB into dev.

Usage:
    python -m app.jobs.data_sync_runner run [--collections quote,industry]
                                            [--from-date 2026-04-01]
                                            [--full]
                                            [--overlap-days 3]
                                            [--dry-run]

Collection aliases (resolved against SYNCABLE_COLLECTIONS):
    quote       -> stock_daily_quote
    daily_basic -> stock_daily_basic
    factor      -> stock_factor_daily
    signal      -> stock_signal_daily
    market      -> finance_market
    industry    -> stock_industry

Designed for deployment as a K8s CronJob (optional). In dev environments,
this can be triggered on-demand or scheduled (e.g. nightly) to keep
external data up-to-date without re-pulling from baostock.
"""

import argparse
import datetime
import json
import logging
import signal

from app.lib.utilities import job_run_helper

logger = logging.getLogger(__name__)

SYNC_JOB_FAMILY = "data_sync"
SYNC_JOB_NAME = "datahub_data_sync"
SYNC_JOB_TRIGGER = "startup"
SYNC_JOB_SOURCE = "cli"


def _make_termination_handler(job_run):
    def _handle_termination(signum, _frame):
        signal_name = signal.Signals(signum).name
        message = f"Data sync terminated by signal {signal_name}"
        logger.error(message)
        try:
            job_run_helper.finish_job_run(
                job_run,
                status="FAILED",
                summary={"failed_phase": "data_sync"},
                error_message=message,
            )
        except Exception:
            logger.exception("Failed to persist terminated data-sync status")
        raise SystemExit(128 + signum)

    return _handle_termination


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
        full_sync=args.full,
        overlap_days=args.overlap_days,
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
        "--full",
        action="store_true",
        help="Disable destination-watermark incremental sync",
    )
    p_run.add_argument(
        "--overlap-days",
        type=int,
        default=3,
        help="Calendar days before the destination watermark to replay (default: 3)",
    )
    p_run.add_argument(
        "--job-name", default=SYNC_JOB_NAME, help="Job name for run tracking"
    )
    p_run.add_argument(
        "--job-family", default=SYNC_JOB_FAMILY, help="Job family for run tracking"
    )
    p_run.add_argument("--trigger", default=SYNC_JOB_TRIGGER)
    p_run.add_argument("--source", default=SYNC_JOB_SOURCE)
    p_run.add_argument(
        "--scheduled-hour",
        type=int,
        default=None,
        help="Scheduled hour in the configured timezone for cron/startup recording.",
    )
    p_run.add_argument(
        "--scheduled-minute",
        type=int,
        default=None,
        help="Scheduled minute in the configured timezone for cron/startup recording.",
    )
    p_run.add_argument(
        "--scheduled-timezone",
        default=job_run_helper.BEIJING_TZ_NAME,
        help="Timezone used to derive scheduled_at when running as cron/startup.",
    )
    p_run.add_argument(
        "--scheduled-at",
        default=None,
        help="Optional explicit scheduled_at timestamp in ISO format.",
    )

    args = parser.parse_args(argv)

    if args.command != "run":
        parser.print_help()
        return
    if args.full and (args.from_date or args.to_date):
        parser.error("--full cannot be combined with --from-date or --to-date")
    if args.overlap_days < 0:
        parser.error("--overlap-days must be a non-negative integer")
    if args.full:
        if args.job_name == SYNC_JOB_NAME:
            args.job_name = f"{SYNC_JOB_NAME}_full"
        if args.job_family == SYNC_JOB_FAMILY:
            args.job_family = f"{SYNC_JOB_FAMILY}_full"
    _run_with_tracking(args)


def _run_with_tracking(args) -> None:
    _init_db_connection()
    try:
        cleanup_kwargs = {"job_family": args.job_family}
        if args.full:
            cleanup_kwargs["max_age_minutes"] = 1440
        job_run_helper.mark_stale_running_job_runs_failed(**cleanup_kwargs)
    except Exception:
        logger.exception("Stale RUNNING job-run cleanup failed; continuing")
    if args.scheduled_at:
        scheduled_at = job_run_helper.normalize_datetime(
            datetime.datetime.fromisoformat(args.scheduled_at)
        )
    elif (
        args.trigger in {"cron", "startup"}
        and args.scheduled_hour is not None
        and args.scheduled_minute is not None
    ):
        scheduled_at = job_run_helper.compute_daily_schedule_at(
            args.scheduled_hour,
            args.scheduled_minute,
            timezone_name=args.scheduled_timezone,
        )
    else:
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
            "full": args.full,
            "overlap_days": args.overlap_days,
        },
    )
    job_run = job_run_helper.create_job_run(context)
    previous_sigterm_handler = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, _make_termination_handler(job_run))

    try:
        result = run_sync(args)
        status = "SUCCESS" if result.get("status") in ("GOOD", "DRY_RUN") else "FAILED"

        summary = {
            "total_read": result.get("total_read", 0),
            "total_upserted": result.get("total_upserted", 0),
            "total_modified": result.get("total_modified", 0),
            "collections_synced": result.get("collections_synced", 0),
            "collections": result.get("collections", {}),
            "elapsed_seconds": result.get("elapsed_seconds", 0),
            "full_sync": result.get("full_sync", False),
            "overlap_days": result.get("overlap_days", args.overlap_days),
        }
        job_run_helper.finish_job_run(job_run, status=status, summary=summary)
        print(json.dumps(result, default=str, ensure_ascii=False, indent=2))

    except Exception as exc:
        logger.exception("Data sync run failed")
        job_run_helper.finish_job_run(
            job_run, status="FAILED", summary={}, error_message=str(exc)
        )
        raise
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm_handler)


if __name__ == "__main__":
    main()
