# -*- coding: utf-8 -*-
"""Industry classification sync runner — entry point for scheduled sync.

Usage:
    python -m app.jobs.industry_sync_runner run [--dry-run] [--force-update]

Designed for deployment as a monthly K8s CronJob (see:
k8s/overlays/example-production/datahub-cronjobs.yaml
for the reference CronJob manifest). Runs monthly on the 1st at 12:00 CST.
"""

import argparse
import datetime
import json
import logging

from app.lib.utilities import job_run_helper

logger = logging.getLogger(__name__)

SYNC_JOB_FAMILY = "industry_sync"
SYNC_JOB_NAME = "datahub_industry_sync"
SYNC_JOB_TRIGGER = "cron"
SYNC_JOB_SOURCE = "k8s-cronjob"
SYNC_JOB_HOUR = 12
SYNC_JOB_MINUTE = 0


def _init_db_connection() -> None:
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher

    mongo_watcher.get_db_connection()


def run_sync(dry_run: bool = False, force_update: bool = False) -> dict:
    from app.lib.datahub.data_integrity_keeper.handler.industry_classification import (
        sync_industry_classification,
    )

    result = sync_industry_classification(
        dry_run=dry_run,
        force_update=force_update,
    )
    return result


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Industry Classification Sync Runner")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    p_run = subparsers.add_parser("run", help="Sync industry classification data")
    p_run.add_argument("--dry-run", action="store_true", help="Preview changes only")
    p_run.add_argument(
        "--force-update",
        action="store_true",
        help="Force update even if synced recently",
    )
    p_run.add_argument(
        "--job-name",
        default=SYNC_JOB_NAME,
        help="Job run name recorded in datahub_job_runs.",
    )
    p_run.add_argument(
        "--job-family",
        default=SYNC_JOB_FAMILY,
        help="Logical job family recorded in datahub_job_runs.",
    )
    p_run.add_argument(
        "--trigger",
        default=SYNC_JOB_TRIGGER,
        help="Trigger type recorded in datahub_job_runs.",
    )
    p_run.add_argument(
        "--source",
        default=SYNC_JOB_SOURCE,
        help="Source recorded in datahub_job_runs.",
    )
    p_run.add_argument(
        "--scheduled-hour",
        type=int,
        default=SYNC_JOB_HOUR,
        help="Scheduled hour.",
    )
    p_run.add_argument(
        "--scheduled-minute",
        type=int,
        default=SYNC_JOB_MINUTE,
        help="Scheduled minute.",
    )
    p_run.add_argument(
        "--scheduled-timezone",
        default=job_run_helper.BEIJING_TZ_NAME,
        help="Timezone for scheduled_at.",
    )
    p_run.add_argument(
        "--scheduled-at",
        default=None,
        help="Optional explicit scheduled_at timestamp in ISO format.",
    )

    args = parser.parse_args(argv)

    if args.command == "run":
        _run_with_tracking(args)
    else:
        parser.print_help()


def _run_with_tracking(args) -> None:
    _init_db_connection()

    scheduled_at = None
    if args.scheduled_at:
        scheduled_at = job_run_helper.normalize_datetime(
            datetime.datetime.fromisoformat(args.scheduled_at)
        )
    elif args.trigger in {"cron", "startup"}:
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
        extra={"dry_run": args.dry_run, "force_update": args.force_update},
    )
    job_run = job_run_helper.create_job_run(context)

    try:
        result = run_sync(
            dry_run=args.dry_run,
            force_update=args.force_update,
        )

        if result.get("status") == "GOOD":
            status = "SUCCESS"
        else:
            status = "FAILED"

        summary = {
            "total_processed": result.get("total_processed", 0),
            "new_classifications": result.get("new_classifications", 0),
            "updated_classifications": result.get("updated_classifications", 0),
        }
        job_run_helper.finish_job_run(
            job_run,
            status=status,
            summary=summary,
        )
        print(json.dumps(result, default=str, ensure_ascii=False, indent=2))

    except Exception as exc:
        logger.exception("Industry sync run failed")
        job_run_helper.finish_job_run(
            job_run,
            status="FAILED",
            summary={},
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    main()
