# -*- coding: utf-8 -*-
"""Snapshot export runner — write a controlled dev-import snapshot.

Exports the allow-list collections (SYNCABLE_COLLECTIONS) from the local
database into a versioned, checksummed snapshot directory. The snapshot is
the only external source dev's snapshot import accepts; it is produced on
the research/data plane and transferred as files (no online MongoDB
connection is required on the consuming side).

Usage:
    python -m app.jobs.snapshot_export_runner run [--collections quote,daily_basic]
                                                  [--from-date 2026-04-01]
                                                  [--to-date 2026-04-30]
                                                  [--out-dir /work/snapshot]
                                                  [--upload-uri s3://bucket/prefix]
                                                  [--dry-run]

Collection aliases (resolved against SNAPSHOT_EXPORT_COLLECTIONS):
    quote       -> stock_daily_quote
    daily_basic -> stock_daily_basic
    factor      -> stock_factor_daily
    signal      -> stock_signal_daily
    market      -> finance_market
    industry    -> stock_industry

Environment:
    SNAPSHOT_DIR  Default output directory (default: /work/snapshot).
    IMAGE_SHA     Producer image SHA recorded in the manifest (fallback:
                  container hostname).
    DATA_LAKE_ENDPOINT_URL / DATA_LAKE_REGION
                  Optional S3 endpoint/region for --upload-uri (mirrors the
                  parquet exporter's object-store settings).

With ``--upload-uri s3://<bucket>/<key-prefix>`` the exported snapshot is
uploaded after a successful (non-dry-run) export: manifest.json, its
checksum sidecar, then every manifest-listed data file, under derived
object keys ``<prefix>/<file>``. A failed upload marks the job run FAILED;
local snapshot files are never deleted, so the upload can be retried.

Designed to run as a K8s CronJob or on-demand CLI step on the producing
deployment; dev consumes the snapshot via snapshot_import_runner.
"""

import argparse
import datetime
import json
import logging
import signal
from pathlib import Path

from app.lib.utilities import job_run_helper

logger = logging.getLogger(__name__)

SYNC_JOB_FAMILY = "snapshot_transfer"
SYNC_JOB_NAME = "datahub_snapshot_export"
SYNC_JOB_TRIGGER = "cli"
SYNC_JOB_SOURCE = "cli"


def _make_termination_handler(job_run):
    def _handle_termination(signum, _frame):
        signal_name = signal.Signals(signum).name
        message = f"Snapshot export terminated by signal {signal_name}"
        logger.error(message)
        try:
            job_run_helper.finish_job_run(
                job_run,
                status="FAILED",
                summary={"failed_phase": "snapshot_export"},
                error_message=message,
            )
        except Exception:
            logger.exception("Failed to persist terminated snapshot-export status")
        raise SystemExit(128 + signum)

    return _handle_termination


def _init_db_connection() -> None:
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher

    mongo_watcher.get_db_connection()


def parse_date(value: str | None) -> datetime.datetime | None:
    if not value:
        return None
    return datetime.datetime.strptime(value, "%Y-%m-%d")


def run_export(args) -> dict:
    from app.lib.datahub import snapshot_transfer

    collections = args.collections.split(",") if args.collections else None
    out_dir = (
        Path(args.out_dir) if args.out_dir else snapshot_transfer.default_snapshot_dir()
    )
    db = snapshot_transfer._get_local_db()
    result = snapshot_transfer.run_export(
        db=db,
        out_dir=out_dir,
        collections=collections,
        from_date=parse_date(args.from_date),
        to_date=parse_date(args.to_date),
        producer_image=snapshot_transfer.resolve_producer_image(),
        dry_run=args.dry_run,
    )
    if args.upload_uri and not result.get("dry_run"):
        # endpoint/region default from DATA_LAKE_* inside upload_snapshot
        result["upload"] = snapshot_transfer.upload_snapshot(
            Path(result["snapshot_dir"]), args.upload_uri
        )
    return result


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Snapshot Export Runner")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    p_run = subparsers.add_parser(
        "run", help="Export a checksummed dev-import snapshot"
    )
    p_run.add_argument(
        "--collections",
        default=None,
        help="Comma-separated collection names or aliases (default: all)",
    )
    p_run.add_argument(
        "--from-date",
        default=None,
        help="Export date-partitioned collections from this date (YYYY-MM-DD)",
    )
    p_run.add_argument(
        "--to-date",
        default=None,
        help="Export date-partitioned collections up to this date (YYYY-MM-DD)",
    )
    p_run.add_argument(
        "--out-dir",
        default=None,
        help="Snapshot output directory (default: $SNAPSHOT_DIR or /work/snapshot)",
    )
    p_run.add_argument(
        "--upload-uri",
        default=None,
        help=(
            "Optional s3://<bucket>/<key-prefix> to upload the exported "
            "snapshot to (endpoint/region from DATA_LAKE_ENDPOINT_URL/"
            "DATA_LAKE_REGION)"
        ),
    )
    p_run.add_argument(
        "--dry-run", action="store_true", help="Print the plan, no write"
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
    _run_with_tracking(args)


def _run_with_tracking(args) -> None:
    _init_db_connection()
    try:
        job_run_helper.mark_stale_running_job_runs_failed(job_family=args.job_family)
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
            "out_dir": args.out_dir,
            "upload_uri": args.upload_uri,
            "dry_run": args.dry_run,
        },
    )
    job_run = job_run_helper.create_job_run(context)
    previous_sigterm_handler = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, _make_termination_handler(job_run))

    try:
        result = run_export(args)
        status = "SUCCESS" if result.get("status") in ("GOOD", "DRY_RUN") else "FAILED"

        summary = {
            "snapshot_id": result.get("snapshot_id"),
            "snapshot_dir": result.get("snapshot_dir"),
            "manifest_sha256": result.get("manifest_sha256"),
            "producer_image": result.get("producer_image"),
            "total_docs": result.get("total_docs", 0),
            "collections_exported": result.get("collections_exported", 0),
            "collections": result.get("collections", {}),
            "upload": result.get("upload"),
            "elapsed_seconds": result.get("elapsed_seconds", 0),
        }
        job_run_helper.finish_job_run(job_run, status=status, summary=summary)
        print(json.dumps(result, default=str, ensure_ascii=False, indent=2))

    except Exception as exc:
        logger.exception("Snapshot export run failed")
        job_run_helper.finish_job_run(
            job_run, status="FAILED", summary={}, error_message=str(exc)
        )
        raise
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm_handler)


if __name__ == "__main__":
    main()
