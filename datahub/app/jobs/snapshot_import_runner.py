# -*- coding: utf-8 -*-
"""Snapshot import runner — apply a controlled snapshot into this environment.

Verifies and applies a snapshot produced by snapshot_export_runner: the
manifest and every data file's sha256/document count/watermark are verified
BEFORE any local collection is mutated (fail-closed), then collections are
applied idempotently per class — business-key upserts for date-partitioned
collections, staging + atomic rename replacement for snapshot-class ones
(see app.lib.datahub.snapshot_transfer).

Usage:
    python -m app.jobs.snapshot_import_runner run [--snapshot-dir /work/snapshot/snapshot-XXXX]
                                                  [--snapshot-uri s3://bucket/prefix/snapshot-XXXX]
                                                  [--snapshot-id latest]
                                                  [--dry-run]

Snapshot source precedence: an explicit ``--snapshot-dir`` always wins;
otherwise ``--snapshot-uri s3://<bucket>/<key-prefix>`` downloads the
snapshot into ``<SNAPSHOT_DIR base>/downloads/<uri basename>`` (manifest
pair first, sidecar verified before any data file; the import afterwards
re-verifies everything) and imports from that directory; otherwise the
snapshot id is resolved under the snapshot base directory as before.

``--snapshot-id latest`` (default) picks the most recently written manifest
under the snapshot directory; a concrete snapshot id selects
``<snapshot-dir>/<snapshot-id>``. Pass ``--snapshot-dir`` to point at a
different base directory (or at a specific snapshot directory directly).

Environment:
    SNAPSHOT_DIR  Default snapshot base directory (default: /work/snapshot).
    IMAGE_SHA     Importing-environment image SHA recorded as a note.
    DATA_LAKE_ENDPOINT_URL / DATA_LAKE_REGION
                  Optional S3 endpoint/region for --snapshot-uri (mirrors the
                  parquet exporter's object-store settings).

This is the replacement for the online direct-sync path
(app.jobs.data_sync_runner): it never connects to a research or
legacy-stable online MongoDB and needs no MONGODB_SRC_* credentials.
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
SYNC_JOB_NAME = "datahub_snapshot_import"
SYNC_JOB_TRIGGER = "cli"
SYNC_JOB_SOURCE = "cli"


def _make_termination_handler(job_run):
    def _handle_termination(signum, _frame):
        signal_name = signal.Signals(signum).name
        message = f"Snapshot import terminated by signal {signal_name}"
        logger.error(message)
        try:
            job_run_helper.finish_job_run(
                job_run,
                status="FAILED",
                summary={"failed_phase": "snapshot_import"},
                error_message=message,
            )
        except Exception:
            logger.exception("Failed to persist terminated snapshot-import status")
        raise SystemExit(128 + signum)

    return _handle_termination


def _init_db_connection() -> None:
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher

    mongo_watcher.get_db_connection()


def run_import(args) -> dict:
    from app.lib.datahub import snapshot_transfer

    if args.snapshot_dir:
        # An explicit --snapshot-dir always wins over --snapshot-uri.
        base_dir = Path(args.snapshot_dir)
        snapshot_dir = snapshot_transfer.resolve_snapshot_dir(
            base_dir, args.snapshot_id
        )
    elif args.snapshot_uri:
        # Otherwise pull the snapshot from object storage into
        # <SNAPSHOT_DIR base>/downloads/<uri basename> and import from there;
        # endpoint/region default from DATA_LAKE_* inside download_snapshot.
        # Note: the download happens even with --dry-run — dry-run skips the
        # DB writes, not the transfer; the import itself re-verifies every
        # byte as defense in depth.
        download_dir = snapshot_transfer.download_dir_for_uri(
            snapshot_transfer.default_snapshot_dir(), args.snapshot_uri
        )
        snapshot_transfer.download_snapshot(args.snapshot_uri, download_dir)
        snapshot_dir = download_dir
    else:
        snapshot_dir = snapshot_transfer.resolve_snapshot_dir(
            snapshot_transfer.default_snapshot_dir(), args.snapshot_id
        )
    db = snapshot_transfer._get_local_db()
    return snapshot_transfer.run_import(
        db=db,
        snapshot_dir=snapshot_dir,
        dry_run=args.dry_run,
        producer_image_note=snapshot_transfer.resolve_producer_image(),
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Snapshot Import Runner")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    p_run = subparsers.add_parser(
        "run", help="Verify and import a dev-import snapshot (fail-closed)"
    )
    p_run.add_argument(
        "--snapshot-dir",
        default=None,
        help="Snapshot base directory (default: $SNAPSHOT_DIR or /work/snapshot)",
    )
    p_run.add_argument(
        "--snapshot-uri",
        default=None,
        help=(
            "Optional s3://<bucket>/<key-prefix> to download the snapshot "
            "from before importing (endpoint/region from DATA_LAKE_ENDPOINT_"
            "URL/DATA_LAKE_REGION); ignored when --snapshot-dir is given"
        ),
    )
    p_run.add_argument(
        "--snapshot-id",
        default="latest",
        help="Snapshot id under the base directory, or 'latest' (default: latest)",
    )
    p_run.add_argument(
        "--dry-run",
        action="store_true",
        help="Verify the snapshot only; write nothing",
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
            "snapshot_dir": args.snapshot_dir,
            "snapshot_uri": args.snapshot_uri,
            "snapshot_id": args.snapshot_id,
            "dry_run": args.dry_run,
        },
    )
    job_run = job_run_helper.create_job_run(context)
    previous_sigterm_handler = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, _make_termination_handler(job_run))

    try:
        result = run_import(args)
        status = "SUCCESS" if result.get("status") in ("GOOD", "DRY_RUN") else "FAILED"

        summary = {
            "snapshot_id": result.get("snapshot_id"),
            "snapshot_dir": result.get("snapshot_dir"),
            "manifest_sha256": result.get("manifest_sha256"),
            "producer_image": result.get("producer_image"),
            "total_docs": result.get("total_docs", 0),
            "collections_applied": result.get("collections_applied", 0),
            "collections": result.get("collections", {}),
            "asset_status_refresh": result.get("asset_status_refresh"),
            "elapsed_seconds": result.get("elapsed_seconds", 0),
        }
        job_run_helper.finish_job_run(job_run, status=status, summary=summary)
        print(json.dumps(result, default=str, ensure_ascii=False, indent=2))

    except Exception as exc:
        logger.exception("Snapshot import run failed")
        job_run_helper.finish_job_run(
            job_run, status="FAILED", summary={}, error_message=str(exc)
        )
        raise
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm_handler)


if __name__ == "__main__":
    main()
