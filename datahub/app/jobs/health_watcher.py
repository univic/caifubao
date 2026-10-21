"""Health watcher — report job-run failures and data freshness gaps.

Usage:
    python -m app.jobs.health_watcher [--hours 24] [--webhook URL] [--fail-on-issues]

Checks (read-only):
1. Recent FAILED/SKIPPED/DEGRADED job runs in ``datahub_job_runs``. DEGRADED
   runs are reported as not-clean: the pipeline stayed usable, but part of the
   universe did not validate.
2. Data freshness: ``data_asset_status`` NO_DATA counts and STALE counts per
   asset type (quote / factor / signal), with latest-date distributions.

Outputs a JSON report to stdout (and posts it to the webhook URL when
configured via env ``HEALTH_WEBHOOK_URL`` or ``--webhook``). Exits 1 (with
``--fail-on-issues``) when issues are found — FAILED or DEGRADED jobs, or
NO_DATA assets; STALE assets are reported but not fatal (suspended stocks
legitimately go stale). A CronJob turns the exit code into a visible failure
signal.

This is the P0 alerting gap from docs/operations/roadmap-2026-08.md: job
failures went unnoticed for 3 consecutive trading days (2026-08-26..28).
"""

import argparse
import datetime
import json
import logging
import os
import sys
from collections import Counter
from typing import Any

from app.lib.utilities import job_run_helper

logger = logging.getLogger(__name__)

HEALTH_WEBHOOK_ENV = "HEALTH_WEBHOOK_URL"

# Job names worth watching (the daily market-data chain). These are the
# names the k8s CronJobs record via --job-name (see
# k8s/overlays/example-development/datahub-cronjobs.yaml); manual
# `scripts/caifubao` runs use different defaults and are out of scope.
WATCHED_JOB_NAMES = (
    "datahub_quote_stock_daily",
    "datahub_quote_index_daily",
    "datahub_signal_daily",
    "datahub_scoring_daily",
    "datahub_data_sync_daily",
)

FRESHNESS_ASSET_TYPES = ("quote", "factor", "signal")


def _validation_details(record) -> dict[str, Any]:
    """Best-effort extraction of the persisted quote/index validation dict."""
    summary = record.summary or {}
    validation = summary.get("quote_validation") or summary.get("index_validation")
    if validation:
        return validation
    for phase in (record.phase_stats or {}).values():
        if not isinstance(phase, dict):
            continue
        validation = phase.get("quote_validation") or phase.get("index_validation")
        if validation:
            return validation
    return {}


def _build_report(
    *,
    failed_jobs: list[dict[str, Any]],
    skipped_jobs: list[dict[str, Any]],
    stale_assets: list[dict[str, Any]],
    no_data_assets: list[dict[str, Any]],
    latest_dates: dict[str, Counter],
    degraded_jobs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Assemble the JSON report; pure function for testability.

    Issues (make the report unhealthy): FAILED and DEGRADED job runs and
    NO_DATA assets. A DEGRADED run kept the chain usable, but it is explicitly
    *not* clean: the operator must still see the partial coverage. STALE assets
    are reported for visibility but are not fatal — suspended stocks
    legitimately stay STALE (e.g. the 4 known prod suspension cases).
    """
    degraded_jobs = degraded_jobs or []
    issues: list[str] = []
    for entry in failed_jobs:
        issues.append(
            f"job {entry['job_name']} FAILED at {entry['started_at']} "
            f"(error: {(entry.get('error_message') or 'none')[:120]})"
        )
    for entry in degraded_jobs:
        details = []
        if entry.get("coverage_rate") is not None:
            details.append(f"coverage={entry['coverage_rate']}")
        if entry.get("failed_count"):
            details.append(f"failed={entry['failed_count']}")
        detail_text = f" ({', '.join(details)})" if details else ""
        issues.append(
            f"job {entry['job_name']} DEGRADED at {entry['started_at']}{detail_text}"
        )
    if no_data_assets:
        issues.append(f"{len(no_data_assets)} NO_DATA assets")

    return {
        "checked_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "healthy": not issues,
        "issues": issues,
        "job_runs": {
            "failed": failed_jobs,
            "skipped": skipped_jobs,
            "degraded": degraded_jobs,
        },
        "freshness": {
            "stale_assets": stale_assets[:20],
            "stale_count": len(stale_assets),
            "no_data_assets": no_data_assets[:20],
            "no_data_count": len(no_data_assets),
            "latest_dates": {
                asset_type: dict(counter.most_common(5))
                for asset_type, counter in latest_dates.items()
            },
        },
    }


def run_checks(*, hours: int = 24) -> dict[str, Any]:
    """Run the read-only checks against datahub_job_runs and data_asset_status."""
    cutoff = job_run_helper.utc_now_naive() - datetime.timedelta(hours=hours)

    failed_jobs: list[dict[str, Any]] = []
    skipped_jobs: list[dict[str, Any]] = []
    degraded_jobs: list[dict[str, Any]] = []
    for record in (
        job_run_helper.DatahubJobRun.objects(
            job_name__in=list(WATCHED_JOB_NAMES),
            status__in=[
                job_run_helper.STATUS_FAILED,
                job_run_helper.STATUS_SKIPPED,
                job_run_helper.STATUS_DEGRADED,
            ],
            started_at__gte=cutoff,
        )
        .order_by("-started_at")
        .limit(100)
    ):
        entry = {
            "job_name": record.job_name,
            "status": record.status,
            "started_at": str(record.started_at)[:19],
            "error_message": record.error_message,
        }
        if record.status == job_run_helper.STATUS_FAILED:
            failed_jobs.append(entry)
        elif record.status == job_run_helper.STATUS_DEGRADED:
            validation = _validation_details(record)
            entry["coverage_rate"] = validation.get("coverage_rate")
            entry["failed_count"] = validation.get("failed_count")
            entry["failed_codes"] = list(validation.get("failed_codes") or [])
            entry["skipped_indices"] = list(validation.get("skipped_indices") or [])
            degraded_jobs.append(entry)
        else:
            skipped_jobs.append(entry)

    from app.model.data_asset_status import (
        STATUS_NO_DATA,
        STATUS_STALE,
        DataAssetStatus,
    )

    stale_assets: list[dict[str, Any]] = []
    no_data_assets: list[dict[str, Any]] = []
    latest_dates: dict[str, Counter] = {}
    cursor = DataAssetStatus._get_collection().find(
        {"asset_type": {"$in": list(FRESHNESS_ASSET_TYPES)}},
        {
            "asset_type": 1,
            "status": 1,
            "latest_data_date": 1,
            "code": 1,
            "asset_name": 1,
        },
    )
    for doc in cursor:
        asset_type = doc.get("asset_type", "?")
        status = doc.get("status", "?")
        latest_dates.setdefault(asset_type, Counter())[
            str(doc.get("latest_data_date"))[:10]
        ] += 1
        if status == STATUS_STALE:
            stale_assets.append(
                {
                    "code": doc.get("code"),
                    "asset": doc.get("asset_name"),
                    "latest": str(doc.get("latest_data_date"))[:10],
                }
            )
        elif status == STATUS_NO_DATA:
            no_data_assets.append(
                {"code": doc.get("code"), "asset": doc.get("asset_name")}
            )

    return _build_report(
        failed_jobs=failed_jobs,
        skipped_jobs=skipped_jobs,
        stale_assets=stale_assets,
        no_data_assets=no_data_assets,
        latest_dates=latest_dates,
        degraded_jobs=degraded_jobs,
    )


def _post_webhook(url: str, report: dict[str, Any]) -> None:
    import requests

    try:
        response = requests.post(url, json=report, timeout=10)
        response.raise_for_status()
        logger.info("Health report posted to webhook (status=%s)", response.status_code)
    except Exception as exc:  # noqa: BLE001 - never fail the watcher on notify errors
        logger.warning("Failed to post health report to webhook: %s", exc)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run datahub health checks.")
    parser.add_argument("--hours", type=int, default=24, help="Look-back window (>=1).")
    parser.add_argument("--webhook", default=None, help="Webhook URL (overrides env).")
    parser.add_argument(
        "--fail-on-issues",
        action="store_true",
        help="Exit 1 when issues are found (CronJob-friendly).",
    )
    args = parser.parse_args(argv)
    if args.hours < 1:
        parser.error("--hours must be >= 1")

    # Same connection bootstrap as every other runner (signal/data_sync).
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher

    mongo_watcher.get_db_connection()

    report = run_checks(hours=args.hours)
    print(json.dumps(report, ensure_ascii=False, indent=2))

    webhook = args.webhook or os.getenv(HEALTH_WEBHOOK_ENV)
    if webhook:
        _post_webhook(webhook, report)

    if args.fail_on_issues and not report["healthy"]:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
