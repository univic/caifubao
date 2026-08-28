"""Health watcher — report job-run failures and data freshness gaps.

Usage:
    python -m app.jobs.health_watcher run [--hours 24] [--webhook URL]

Checks (read-only):
1. Recent FAILED/SKIPPED job runs in ``datahub_job_runs``.
2. Data freshness: ``data_asset_status`` STALE / NO_DATA counts and the
   latest data date distribution per asset type (quote / factor / signal).

Outputs a JSON report to stdout (and logs it), and posts it to the webhook
URL when configured (env ``HEALTH_WEBHOOK_URL`` or ``--webhook``). Exits 0
when healthy, 1 when failures or freshness gaps are found — a CronJob can
turn that into a visible failure signal.

This is the P0 alerting gap from docs/operations/roadmap-2026-08.md: job
failures went unnoticed for 4 consecutive trading days (2026-08-26..28).
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

# Job names worth watching (the daily market-data chain).
WATCHED_JOB_NAMES = (
    "datahub_quote_stock_daily",
    "datahub_quote_index_daily",
    "datahub_signal_daily",
    "datahub_scoring_daily",
    "datahub_data_sync_daily",
)

FRESHNESS_ASSET_TYPES = ("quote", "factor", "signal")


def _build_report(
    *,
    failed_jobs: list[dict[str, Any]],
    skipped_jobs: list[dict[str, Any]],
    stale_assets: list[dict[str, Any]],
    no_data_assets: list[dict[str, Any]],
    latest_dates: dict[str, Counter],
) -> dict[str, Any]:
    """Assemble the JSON report; pure function for testability."""
    issues: list[str] = []
    for entry in failed_jobs:
        issues.append(
            f"job {entry['job_name']} FAILED at {entry['started_at']} "
            f"(error: {(entry.get('error_message') or 'none')[:120]})"
        )
    if stale_assets:
        issues.append(f"{len(stale_assets)} stale data assets")
    if no_data_assets:
        issues.append(f"{len(no_data_assets)} NO_DATA assets")

    return {
        "checked_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "healthy": not issues,
        "issues": issues,
        "job_runs": {
            "failed": failed_jobs,
            "skipped": skipped_jobs,
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
    for record in (
        job_run_helper.DatahubJobRun.objects(
            job_name__in=list(WATCHED_JOB_NAMES),
            status__in=[job_run_helper.STATUS_FAILED, job_run_helper.STATUS_SKIPPED],
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
    parser.add_argument("--hours", type=int, default=24, help="Look-back window.")
    parser.add_argument("--webhook", default=None, help="Webhook URL (overrides env).")
    parser.add_argument(
        "--fail-on-issues",
        action="store_true",
        help="Exit 1 when issues are found (CronJob-friendly).",
    )
    args = parser.parse_args(argv)

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
