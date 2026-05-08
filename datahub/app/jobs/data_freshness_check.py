"""Monitor data freshness after the daily pipeline completes.

Queries DataAssetStatus for quote, signal, and scoring assets, comparing
latest_data_date against the most recent trading day, and reports counts
of up-to-date vs lagging stocks.

Intended to run as a CronJob after the daily pipeline (~19:00).
"""

from __future__ import annotations

import argparse
import datetime
import logging
import sys

from app.lib.db_watcher.mongoengine_tool import mongo_watcher
from app.lib.utilities import job_run_helper
from app.lib.utilities.trading_day_helper import (
    determine_most_recent_previous_complete_trading_day,
    get_a_stock_market_trade_calendar,
)
from app.model.data_asset_status import DataAssetStatus
from app.model.datahub_job_run import DatahubJobRun

logger = logging.getLogger(__name__)

MONITOR_JOB_FAMILY = "freshness_check"
MONITOR_JOB_NAME = "datahub_freshness_check"
FRESHNESS_DIFF_DAYS = 3

# Map freshness result status to job run status
STATUS_MAP: dict[str, str] = {
    "OK": "SUCCESS",
    "WARN": "SUCCESS",
    "FAILED": "FAILED",
}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check data freshness after daily pipeline."
    )
    parser.add_argument(
        "--job-name",
        default=MONITOR_JOB_NAME,
        help="Job run name (default: %(default)s).",
    )
    parser.add_argument(
        "--job-family",
        default=MONITOR_JOB_FAMILY,
        help="Job run family (default: %(default)s).",
    )
    parser.add_argument(
        "--trigger",
        default="cron",
        choices=["cron", "manual"],
        help="Run trigger (default: cron).",
    )
    parser.add_argument(
        "--source",
        default="k8s-cronjob",
        help="Run source (default: %(default)s).",
    )
    parser.add_argument(
        "--scheduled-hour",
        type=int,
        default=19,
        help="Scheduled run hour (default: 19).",
    )
    parser.add_argument(
        "--scheduled-minute",
        type=int,
        default=0,
        help="Scheduled run minute (default: 0).",
    )
    parser.add_argument(
        "--scheduled-timezone",
        default="Asia/Shanghai",
        help="Scheduled timezone (default: %(default)s).",
    )
    return parser.parse_args(argv)


def _check_asset_freshness(
    asset_type: str, asset_name: str, latest_trading_day: datetime.datetime
) -> dict[str, object]:
    """Count how many stocks are up-to-date vs lagging for one asset type."""
    total = DataAssetStatus.objects(
        asset_type=asset_type,
        asset_name__ne="",
    ).count()

    if total == 0:
        return {"total": 0, "ok": 0, "lagging": 0, "no_data": 0}

    ok = DataAssetStatus.objects(
        asset_type=asset_type,
        asset_name__ne="",
        latest_data_date__gte=latest_trading_day,
    ).count()

    no_data = DataAssetStatus.objects(
        asset_type=asset_type,
        asset_name__ne="",
        status="NO_DATA",
    ).count()

    lagging = total - ok
    return {
        "total": total,
        "ok": ok,
        "lagging": lagging,
        "no_data": no_data,
    }


def check_freshness() -> dict[str, object]:
    """Run freshness check against all tracked asset types."""
    mongo_watcher.get_db_connection()

    now = job_run_helper.beijing_now()

    trade_calendar = get_a_stock_market_trade_calendar()
    calendar_now = datetime.datetime.combine(now.date(), datetime.time())

    if not trade_calendar:
        return {
            "status": "SKIPPED",
            "reason": "no_trade_calendar",
            "results": {},
        }

    latest_trading_day = determine_most_recent_previous_complete_trading_day(
        trade_calendar, calendar_now
    )
    if latest_trading_day is None:
        return {
            "status": "SKIPPED",
            "reason": "no_trading_day",
            "results": {},
        }

    # Check quotes
    quote_result = _check_asset_freshness("quote", "daily_quote", latest_trading_day)
    factor_result = _check_asset_freshness("factor", "FQ_FACTOR", latest_trading_day)

    # Signal and scoring are derived - check they ran for today
    today = latest_trading_day
    signal_run = DatahubJobRun.objects(
        job_family="signal_daily",
        status="SUCCESS",
        started_at__gte=today,
    ).count()
    scoring_run = DatahubJobRun.objects(
        job_family="scoring_daily",
        status="SUCCESS",
        started_at__gte=today,
    ).count()

    overall_status = "OK"
    issues = []

    if quote_result["lagging"] > FRESHNESS_DIFF_DAYS:
        overall_status = "WARN"
        issues.append(
            f"行情数据: {quote_result['lagging']}/{quote_result['total']} 只股票滞后"
        )

    if signal_run == 0:
        overall_status = "WARN"
        issues.append("信号生成今日未运行 (signal_daily)")
    if scoring_run == 0:
        overall_status = "WARN"
        issues.append("评分计算今日未运行 (scoring_daily)")

    results = {
        "latest_trading_day": str(latest_trading_day.date()),
        "quote": quote_result,
        "factor": factor_result,
        "signal_today": signal_run > 0,
        "scoring_today": scoring_run > 0,
    }

    summary_parts = [
        f"行情: {quote_result['ok']}/{quote_result['total']} 已更新",
        f"因子: {factor_result['ok']}/{factor_result['total']} 已更新",
        f"信号: {'✅' if signal_run else '❌'}",
        f"评分: {'✅' if scoring_run else '❌'}",
    ]
    logger.info(
        "数据新鲜度检查 | %s | %s",
        overall_status,
        " | ".join(summary_parts),
    )
    if issues:
        for issue in issues:
            logger.warning("→ %s", issue)

    return {
        "status": overall_status,
        "reason": "; ".join(issues) if issues else "all_ok",
        "results": results,
    }


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)

    mongo_watcher.get_db_connection()

    scheduled_at = job_run_helper.compute_daily_schedule_at(
        args.scheduled_hour,
        args.scheduled_minute,
        timezone_name=args.scheduled_timezone,
    )

    context = job_run_helper.JobRunContext(
        job_name=args.job_name,
        job_family=args.job_family,
        trigger=args.trigger,
        source=args.source,
        scheduled_at=scheduled_at,
        extra={
            "scheduled_hour": args.scheduled_hour,
            "scheduled_minute": args.scheduled_minute,
        },
    )

    job_run = job_run_helper.create_job_run(context)

    try:
        result = check_freshness()
        status = STATUS_MAP.get(result["status"], "SUCCESS")
        job_run_helper.finish_job_run(job_run, status=status, summary=result)
        logger.info(
            "Freshness check complete: %s — %s",
            result["status"],
            result.get("reason", ""),
        )
    except Exception:
        exc_info = sys.exc_info()[1]
        logger.exception("Freshness check failed")
        job_run_helper.finish_job_run(
            job_run,
            status="FAILED",
            summary={"error": str(exc_info)},
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
