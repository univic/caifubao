from __future__ import annotations

import datetime
import logging
import os

from app.jobs.quote_runner import (
    TARGET_ALL,
    QuoteJobMetadata,
    run_quote_job,
)
from app.lib.db_watcher.mongoengine_tool import mongo_watcher
from app.lib.utilities import job_run_helper
from app.lib.utilities.data_asset_status_helper import (
    ASSET_DAILY_QUOTE,
    ASSET_TYPE_QUOTE,
)
from app.model.data_asset_status import STATUS_NOT_APPLICABLE, DataAssetStatus
from app.lib.utilities.trading_day_helper import (
    determine_most_recent_previous_complete_trading_day,
    get_a_stock_market_trade_calendar,
    is_trading_day,
)


logger = logging.getLogger(__name__)

DEFAULT_CATCHUP_HOUR = 18
DEFAULT_CATCHUP_MINUTE = 10


def _is_enabled() -> bool:
    value = os.getenv("DATAHUB_STARTUP_CATCHUP_ENABLED", "true").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _quote_status_is_lagging(latest_trading_day: datetime.datetime) -> bool:
    lagging_count = DataAssetStatus.objects(
        asset_type=ASSET_TYPE_QUOTE,
        asset_name=ASSET_DAILY_QUOTE,
        status__ne=STATUS_NOT_APPLICABLE,
        latest_data_date__lt=latest_trading_day,
    ).count()
    return lagging_count > 0


def _has_active_quote_job(scheduled_at: datetime.datetime) -> bool:
    return job_run_helper.has_active_job_run(
        job_family=job_run_helper.DEFAULT_QUOTE_JOB_FAMILY,
        scheduled_at=scheduled_at,
        max_age_minutes=180,
    )


def should_run_startup_quote_catchup(
    now: datetime.datetime | None = None,
) -> tuple[bool, dict[str, object]]:
    if not _is_enabled():
        return False, {"reason": "disabled"}

    now = now or job_run_helper.beijing_now()
    mongo_watcher.get_db_connection()
    trade_calendar = get_a_stock_market_trade_calendar()
    calendar_now = datetime.datetime.combine(now.date(), datetime.time())
    if not trade_calendar or not is_trading_day(trade_calendar, calendar_now):
        return False, {"reason": "non_trading_day"}

    scheduled_at = job_run_helper.compute_daily_schedule_at(
        DEFAULT_CATCHUP_HOUR,
        DEFAULT_CATCHUP_MINUTE,
        reference_time=now,
    )
    if now.time() < datetime.time(DEFAULT_CATCHUP_HOUR, DEFAULT_CATCHUP_MINUTE):
        return False, {
            "reason": "before_window",
            "scheduled_at": scheduled_at,
        }

    latest_trading_day = determine_most_recent_previous_complete_trading_day(
        trade_calendar, calendar_now
    )
    if latest_trading_day is None:
        return False, {"reason": "no_trading_day"}

    lagging = _quote_status_is_lagging(latest_trading_day)
    if not lagging:
        return False, {
            "reason": "already_current",
            "scheduled_at": scheduled_at,
            "latest_trading_day": latest_trading_day,
        }

    if _has_active_quote_job(scheduled_at):
        return False, {
            "reason": "active_job_exists",
            "scheduled_at": scheduled_at,
        }

    return True, {
        "scheduled_at": scheduled_at,
        "latest_trading_day": latest_trading_day,
    }


def run_startup_quote_catchup() -> dict[str, object]:
    should_run, context = should_run_startup_quote_catchup()
    if not should_run:
        logger.info("Skipping startup quote catch-up: %s", context.get("reason"))
        return {
            "status": "SKIPPED",
            "reason": context.get("reason"),
            "context": context,
        }

    scheduled_at = context["scheduled_at"]
    metadata = QuoteJobMetadata(
        job_name=job_run_helper.DEFAULT_QUOTE_CATCHUP_JOB_NAME,
        job_family=job_run_helper.DEFAULT_QUOTE_JOB_FAMILY,
        trigger=job_run_helper.DEFAULT_QUOTE_CATCHUP_TRIGGER,
        source=job_run_helper.DEFAULT_QUOTE_CATCHUP_SOURCE,
        scheduled_at=scheduled_at,
        extra={"latest_trading_day": context["latest_trading_day"]},
    )
    logger.info(
        "Running startup quote catch-up: scheduled_at=%s latest_trading_day=%s",
        scheduled_at,
        context["latest_trading_day"],
    )
    try:
        result = run_quote_job(TARGET_ALL, job_metadata=metadata)
    except job_run_helper.JobRunClaimExistsError:
        # Lost the atomic claim against an overlapping process (e.g. a pod
        # rollout): the winner is already running the catch-up.
        logger.info(
            "Startup quote catch-up already claimed by an active RUNNING "
            "job run; skipping"
        )
        return {
            "status": "SKIPPED",
            "reason": "already_claimed_by_active_run",
            "context": context,
        }
    result["status"] = result.get("status", "SUCCESS")
    result["reason"] = "catchup_run"
    result["context"] = context
    return result
