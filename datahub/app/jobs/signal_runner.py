from __future__ import annotations

import argparse
import datetime
import json
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from app.lib.utilities import job_run_helper

logger = logging.getLogger(__name__)

SIGNAL_MA_CROSS = "ma-cross"
SIGNAL_ALL = "all"
MODE_STALE = "stale"
MODE_FORCE = "force"

SIGNAL_JOB_FAMILY = "signal_daily"
SIGNAL_JOB_NAME = "datahub_signal_daily"
SIGNAL_JOB_TRIGGER = "cron"
SIGNAL_JOB_SOURCE = "k8s-cronjob"
SIGNAL_JOB_HOUR = 18
SIGNAL_JOB_MINUTE = 30

# The quote_daily job family is required to have a SUCCESS record before signal runs
DEPENDENCY_JOB_FAMILY = "quote_daily"
DEPENDENCY_JOB_HOUR = 18
DEPENDENCY_JOB_MINUTE = 0


@dataclass(frozen=True)
class SignalConfig:
    service_factory: Callable[[], Any]
    capability: str


def _load_default_runtime() -> tuple[dict[str, SignalConfig], Callable[[str], Any]]:
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher
    from app.lib.signal_factory import MovingAverageSignalService
    from app.model.stock import FinanceMarket

    mongo_watcher.get_db_connection()

    return (
        {
            SIGNAL_MA_CROSS: SignalConfig(MovingAverageSignalService, "ma_factor"),
            SIGNAL_ALL: SignalConfig(MovingAverageSignalService, "ma_factor"),
        },
        lambda market_name: FinanceMarket.objects(name=market_name).first(),
    )


def _load_supported_stock_codes(service, market, capability: str) -> list[str]:
    from app.lib.utilities import data_capability_helper

    stock_query = service.stock_model.objects(active_status=0)
    if market is not None:
        stock_query = stock_query.filter(market=market)
    return [
        stock.code
        for stock in stock_query.only("code", "data_capabilities")
        if data_capability_helper.stock_supports(stock, capability)
    ]


def _apply_limit(values: Iterable[str], limit: int | None) -> list[str]:
    value_list = list(values)
    if limit is None:
        return value_list
    return value_list[:limit]


def run_signal(
    signal: str,
    *,
    mode: str = MODE_STALE,
    codes: list[str] | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    market_name: str = "ChinaAStock",
    configs: dict[str, SignalConfig] | None = None,
    market_loader: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    if configs is None or market_loader is None:
        configs, market_loader = _load_default_runtime()

    config = configs[signal]
    service = config.service_factory()
    market = market_loader(market_name)

    if codes:
        selected_codes = _apply_limit(codes, limit)
    elif mode == MODE_STALE:
        selected_codes = _apply_limit(
            service.get_codes_requiring_update(market=market), limit
        )
    else:
        selected_codes = _apply_limit(
            _load_supported_stock_codes(service, market, config.capability), limit
        )

    result: dict[str, Any] = {
        "signal": signal,
        "mode": mode,
        "dry_run": dry_run,
        "market": market_name,
        "pulled_count": len(selected_codes),
        "written_count": 0,
        "skipped_count": 0,
        "failed_count": 0,
        "failed_codes": [],
        "codes": selected_codes if dry_run else selected_codes[:20],
    }
    if dry_run:
        return result

    for code in selected_codes:
        try:
            update_result = service.update_code(code)
        except Exception as exc:
            result["failed_count"] += 1
            result["failed_codes"].append(code)
            logger.exception("%s signal update failed: code=%s", signal, code)
            result["message"] = str(exc)
            continue

        if update_result.get("code") == "SKIP":
            result["skipped_count"] += 1
        result["written_count"] += int(update_result.get("written_count", 0))

    return result


def _check_dependency(scheduled_at: datetime.datetime | None) -> bool:
    """Check if the required upstream job family has a SUCCESS record for today.

    Uses the upstream job's schedule time (DEPENDENCY_JOB_HOUR/MINUTE),
    not this job's own schedule, to match the upstream job's recorded
    scheduled_at value.
    """
    if scheduled_at is None:
        scheduled_at = job_run_helper.compute_daily_schedule_at(
            DEPENDENCY_JOB_HOUR, DEPENDENCY_JOB_MINUTE
        )

    latest = job_run_helper.latest_job_run(
        job_family=DEPENDENCY_JOB_FAMILY,
        scheduled_at=scheduled_at,
        statuses=[job_run_helper.STATUS_SUCCESS],
    )
    return latest is not None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run datahub signal updates with safe stale-only defaults."
    )
    parser.add_argument(
        "--signal",
        choices=(SIGNAL_MA_CROSS, SIGNAL_ALL),
        default=SIGNAL_MA_CROSS,
        help="Signal family to update. Defaults to MA cross.",
    )
    parser.add_argument(
        "--mode",
        choices=(MODE_STALE, MODE_FORCE),
        default=MODE_STALE,
        help="stale updates only missing/outdated signals; force scans all supported active stocks.",
    )
    parser.add_argument(
        "--code",
        action="append",
        dest="codes",
        default=[],
        help="Specific stock code to update. Can be passed multiple times.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of selected codes to process.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print selected codes without writing signal data.",
    )
    parser.add_argument(
        "--market",
        default="ChinaAStock",
        help="FinanceMarket name used when selecting market-wide stocks.",
    )
    parser.add_argument(
        "--job-name",
        default=SIGNAL_JOB_NAME,
        help="Job run name recorded in datahub_job_runs.",
    )
    parser.add_argument(
        "--job-family",
        default=SIGNAL_JOB_FAMILY,
        help="Logical job family recorded in datahub_job_runs.",
    )
    parser.add_argument(
        "--trigger",
        default=SIGNAL_JOB_TRIGGER,
        help="Trigger type recorded in datahub_job_runs.",
    )
    parser.add_argument(
        "--source",
        default=SIGNAL_JOB_SOURCE,
        help="Source recorded in datahub_job_runs.",
    )
    parser.add_argument(
        "--scheduled-hour",
        type=int,
        default=SIGNAL_JOB_HOUR,
        help="Scheduled hour in the configured timezone for cron/startup recording.",
    )
    parser.add_argument(
        "--scheduled-minute",
        type=int,
        default=SIGNAL_JOB_MINUTE,
        help="Scheduled minute in the configured timezone for cron/startup recording.",
    )
    parser.add_argument(
        "--scheduled-timezone",
        default=job_run_helper.BEIJING_TZ_NAME,
        help="Timezone used to derive scheduled_at when running as cron/startup.",
    )
    parser.add_argument(
        "--scheduled-at",
        default=None,
        help="Optional explicit scheduled_at timestamp in ISO format.",
    )
    return parser.parse_args(argv)


def _init_db_connection() -> None:
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher

    mongo_watcher.get_db_connection()


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    # Initialize DB connection before any queries
    _init_db_connection()

    # Compute scheduled_at
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

    # Check upstream dependency
    if not _check_dependency(scheduled_at):
        logger.warning(
            "Dependency check failed: no SUCCESS record found for job_family=%s "
            "at scheduled_at=%s. Skipping signal run.",
            DEPENDENCY_JOB_FAMILY,
            scheduled_at,
        )
        context = job_run_helper.JobRunContext(
            job_name=args.job_name,
            job_family=args.job_family,
            trigger=args.trigger,
            source=args.source,
            scheduled_at=scheduled_at,
            extra={"signal": args.signal, "mode": args.mode},
        )
        job_run_helper.mark_job_run_skipped(
            context=context,
            summary={
                "reason": "dependency_failed",
                "dependency_job_family": DEPENDENCY_JOB_FAMILY,
                "signal": args.signal,
                "mode": args.mode,
            },
        )
        print(
            json.dumps(
                {
                    "status": "SKIPPED",
                    "reason": f"No successful {DEPENDENCY_JOB_FAMILY} job for today",
                }
            )
        )
        return

    # Validate arguments before creating job run
    if args.limit is not None and args.limit < 1:
        raise ValueError("--limit must be greater than 0.")

    # Create job run tracking
    context = job_run_helper.JobRunContext(
        job_name=args.job_name,
        job_family=args.job_family,
        trigger=args.trigger,
        source=args.source,
        scheduled_at=scheduled_at,
        extra={"signal": args.signal, "mode": args.mode},
    )
    job_run = job_run_helper.create_job_run(context)

    try:
        result = run_signal(
            args.signal,
            mode=args.mode,
            codes=args.codes,
            limit=args.limit,
            dry_run=args.dry_run,
            market_name=args.market,
        )

        summary = {
            "signal": args.signal,
            "mode": args.mode,
            "market": args.market,
            "pulled_total": result.get("pulled_count", 0),
            "written_total": result.get("written_count", 0),
            "skipped_count": result.get("skipped_count", 0),
            "failed_count": result.get("failed_count", 0),
            "failed_codes": result.get("failed_codes", []),
        }

        job_run_helper.finish_job_run(job_run, status="SUCCESS", summary=summary)
        print(json.dumps(result, default=str, ensure_ascii=False, indent=2))

    except Exception as exc:
        job_run_helper.finish_job_run(
            job_run,
            status="FAILED",
            summary={
                "signal": args.signal,
                "mode": args.mode,
            },
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    main()
