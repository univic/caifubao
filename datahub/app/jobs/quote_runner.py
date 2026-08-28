from __future__ import annotations

import argparse
import datetime
import json
import logging
from dataclasses import dataclass, field
from collections.abc import Callable
from typing import Any

from app.lib.utilities import job_run_helper


TARGET_INDEX = "index"
TARGET_STOCK = "stock"
TARGET_ALL = "all"


@dataclass(frozen=True)
class QuoteJobMetadata:
    job_name: str
    job_family: str
    trigger: str
    source: str
    scheduled_at: datetime.datetime | None = None
    extra: dict[str, Any] = field(default_factory=dict)


def _load_default_runtime(as_of_date: datetime.datetime | None = None) -> Any:
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher
    from app.lib.datahub import Datahub

    mongo_watcher.get_db_connection()
    return Datahub(quote_as_of_date=as_of_date)


def _normalize_summary(target: str, summary: dict[str, Any], include_factors: bool):
    return {
        "target": target,
        "include_factors": include_factors,
        "status": summary.get("status"),
        "failed_phase": summary.get("failed_phase"),
        "pulled_total": summary.get("pulled_total", 0),
        "written_total": summary.get("written_total", 0),
        "validated_total": summary.get("validated_total", 0),
        "as_of_date": summary.get("as_of_date"),
        "phase_stats": summary.get("phase_stats", {}),
    }


def run_quote_job(
    target: str,
    *,
    include_factors: bool = False,
    datahub_factory: Callable[[], Any] | None = None,
    job_metadata: QuoteJobMetadata | None = None,
    as_of_date: datetime.datetime | None = None,
) -> dict[str, Any]:
    datahub = (
        datahub_factory() if datahub_factory else _load_default_runtime(as_of_date)
    )
    job_run = None
    if job_metadata:
        from app.lib.utilities.job_run_helper import JobRunContext, create_job_run

        job_run = create_job_run(
            JobRunContext(
                job_name=job_metadata.job_name,
                job_family=job_metadata.job_family,
                trigger=job_metadata.trigger,
                source=job_metadata.source,
                scheduled_at=job_metadata.scheduled_at,
                target=target,
                include_factors=include_factors,
                extra=job_metadata.extra,
            )
        )

    completed_results = []
    current_result_target = target
    try:
        if target == TARGET_INDEX:
            summary = datahub.start_index_job()
            result = _normalize_summary(TARGET_INDEX, summary, include_factors=False)
        elif target == TARGET_STOCK:
            runner = (
                datahub.start_stock_job
                if include_factors
                else datahub.start_stock_quote_job
            )
            summary = runner()
            result = _normalize_summary(
                TARGET_STOCK, summary, include_factors=include_factors
            )
        else:
            current_result_target = TARGET_INDEX
            completed_results.append(
                _normalize_summary(
                    TARGET_INDEX, datahub.start_index_job(), include_factors=False
                )
            )
            stock_runner = (
                datahub.start_stock_job
                if include_factors
                else datahub.start_stock_quote_job
            )
            current_result_target = TARGET_STOCK
            completed_results.append(
                _normalize_summary(
                    TARGET_STOCK,
                    stock_runner(),
                    include_factors=include_factors,
                )
            )
            result = {
                "target": TARGET_ALL,
                "include_factors": include_factors,
                "results": completed_results,
                "status": "SUCCESS"
                if all(item["status"] == "SUCCESS" for item in completed_results)
                else "FAILED",
                "pulled_total": sum(item["pulled_total"] for item in completed_results),
                "written_total": sum(
                    item["written_total"] for item in completed_results
                ),
                "validated_total": sum(
                    item["validated_total"] for item in completed_results
                ),
                "as_of_date": completed_results[0]["as_of_date"],
            }
        if job_run:
            job_run_helper.finish_job_run(
                job_run,
                status=result.get("status", "SUCCESS"),
                summary=result,
            )
        return result
    except Exception as exc:
        if job_run:
            partial_summary = getattr(datahub, "last_job_summary", None) or {}
            partial_result = _normalize_summary(
                current_result_target,
                partial_summary,
                include_factors=include_factors,
            )
            failure_results = [*completed_results]
            if partial_summary:
                failure_results.append(partial_result)
            job_run_helper.finish_job_run(
                job_run,
                status="FAILED",
                summary={
                    "target": target,
                    "include_factors": include_factors,
                    "as_of_date": partial_summary.get("as_of_date")
                    or (
                        completed_results[0].get("as_of_date")
                        if completed_results
                        else None
                    )
                    or (as_of_date.isoformat() if as_of_date else None),
                    "pulled_total": sum(
                        item["pulled_total"] for item in failure_results
                    ),
                    "written_total": sum(
                        item["written_total"] for item in failure_results
                    ),
                    "validated_total": sum(
                        item["validated_total"] for item in failure_results
                    ),
                    "failed_phase": partial_summary.get("failed_phase"),
                    "phase_stats": partial_summary.get("phase_stats", {}),
                    "results": failure_results if target == TARGET_ALL else None,
                },
                error_message=str(exc),
            )
        raise


def _reap_stale_running_job_runs() -> None:
    """Best-effort startup cleanup: orphan RUNNING records left by dead runs.

    Re-run cron attempts (the 2026-08-26 pattern) would otherwise keep
    accumulating zombie records whenever a pod dies mid-run. Never blocks
    the job: a failure to clean up must not stop today's quote update.
    """
    try:
        from app.lib.db_watcher.mongoengine_tool import mongo_watcher

        mongo_watcher.get_db_connection()
        job_run_helper.mark_stale_running_job_runs_failed()
    except Exception:
        logger = logging.getLogger(__name__)
        logger.exception(
            "Stale RUNNING job-run cleanup failed; continuing with the job"
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run datahub quote update jobs without relying on scheduler timing."
    )
    parser.add_argument(
        "--target",
        choices=(TARGET_INDEX, TARGET_STOCK, TARGET_ALL),
        default=TARGET_STOCK,
        help="Quote update target. Defaults to stock.",
    )
    parser.add_argument(
        "--include-factors",
        action="store_true",
        help="For stock/all targets, also run FQ and MA factor phases after stock quotes.",
    )
    parser.add_argument(
        "--job-name",
        default=job_run_helper.DEFAULT_QUOTE_JOB_NAME,
        help="Job run name recorded in datahub_job_runs.",
    )
    parser.add_argument(
        "--job-family",
        default=job_run_helper.DEFAULT_QUOTE_JOB_FAMILY,
        help="Logical job family recorded in datahub_job_runs.",
    )
    parser.add_argument(
        "--trigger",
        default=job_run_helper.DEFAULT_QUOTE_JOB_TRIGGER,
        help="Trigger type recorded in datahub_job_runs.",
    )
    parser.add_argument(
        "--source",
        default=job_run_helper.DEFAULT_QUOTE_JOB_SOURCE,
        help="Source recorded in datahub_job_runs.",
    )
    parser.add_argument(
        "--scheduled-hour",
        type=int,
        default=job_run_helper.DEFAULT_QUOTE_JOB_HOUR,
        help="Scheduled hour in the configured timezone for cron/startup recording.",
    )
    parser.add_argument(
        "--scheduled-minute",
        type=int,
        default=job_run_helper.DEFAULT_QUOTE_JOB_MINUTE,
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
    parser.add_argument(
        "--as-of-date",
        default=None,
        help="Optional frozen quote cutoff date in YYYY-MM-DD format.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    _reap_stale_running_job_runs()
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

    job_metadata = QuoteJobMetadata(
        job_name=args.job_name,
        job_family=args.job_family,
        trigger=args.trigger,
        source=args.source,
        scheduled_at=scheduled_at,
        extra={"as_of_date": args.as_of_date} if args.as_of_date else {},
    )
    as_of_date = (
        datetime.datetime.strptime(args.as_of_date, "%Y-%m-%d")
        if args.as_of_date
        else None
    )
    result = run_quote_job(
        args.target,
        include_factors=args.include_factors,
        job_metadata=job_metadata,
        as_of_date=as_of_date,
    )
    print(json.dumps(result, default=str, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
