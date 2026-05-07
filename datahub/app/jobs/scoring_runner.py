# -*- coding: utf-8 -*-

import argparse
import datetime
import json
import logging

from app.lib.utilities import job_run_helper

logger = logging.getLogger(__name__)

SCORING_JOB_FAMILY = "scoring_daily"
SCORING_JOB_NAME = "datahub_scoring_daily"
SCORING_JOB_TRIGGER = "cron"
SCORING_JOB_SOURCE = "k8s-cronjob"
SCORING_JOB_HOUR = 18
SCORING_JOB_MINUTE = 35

# The signal_daily job family is required to have a SUCCESS record before scoring runs
DEPENDENCY_JOB_FAMILY = "signal_daily"

# All scoring horizons to run when none is specified
DEFAULT_HORIZONS = [5, 20, 60]


def _init_db_connection() -> None:
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher

    mongo_watcher.get_db_connection()


def parse_date(value: str | None) -> datetime.datetime | None:
    if not value:
        return None
    return datetime.datetime.strptime(value, "%Y-%m-%d")


def run_scoring(args) -> dict:
    from app.lib.scoring_engine.scoring_service import StockScoringService

    service = StockScoringService(model_version=args.model_version)
    horizons = [args.horizon] if args.horizon else DEFAULT_HORIZONS

    results = {}
    for horizon in horizons:
        logger.info("Running scoring for horizon=%d...", horizon)
        result = service.score_all_stocks(
            date=parse_date(args.date),
            horizon=horizon,
            dry_run=args.dry_run,
            replace=args.replace,
        )
        results[str(horizon)] = result
        logger.info("Scoring horizon=%d completed: %s", horizon, result)

    summary = {
        "horizons": horizons,
        "results": results,
        "pulled_total": sum(
            r.get("scored_count", 0) if isinstance(r, dict) else 0
            for r in results.values()
        ),
        "written_total": sum(
            r.get("scored_count", 0) if isinstance(r, dict) else 0
            for r in results.values()
        ),
    }
    return summary


def run_verification(
    *,
    horizon: int | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    model_version: str = "v1",
) -> dict:
    from app.lib.scoring_engine.verification_service import ScoreVerificationService

    service = ScoreVerificationService(model_version=model_version)
    horizons = [horizon] if horizon else DEFAULT_HORIZONS

    results = {}
    for h in horizons:
        logger.info("Running verification for horizon=%d...", h)
        result = service.verify_predictions(
            start_date=parse_date(from_date),
            end_date=parse_date(to_date),
            horizon=h,
        )
        results[str(h)] = result

    return {"horizons": horizons, "results": results}


def run_backfill(args):
    _init_db_connection()
    from app.lib.scoring_engine.replay_service import ScoreReplayService

    service = ScoreReplayService(model_version=args.model_version)
    result = service.backfill_predictions(
        start_date=parse_date(args.from_date),
        end_date=parse_date(args.to_date),
        horizon=args.horizon,
        stock_code=args.stock_code,
        dry_run=args.dry_run,
        replace=args.replace,
    )
    print(f"Backfill completed: {result}")


def run_report(args):
    _init_db_connection()
    from app.lib.scoring_engine.calibration_report import ScoreCalibrationReport

    service = ScoreCalibrationReport(model_version=args.model_version)
    result = service.generate(
        start_date=parse_date(args.from_date),
        end_date=parse_date(args.to_date),
        horizon=args.horizon,
    )
    if args.format == "json":
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    else:
        print(f"Calibration report: {result}")


def run_experiment(args):
    _init_db_connection()
    from app.lib.scoring_engine.experiment_service import ScoreExperimentService

    service = ScoreExperimentService()
    result = service.run_experiment(
        experiment_id=args.experiment_id,
        backfill=not args.skip_backfill,
        verify=not args.skip_verify,
        replace=args.replace,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


def _check_dependency(scheduled_at: datetime.datetime | None) -> bool:
    """Check if the required upstream job family has a SUCCESS record for today."""
    if scheduled_at is None:
        scheduled_at = job_run_helper.compute_daily_schedule_at(
            SCORING_JOB_HOUR, SCORING_JOB_MINUTE
        )

    latest = job_run_helper.latest_job_run(
        job_family=DEPENDENCY_JOB_FAMILY,
        scheduled_at=scheduled_at,
        statuses=["SUCCESS"],
    )
    return latest is not None


def add_common_options(
    parser, include_date=False, include_range=False, include_horizon=True
):
    parser.add_argument(
        "--model-version",
        default="v1",
        help="Scoring model version.",
    )
    if include_horizon:
        parser.add_argument(
            "--horizon",
            type=int,
            choices=[5, 20, 60],
            help="Scoring horizon in days. If omitted, all horizons (5, 20, 60) are run.",
        )
    if include_date:
        parser.add_argument(
            "--date",
            help="Evaluation date (YYYY-MM-DD), defaults to latest trading day",
        )
    if include_range:
        parser.add_argument("--from", dest="from_date", required=True)
        parser.add_argument("--to", dest="to_date", required=True)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Caifubao Stock Scoring Runner")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # run command - the primary cronjob entry point
    p_run = subparsers.add_parser("run", help="Calculate daily scores")
    p_run.add_argument("--horizon", type=int, choices=[5, 20, 60])
    p_run.add_argument("--date", help="Evaluation date (YYYY-MM-DD)")
    p_run.add_argument("--dry-run", action="store_true")
    p_run.add_argument("--replace", action="store_true")
    p_run.add_argument("--model-version", default="v1")

    # backfill command
    p_backfill = subparsers.add_parser("backfill", help="Backfill historical scores")
    add_common_options(p_backfill, include_range=True)
    p_backfill.add_argument("--stock-code")
    p_backfill.add_argument("--dry-run", action="store_true")
    p_backfill.add_argument("--replace", action="store_true")

    # verify command
    p_verify = subparsers.add_parser("verify", help="Verify score predictions")
    add_common_options(p_verify, include_range=True)

    # report command
    p_report = subparsers.add_parser("report", help="Generate calibration report")
    add_common_options(p_report, include_range=True, include_horizon=False)
    p_report.add_argument("--horizon", type=int, choices=[5, 20, 60], required=True)
    p_report.add_argument("--format", choices=["json", "text"], default="json")

    # experiment command
    p_experiment = subparsers.add_parser(
        "experiment", help="Run a stored score experiment"
    )
    p_experiment.add_argument("--id", dest="experiment_id", required=True)
    p_experiment.add_argument("--skip-backfill", action="store_true")
    p_experiment.add_argument("--skip-verify", action="store_true")
    p_experiment.add_argument("--replace", action="store_true")
    p_experiment.add_argument("--dry-run", action="store_true")

    # Add job tracking args to the "run" subparser
    p_run.add_argument(
        "--job-name",
        default=SCORING_JOB_NAME,
        help="Job run name recorded in datahub_job_runs.",
    )
    p_run.add_argument(
        "--job-family",
        default=SCORING_JOB_FAMILY,
        help="Logical job family recorded in datahub_job_runs.",
    )
    p_run.add_argument(
        "--trigger",
        default=SCORING_JOB_TRIGGER,
        help="Trigger type recorded in datahub_job_runs.",
    )
    p_run.add_argument(
        "--source",
        default=SCORING_JOB_SOURCE,
        help="Source recorded in datahub_job_runs.",
    )
    p_run.add_argument(
        "--scheduled-hour",
        type=int,
        default=SCORING_JOB_HOUR,
        help="Scheduled hour in the configured timezone for cron/startup recording.",
    )
    p_run.add_argument(
        "--scheduled-minute",
        type=int,
        default=SCORING_JOB_MINUTE,
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

    if args.command == "run":
        _run_with_tracking(args)
    elif args.command == "backfill":
        _init_db_connection()
        run_backfill(args)
    elif args.command == "verify":
        _init_db_connection()
        result = run_verification(
            horizon=args.horizon,
            from_date=args.from_date,
            to_date=args.to_date,
            model_version=args.model_version,
        )
        print(f"Verification completed: {json.dumps(result, default=str)}")
    elif args.command == "report":
        _init_db_connection()
        run_report(args)
    elif args.command == "experiment":
        _init_db_connection()
        run_experiment(args)
    else:
        parser.print_help()


def _run_with_tracking(args) -> None:
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
            "at scheduled_at=%s. Skipping scoring run.",
            DEPENDENCY_JOB_FAMILY,
            scheduled_at,
        )
        context = job_run_helper.JobRunContext(
            job_name=args.job_name,
            job_family=args.job_family,
            trigger=args.trigger,
            source=args.source,
            scheduled_at=scheduled_at,
        )
        job_run_helper.mark_job_run_skipped(
            context=context,
            summary={
                "reason": "dependency_failed",
                "dependency_job_family": DEPENDENCY_JOB_FAMILY,
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

    # Create job run tracking
    context = job_run_helper.JobRunContext(
        job_name=args.job_name,
        job_family=args.job_family,
        trigger=args.trigger,
        source=args.source,
        scheduled_at=scheduled_at,
        extra={"horizon": args.horizon, "date": args.date},
    )
    job_run = job_run_helper.create_job_run(context)

    try:
        result = run_scoring(args)

        # Also run verification after scoring
        try:
            verify_result = run_verification(
                horizon=args.horizon,
                from_date=args.from_date if hasattr(args, "from_date") else None,
                to_date=args.to_date if hasattr(args, "to_date") else None,
                model_version=args.model_version,
            )
            result["verify_results"] = verify_result
        except Exception as exc:
            logger.warning("Verification after scoring failed: %s", exc)
            result["verify_error"] = str(exc)

        summary = {
            "horizons": result.get("horizons", []),
            "pulled_total": result.get("pulled_total", 0),
            "written_total": result.get("written_total", 0),
            "has_verify": "verify_results" in result,
        }

        job_run_helper.finish_job_run(
            job_run,
            status="SUCCESS",
            summary=summary,
        )
        print(json.dumps(result, default=str, ensure_ascii=False, indent=2))

    except Exception as exc:
        logger.exception("Scoring run failed")
        job_run_helper.finish_job_run(
            job_run,
            status="FAILED",
            summary={"horizon": args.horizon, "date": args.date},
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    main()
