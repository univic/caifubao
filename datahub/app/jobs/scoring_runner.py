# -*- coding: utf-8 -*-

import argparse
import datetime
import json

from app.lib.db_watcher.mongoengine_tool import mongo_watcher
from app.lib.scoring_engine.calibration_report import ScoreCalibrationReport
from app.lib.scoring_engine.config import DEFAULT_MODEL_VERSION
from app.lib.scoring_engine.experiment_service import ScoreExperimentService
from app.lib.scoring_engine.replay_service import ScoreReplayService
from app.lib.scoring_engine.scoring_service import StockScoringService
from app.lib.scoring_engine.verification_service import ScoreVerificationService


def parse_date(value: str | None) -> datetime.datetime | None:
    if not value:
        return None
    return datetime.datetime.strptime(value, "%Y-%m-%d")


def run_scoring(args):
    mongo_watcher.get_db_connection()
    service = StockScoringService(model_version=args.model_version)
    result = service.score_all_stocks(
        date=parse_date(args.date),
        horizon=args.horizon,
        dry_run=args.dry_run,
        replace=args.replace,
    )
    print(f"Scoring completed: {result}")


def run_backfill(args):
    mongo_watcher.get_db_connection()
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


def run_verification(args):
    mongo_watcher.get_db_connection()
    service = ScoreVerificationService(model_version=args.model_version)
    result = service.verify_predictions(
        start_date=parse_date(args.from_date),
        end_date=parse_date(args.to_date),
        horizon=args.horizon,
    )
    print(f"Verification completed: {result}")


def run_report(args):
    mongo_watcher.get_db_connection()
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
    mongo_watcher.get_db_connection()
    service = ScoreExperimentService()
    result = service.run_experiment(
        experiment_id=args.experiment_id,
        backfill=not args.skip_backfill,
        verify=not args.skip_verify,
        replace=args.replace,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


def add_common_options(
    parser, include_date=False, include_range=False, include_horizon=True
):
    parser.add_argument(
        "--model-version",
        default=DEFAULT_MODEL_VERSION,
        help="Scoring model version.",
    )
    if include_horizon:
        parser.add_argument("--horizon", type=int, choices=[5, 20, 60])
    if include_date:
        parser.add_argument(
            "--date",
            help="Evaluation date (YYYY-MM-DD), defaults to latest trading day",
        )
    if include_range:
        parser.add_argument("--from", dest="from_date", required=True)
        parser.add_argument("--to", dest="to_date", required=True)


def main():
    parser = argparse.ArgumentParser(description="Caifubao Stock Scoring Runner")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    p_run = subparsers.add_parser("run", help="Calculate daily scores")
    add_common_options(p_run, include_date=True)
    p_run.add_argument("--dry-run", action="store_true")
    p_run.add_argument("--replace", action="store_true")

    p_backfill = subparsers.add_parser("backfill", help="Backfill historical scores")
    add_common_options(p_backfill, include_range=True)
    p_backfill.add_argument("--stock-code")
    p_backfill.add_argument("--dry-run", action="store_true")
    p_backfill.add_argument("--replace", action="store_true")

    p_verify = subparsers.add_parser("verify", help="Verify score predictions")
    add_common_options(p_verify, include_range=True)

    p_report = subparsers.add_parser("report", help="Generate calibration report")
    add_common_options(p_report, include_range=True, include_horizon=False)
    p_report.add_argument("--horizon", type=int, choices=[5, 20, 60], required=True)
    p_report.add_argument("--format", choices=["json", "text"], default="json")

    p_experiment = subparsers.add_parser(
        "experiment", help="Run a stored score experiment"
    )
    p_experiment.add_argument("--id", dest="experiment_id", required=True)
    p_experiment.add_argument("--skip-backfill", action="store_true")
    p_experiment.add_argument("--skip-verify", action="store_true")
    p_experiment.add_argument("--replace", action="store_true")
    p_experiment.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    if args.command == "run":
        run_scoring(args)
    elif args.command == "backfill":
        run_backfill(args)
    elif args.command == "verify":
        run_verification(args)
    elif args.command == "report":
        run_report(args)
    elif args.command == "experiment":
        run_experiment(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
