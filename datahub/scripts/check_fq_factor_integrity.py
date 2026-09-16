#!/usr/bin/env python3

import argparse
import datetime
import json
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_SAMPLE_CODES = ["sh600000", "sz002797"]


def get_app_config():
    from app.conf import app_config

    return app_config


def build_client():
    from pymongo import MongoClient

    app_config = get_app_config()
    return MongoClient(
        host=app_config.MONGODB_HOST,
        port=app_config.MONGODB_PORT,
        username=app_config.MONGODB_USERNAME,
        password=app_config.MONGODB_PASSWORD,
        authSource="admin",
        serverSelectionTimeoutMS=5000,
    )


def get_sample_latest_quote(db, code: str) -> dict[str, Any] | None:
    from pymongo import DESCENDING

    return db["stock_daily_quote"].find_one(
        {"code": code},
        sort=[("date", DESCENDING)],
        projection={
            "_id": 0,
            "code": 1,
            "date": 1,
            "close": 1,
            "previous_close": 1,
            "fq_factor": 1,
            "close_hfq": 1,
            "open_hfq": 1,
        },
    )


def build_summary(
    db, sample_codes: list[str], include_any_fq_probe: bool
) -> dict[str, Any]:
    app_config = get_app_config()
    basic_stock = db["basic_stock"]
    freshness_meta = db["data_freshness_meta"]
    stock_daily_quote = db["stock_daily_quote"]

    summary: dict[str, Any] = {
        "database": app_config.MONGODB_DB,
        "individual_stock_count": basic_stock.count_documents(
            {"object_type": "individual_stock"}
        ),
        "fq_factor_meta_count": freshness_meta.count_documents(
            {
                "object_type": "individual_stock",
                "meta_type": "factor",
                "meta_name": "FQ_FACTOR",
            }
        ),
        "sample_latest_quotes": [
            get_sample_latest_quote(db, code) for code in sample_codes if code
        ],
    }

    if include_any_fq_probe:
        summary["any_fq_factor_quote"] = stock_daily_quote.find_one(
            {"fq_factor": {"$exists": True}},
            projection={"_id": 0, "code": 1, "date": 1, "fq_factor": 1, "close_hfq": 1},
        )

    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only fq_factor integrity probe for the current MongoDB "
            "environment (sample spot-check, plus an optional market-wide "
            "jump scan)."
        )
    )
    parser.add_argument(
        "--sample-code",
        action="append",
        dest="sample_codes",
        default=[],
        help="Stock code to spot-check on the latest quote. Can be passed multiple times.",
    )
    parser.add_argument(
        "--skip-any-fq-probe",
        action="store_true",
        help="Skip the probe that searches for any quote document containing fq_factor.",
    )
    parser.add_argument(
        "--jump-scan",
        action="store_true",
        help=(
            "Also stream a bounded date window and report per-date fq_factor "
            "jumps (market-wide acceptance scan for the FQ recompute)."
        ),
    )
    parser.add_argument(
        "--from-date",
        default=None,
        help="Jump-scan window start (YYYY-MM-DD); required with --jump-scan.",
    )
    parser.add_argument(
        "--to-date",
        default=None,
        help=(
            "Jump-scan window end (YYYY-MM-DD); defaults to today. Use the "
            "recompute window, not the full history."
        ),
    )
    parser.add_argument(
        "--jump-threshold",
        type=float,
        default=None,
        help="Relative day-over-day change treated as a jump (default 0.10).",
    )
    parser.add_argument(
        "--max-gap-days",
        type=int,
        default=None,
        help=(
            "Only compare rows at most this many days apart, so a suspended "
            "stock resuming does not look like a one-day jump (default 5)."
        ),
    )
    parser.add_argument(
        "--top-dates",
        type=int,
        default=None,
        help="How many anomalous dates to report (default 10).",
    )
    parser.add_argument(
        "--date",
        default=None,
        help=(
            "Optional target date (YYYY-MM-DD, e.g. the known 2026-08-31 "
            "discontinuity) reported explicitly alongside the ranking."
        ),
    )
    parser.add_argument(
        "--allow-long-window",
        action="store_true",
        help="Permit jump-scan windows longer than the built-in 500-day guard.",
    )
    args = parser.parse_args()
    if args.jump_scan and not args.from_date:
        parser.error("--jump-scan requires --from-date (use the recompute window)")
    if not args.jump_scan:
        ignored = [
            flag
            for flag, value in (
                ("--from-date", args.from_date),
                ("--to-date", args.to_date),
                ("--jump-threshold", args.jump_threshold),
                ("--max-gap-days", args.max_gap_days),
                ("--top-dates", args.top_dates),
                ("--date", args.date),
                ("--allow-long-window", args.allow_long_window),
            )
            if value
        ]
        if ignored:
            print(
                "warning: " + ", ".join(ignored) + " ignored without --jump-scan",
                file=sys.stderr,
            )
    if args.jump_scan:
        if args.jump_threshold is not None and args.jump_threshold < 0:
            parser.error("--jump-threshold must be >= 0")
        if args.max_gap_days is not None and args.max_gap_days < 1:
            parser.error("--max-gap-days must be >= 1")
        if args.top_dates is not None and args.top_dates < 1:
            parser.error("--top-dates must be >= 1")
    return args


def _parse_day(value: str, flag: str) -> datetime.date:
    try:
        return datetime.date.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(f"{flag} must be YYYY-MM-DD: {value!r}") from exc


def build_jump_scan(args, db) -> dict[str, Any]:
    from app.lib.datahub.data_integrity_keeper.handler import fq_factor_integrity

    date_from = _parse_day(args.from_date, "--from-date")
    date_to = (
        _parse_day(args.to_date, "--to-date") if args.to_date else datetime.date.today()
    )
    target_date = _parse_day(args.date, "--date") if args.date else None
    kwargs: dict[str, Any] = {
        "date_from": date_from,
        "date_to": date_to,
        "allow_long_window": args.allow_long_window,
        # The acceptance scan must exclude index rows; the sample probe alone
        # has no such requirement.
        "require_universe": True,
    }
    if args.jump_threshold is not None:
        kwargs["threshold"] = args.jump_threshold
    if args.max_gap_days is not None:
        kwargs["max_gap_days"] = args.max_gap_days
    if args.top_dates is not None:
        kwargs["top_n"] = args.top_dates
    if target_date is not None:
        kwargs["target_date"] = target_date

    return fq_factor_integrity.scan_fq_factor_jumps(db, **kwargs)


def main() -> None:
    args = parse_args()
    sample_codes = args.sample_codes or DEFAULT_SAMPLE_CODES
    app_config = get_app_config()

    client = build_client()
    try:
        db = client[app_config.MONGODB_DB]
        client.admin.command("ping")
        summary = build_summary(
            db,
            sample_codes=sample_codes,
            include_any_fq_probe=not args.skip_any_fq_probe,
        )
        if args.jump_scan:
            try:
                summary["jump_scan"] = build_jump_scan(args, db)
            except ValueError as exc:
                print(f"error: {exc}", file=sys.stderr)
                sys.exit(2)
        print(json.dumps(summary, default=str, ensure_ascii=False, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
