"""Operator runner for tushare on-exchange fund (ETF/LOF) market data.

Usage:
    python -m app.jobs.etf_sync_runner universe
    python -m app.jobs.etf_sync_runner daily --date 2026-10-23
    python -m app.jobs.etf_sync_runner backfill --code sh510300
    python -m app.jobs.etf_sync_runner backfill --all
    python -m app.jobs.etf_sync_runner status [--code sh510300]

``daily`` writes one trade date's quotes and then their distribution-adjusted
fields; a failure in either phase is reported in ``datahub_job_runs`` and the
process exits non-zero (no silent partial success). Nothing here trades.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import sys

from caifubao_contracts.time import beijing_now_naive

from app.lib.utilities import job_run_helper

logger = logging.getLogger(__name__)

JOB_FAMILY = "etf_market"
COMMAND_NAMES = {
    "universe": "datahub_etf_universe_sync",
    "daily": "datahub_etf_daily_sync",
    "backfill": "datahub_etf_history_backfill",
}


def _init_db_connection() -> None:
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher

    mongo_watcher.get_db_connection()


def _parse_date(value: str | None) -> datetime.datetime:
    if value:
        return datetime.datetime.strptime(value, "%Y-%m-%d")
    return beijing_now_naive().replace(hour=0, minute=0, second=0, microsecond=0)


def _service():
    from app.lib.datahub.etf_market import EtfMarketService

    return EtfMarketService()


def _run_with_job(command: str, action) -> dict:
    context = job_run_helper.JobRunContext(
        job_name=COMMAND_NAMES[command],
        job_family=JOB_FAMILY,
        trigger="cli",
        source="cli",
        target=command,
    )
    job_run = job_run_helper.create_job_run(context)
    try:
        summary = action()
    except Exception as exc:
        job_run_helper.finish_job_run(
            job_run,
            status="FAILED",
            summary={"failed_phase": command},
            error_message=str(exc),
        )
        raise
    job_run_helper.finish_job_run(
        job_run,
        status="SUCCESS",
        summary={
            "status": "SUCCESS",
            "pulled_total": int(summary.get("pulled_count", 0) or 0),
            "written_total": int(
                summary.get(
                    "written_count",
                    summary.get("quotes_written", 0),
                )
                or 0
            ),
            "target": summary.get("target_date", command),
            "detail": summary,
        },
    )
    return summary


def _cmd_universe(_args) -> dict:
    return _run_with_job("universe", lambda: _service().sync_universe())


def _cmd_daily(args) -> dict:
    target = _parse_date(args.date)

    def _action():
        return _service().sync_daily(target)

    return _run_with_job("daily", _action)


def _cmd_backfill(args) -> dict:
    if not args.all and not args.code:
        raise SystemExit("backfill requires --code CODE (repeatable) or --all")
    if args.all:
        return _run_with_job("backfill", lambda: _service().backfill_all())
    codes = list(dict.fromkeys(args.code))

    def _action():
        results = [_service().backfill_code(code) for code in codes]
        return {
            "pulled_count": len(results),
            "written_count": sum(
                int(result.get("quotes_written", 0)) for result in results
            ),
            "results": results,
        }

    return _run_with_job("backfill", _action)


def _cmd_status(args) -> dict:
    return _service().status(args.code)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Tushare on-exchange fund (ETF/LOF) market data runner"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("universe", help="Sync the ETF universe from fund_basic")

    daily = subparsers.add_parser(
        "daily", help="Sync one trade date's ETF quotes and FQ fields"
    )
    daily.add_argument(
        "--date",
        default=None,
        help="Trade date YYYY-MM-DD (default: today in Beijing)",
    )

    backfill = subparsers.add_parser(
        "backfill", help="Backfill per-fund history quotes and FQ fields"
    )
    backfill.add_argument(
        "--code", action="append", default=[], help="ETF code, e.g. sh510300"
    )
    backfill.add_argument(
        "--all", action="store_true", help="Backfill every active fund"
    )

    status = subparsers.add_parser("status", help="Read ETF quote/FQ coverage")
    status.add_argument("--code", default=None, help="Optional single ETF code")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO)
    args = build_parser().parse_args(argv)
    _init_db_connection()
    try:
        if args.command == "universe":
            summary = _cmd_universe(args)
        elif args.command == "daily":
            summary = _cmd_daily(args)
        elif args.command == "backfill":
            summary = _cmd_backfill(args)
        else:
            summary = _cmd_status(args)
    except Exception as exc:  # noqa: BLE001 - the CLI reports and fails loudly
        print(json.dumps({"status": "FAILED", "message": str(exc)}))
        return 2
    print(json.dumps({"status": "SUCCESS", "summary": summary}, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
