# -*- coding: utf-8 -*-
"""Paper-first strategy runner (daily).

Reads VERIFIED score predictions for a configured model_version + date +
horizon, applies the versioned strategy config (eligibility, wide-book
selection — buy-high only), persists the target portfolio + rebalance list
(StrategyPaperRun), and records a strategy-freshness datahub_job_run.
Paper-only: never places real orders. Skip (not empty) when the configured
source has no VERIFIED scores for the date.

Usage:
    python -m app.jobs.strategy_runner run --date 2026-09-04 \
        --config-json '{"score_model_version":"flip_wide_shadow_v1"}'
    python -m app.jobs.strategy_runner report --date 2026-09-04
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging

from app.lib.strategy_engine.config import (
    DEFAULT_HORIZON,
    DEFAULT_STRATEGY_CONFIG,
    strategy_config_hash,
    validate_strategy_config,
)
from app.lib.utilities import job_run_helper

logger = logging.getLogger(__name__)

STRATEGY_JOB_FAMILY = "strategy_daily"
STRATEGY_JOB_NAME = "datahub_strategy_daily"
STRATEGY_JOB_TRIGGER = "cron"
STRATEGY_JOB_SOURCE = "k8s-cronjob"
STRATEGY_JOB_HOUR = 19
STRATEGY_JOB_MINUTE = 10

# Strategy run label stored on StrategyPaperRun; the strategy *semantics* are
# pinned by model_version + config_hash (this label is not part of config).
DEFAULT_STRATEGY_NAME = "flip_wide_paper"

# scoring_daily must have a SUCCESS record before strategy runs (strategy
# consumes VERIFIED scores produced later in the evening).
DEPENDENCY_JOB_FAMILY = "scoring_daily"
DEPENDENCY_JOB_NAME = "datahub_scoring_daily"
DEPENDENCY_JOB_HOUR = 18
DEPENDENCY_JOB_MINUTE = 35
DEPENDENCY_WAIT_TIMEOUT_SECONDS = 600
DEPENDENCY_POLL_INTERVAL_SECONDS = 10


def parse_date(value: str | None) -> datetime.datetime | None:
    if not value:
        return None
    return datetime.datetime.strptime(value, "%Y-%m-%d")


def _init_db() -> None:
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher

    mongo_watcher.get_db_connection()


def _resolve_model_version(config: dict) -> str:
    """Assert the score source is ACTIVE-registered and covers the horizon.

    A named-but-missing version silently scores with built-in default
    directions, which would make a flip_wide strategy run on un-flipped
    scores; the runner fails closed unless registration is verified.
    """
    from app.model.scoring import ScoreModelVersion

    model_version = config["score_model_version"]
    horizon = int(config.get("horizon", DEFAULT_HORIZON))
    try:
        registered = ScoreModelVersion.objects(
            model_version=model_version, status="ACTIVE"
        ).first()
    except Exception:  # noqa: BLE001 - registry best-effort; strategy fails closed
        raise ValueError(
            f"cannot verify model_version {model_version!r} registration; "
            "strategy run fails closed"
        ) from None
    if registered is None:
        raise ValueError(
            f"model_version {model_version!r} is not ACTIVE-registered; "
            "register it first (see the task-3.3 runbook). Refusing to run the "
            "strategy on an unverified score source."
        )
    if str(horizon) not in (registered.config or {}):
        raise ValueError(
            f"registered config for {model_version!r} does not cover "
            f"horizon {horizon}; refusing to run on default-direction scores "
            "for that horizon."
        )
    return model_version


def _query_verified_predictions(model_version, date, horizon):
    from app.model.scoring import StockScorePrediction

    return list(
        StockScorePrediction.objects(
            model_version=model_version,
            date=date,
            horizon=horizon,
            status="VERIFIED",
        ).order_by("stock_code")
    )


def _query_flags(date, horizon) -> dict[str, dict]:
    """{stock_code: {is_st, is_bse, trade_status}} for the date's cohort.

    Quote rows carry trade_status/isST; BSE is derived from the stock code.
    Codes without a quote row for the date keep trade_status=1 but ST/BSE flags
    only when known — the engine treats unknown-flag codes as eligible for
    selection; liquidity filtering beyond trade_status is a later-slice concern
    (min_trade_amount is validated but unenforced in the core).
    """
    from app.lib.utilities.data_capability_helper import is_bse_stock_code
    from app.model.scoring import StockScorePrediction
    from app.model.stock import IndividualStock

    predictions = list(
        StockScorePrediction.objects(
            date=date, horizon=horizon, status="VERIFIED"
        ).only("stock_code")
    )
    codes = [p.stock_code for p in predictions]
    flags = {}
    for code in codes:
        is_st = 0
        trade_status = 1
        stock = IndividualStock.objects(code=code).first()
        if stock is not None:
            quotes = [
                q
                for q in (stock.daily_quote_hfq or [])
                if getattr(q, "date", None) == date
            ]
            if quotes:
                quote = quotes[-1]
                is_st = int(quote.isST or 0)
                trade_status = int(quote.trade_status or 1)
        flags[code] = {
            "is_st": is_st,
            "is_bse": 1 if is_bse_stock_code(code) else 0,
            "trade_status": trade_status,
        }
    return flags


def run_strategy(
    *,
    date: datetime.datetime,
    config: dict | None = None,
    dry_run: bool = False,
    replace: bool = False,
) -> dict:
    from app.lib.strategy_engine.runner import assemble_daily_plan
    from app.model.strategy import StrategyPaperRun

    resolved = validate_strategy_config(config or DEFAULT_STRATEGY_CONFIG)
    model_version = _resolve_model_version(resolved)
    horizon = int(resolved.get("horizon", DEFAULT_HORIZON))
    config_hash = strategy_config_hash(resolved)

    query = StrategyPaperRun.objects(
        strategy_name=DEFAULT_STRATEGY_NAME,
        date=date,
        model_version=model_version,
        horizon=horizon,
        config_hash=config_hash,
    )
    existing = query.first()
    if existing is not None and existing.status == "COMPLETED" and not replace:
        logger.info("strategy already completed for %s; pass --replace to rerun", date)
        return {
            "skipped_existing": True,
            "date": date,
            "model_version": model_version,
            "config_hash": config_hash,
        }

    predictions = _query_verified_predictions(model_version, date, horizon)
    flags = _query_flags(date, horizon) if predictions else {}
    plan = assemble_daily_plan(
        config=resolved,
        date=date,
        predictions=predictions,
        previous_holdings=None,
        flags=flags,
        horizon=horizon,
    )
    if dry_run:
        return {"dry_run": True, "date": date, "plan": plan}

    if existing is not None:
        existing.delete()

    run = StrategyPaperRun(
        strategy_name=DEFAULT_STRATEGY_NAME,
        date=date,
        model_version=model_version,
        horizon=horizon,
        config_hash=config_hash,
        config=resolved,
        status="SKIPPED" if plan["skipped"] else "COMPLETED",
        skip_reason=plan.get("reason"),
        target_holdings=plan.get("target_holdings", []),
        rebalance=plan.get("rebalance", {}),
    )
    run.save()
    return {
        "date": date,
        "model_version": model_version,
        "horizon": horizon,
        "config_hash": config_hash,
        "status": run.status,
        "skip_reason": run.skip_reason,
        "target_holdings_count": len(run.target_holdings or []),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Caifubao Strategy Paper Runner")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="Run one day's paper strategy")
    p_run.add_argument("--date", required=True, help="Evaluation date (YYYY-MM-DD)")
    p_run.add_argument(
        "--config-json",
        default=None,
        help="Strategy config JSON; defaults to the flip_wide paper config",
    )
    p_run.add_argument("--dry-run", action="store_true")
    p_run.add_argument("--replace", action="store_true")

    p_report = sub.add_parser("report", help="Show the latest paper run for a date")
    p_report.add_argument("--date", required=True, help="Date (YYYY-MM-DD)")
    p_report.add_argument("--model-version", default=None)
    return parser


def _run_with_tracking(args, config: dict | None) -> None:
    """Run with a datahub_job_runs freshness record (scoring_runner pattern)."""
    scheduled_at = job_run_helper.compute_daily_schedule_at(
        STRATEGY_JOB_HOUR, STRATEGY_JOB_MINUTE
    )
    context = job_run_helper.JobRunContext(
        job_name=STRATEGY_JOB_NAME,
        job_family=STRATEGY_JOB_FAMILY,
        trigger=STRATEGY_JOB_TRIGGER,
        source=STRATEGY_JOB_SOURCE,
        scheduled_at=scheduled_at,
    )
    job_run = job_run_helper.create_job_run(context)
    try:
        result = run_strategy(
            date=parse_date(args.date),
            config=config,
            dry_run=args.dry_run,
            replace=args.replace,
        )
        status = (
            job_run_helper.STATUS_SKIPPED
            if result.get("skipped_existing") or result.get("plan", {}).get("skipped")
            else job_run_helper.STATUS_SUCCESS
        )
        job_run_helper.finish_job_run(
            job_run,
            status=status,
            summary={
                "target": args.date,
                "model_version": result.get("model_version"),
                "config_hash": result.get("config_hash"),
                "written_total": 1 if status == job_run_helper.STATUS_SUCCESS else 0,
            },
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    except Exception as exc:  # noqa: BLE001
        job_run_helper.finish_job_run(
            job_run, status=job_run_helper.STATUS_FAILED, error_message=str(exc)
        )
        raise


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO)
    args = build_parser().parse_args(argv)
    _init_db()
    if args.command == "run":
        config = json.loads(args.config_json) if args.config_json else None
        _run_with_tracking(args, config)
    elif args.command == "report":
        from app.model.strategy import StrategyPaperRun

        query = StrategyPaperRun.objects(date=parse_date(args.date))
        if args.model_version:
            query = query(model_version=args.model_version)
        doc = query.order_by("-created_at").first()
        if doc is None:
            print(json.dumps({"found": False}, ensure_ascii=False))
        else:
            print(
                json.dumps(
                    {
                        "found": True,
                        "strategy_name": doc.strategy_name,
                        "model_version": doc.model_version,
                        "horizon": doc.horizon,
                        "config_hash": doc.config_hash,
                        "status": doc.status,
                        "skip_reason": doc.skip_reason,
                        "target_holdings_count": len(doc.target_holdings or []),
                    },
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                )
            )


if __name__ == "__main__":
    main()
