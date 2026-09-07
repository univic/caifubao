# -*- coding: utf-8 -*-
"""Paper-first strategy runner (daily).

Reads usable scores independently of future outcome verification, records signal
and execution dates, and produces retrospective paper artifacts only. REPLAY
outputs and successful jobs never certify frozen forward evidence.

Usage:
    python -m app.jobs.strategy_runner run --date 2026-09-04 \
        --config-json '{"score_model_version":"flip_wide_shadow_v1"}'
    python -m app.jobs.strategy_runner nav --from 2026-03-01 --to 2026-08-31 \
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

USABLE_SCORE_STATUSES = ("PENDING", "TRACKING", "VERIFIED", "INSUFFICIENT_DATA")

STRATEGY_JOB_FAMILY = "strategy_daily"
STRATEGY_JOB_NAME = "datahub_strategy_daily"
STRATEGY_JOB_TRIGGER = "cron"
STRATEGY_JOB_SOURCE = "k8s-cronjob"
STRATEGY_JOB_HOUR = 19
STRATEGY_JOB_MINUTE = 10

# Strategy run label stored on StrategyPaperRun; the strategy *semantics* are
# pinned by model_version + config_hash (this label is not part of config).
DEFAULT_STRATEGY_NAME = "flip_wide_paper"


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


def _query_usable_predictions(model_version, date, horizon):
    from app.model.scoring import StockScorePrediction

    return list(
        StockScorePrediction.objects(
            model_version=model_version,
            date=date,
            horizon=horizon,
            status__in=USABLE_SCORE_STATUSES,
        ).order_by("stock_code")
    )


def _query_flags(date, horizon, model_version=None) -> dict[str, dict]:
    """{stock_code: {is_st, is_bse, trade_status}} for the date's cohort.

    Sources ST/trade_status from the StockDailyQuote collection (the real quote
    store — the embedded DailyQuote list on IndividualStock is never written by
    any pipeline) via one bulk query; BSE is derived from the stock code.
    Codes with no quote row for the date are excluded by the engine's
    fail-closed rule (unknown status/liquidity is not tradable), so ST or
    suspended names cannot enter the paper portfolio.
    """
    from app.lib.scoring_engine.scoring_service import normalize_date
    from app.lib.utilities.data_capability_helper import is_bse_stock_code
    from app.model.scoring import StockScorePrediction
    from app.model.stock import StockDailyQuote

    query = StockScorePrediction.objects(
        date=date, horizon=horizon, status__in=USABLE_SCORE_STATUSES
    )
    if model_version:
        query = query(model_version=model_version)
    predictions = list(query.only("stock_code"))
    codes = [p.stock_code for p in predictions]
    if not codes:
        return {}

    quotes = {
        q.code: q
        for q in StockDailyQuote.objects(code__in=codes, date=normalize_date(date))
    }
    flags = {}
    for code in codes:
        quote = quotes.get(code)
        if quote is None:
            # No quote row for the date: engine treats it as not eligible
            # (fail-closed); omit from the map so the code is excluded.
            continue
        flags[code] = {
            "is_st": int(quote.isST or 0),
            "is_bse": 1 if is_bse_stock_code(code) else 0,
            "trade_status": quote.trade_status,
        }
    return flags


def _execution_date(date):
    from app.lib.strategy_engine.runner import next_execution_date
    from app.lib.utilities.trading_day_helper import get_a_stock_market_trade_calendar

    return next_execution_date(date, get_a_stock_market_trade_calendar())


def _active_window(model_version: str, horizon: int):
    """Return the ACTIVE certified forward window for (model_version, horizon),
    or None when none is certified (NEXT.1)."""
    from app.model.strategy import StrategyForwardWindow

    return StrategyForwardWindow.objects(
        model_version=model_version,
        horizon=horizon,
        status="ACTIVE",
    ).first()


def run_strategy(
    *,
    date: datetime.datetime,
    config: dict | None = None,
    dry_run: bool = False,
    replace: bool = False,
) -> dict:
    from app.lib.strategy_engine.runner import assemble_daily_plan
    from app.model.strategy import StrategyPaperRun

    date = datetime.datetime.combine(date.date(), datetime.time())
    decision_at = datetime.datetime.now(datetime.UTC)
    execution_date = _execution_date(date)
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
    if (
        existing is not None
        and existing.status == "COMPLETED"
        and not replace
        and not dry_run
    ):
        logger.info("strategy already completed for %s; pass --replace to rerun", date)
        return {
            "skipped_existing": True,
            "date": date,
            "model_version": model_version,
            "config_hash": config_hash,
        }
    # NEXT.1 immutability: a FORWARD plan is certified evidence — replacing it
    # would rewrite a forward capture, so it fails closed.
    if (
        existing is not None
        and existing.status == "COMPLETED"
        and replace
        and getattr(existing, "evidence_kind", "REPLAY") == "FORWARD"
    ):
        raise ValueError(
            "cannot replace a FORWARD plan: forward evidence is immutable "
            f"({date.date()} {model_version} h={horizon})"
        )

    from app.lib.strategy_engine.runner import classify_evidence_kind

    evidence_kind = classify_evidence_kind(
        date=date,
        decision_at=decision_at,
        execution_date=execution_date,
        window=_active_window(model_version, horizon),
        config_hash=config_hash,
        existing_status=getattr(existing, "status", None),
        replace=replace,
    )

    # Previous holdings come from the most recent COMPLETED run BEFORE this
    # date (same strategy/version/horizon), so the persisted rebalance diff is
    # "what changed since the previous portfolio", not "since nothing".
    # Continuity spans BOTH evidence kinds (NEXT.1): a FORWARD run on day S is
    # the previous holdings for day S+1, exactly like a REPLAY run would be.
    previous_run = (
        StrategyPaperRun.objects(
            strategy_name=DEFAULT_STRATEGY_NAME,
            model_version=model_version,
            horizon=horizon,
            date__lt=date,
            config_hash=config_hash,
            # Continuity spans REPLAY + FORWARD (NEXT.1); pre-#202 legacy rows
            # (missing kind) are historical and stay out of the paper track.
            evidence_kind__in=["REPLAY", "FORWARD"],
            status="COMPLETED",
        )
        .order_by("-date")
        .first()
    )
    previous_holdings = (
        previous_run.target_holdings if previous_run is not None else None
    )

    predictions = _query_usable_predictions(model_version, date, horizon)
    flags = (
        _query_flags(date, horizon, model_version=model_version) if predictions else {}
    )
    plan = assemble_daily_plan(
        config=resolved,
        date=date,
        predictions=predictions,
        previous_holdings=previous_holdings,
        flags=flags,
        horizon=horizon,
    )
    if dry_run:
        return {
            "dry_run": True,
            "decision_at": decision_at,
            "execution_date": execution_date,
            "evidence_kind": evidence_kind,
            "date": date,
            "model_version": model_version,
            "horizon": horizon,
            "config_hash": config_hash,
            "previous_run_date": (
                previous_run.date.date() if previous_run is not None else None
            ),
            "plan": plan,
        }

    if existing is not None:
        existing.delete()

    # A SKIPPED day is not captured evidence: it must never carry FORWARD
    # (the counter/continuity/NAV paths already exclude SKIPPED, but a
    # FORWARD kind on a skipped doc would mislabel provenance). The same-day
    # evening rerun that succeeds is the FORWARD capture.
    persisted_kind = "REPLAY" if plan["skipped"] else evidence_kind
    run = StrategyPaperRun(
        strategy_name=DEFAULT_STRATEGY_NAME,
        date=date,
        model_version=model_version,
        horizon=horizon,
        config_hash=config_hash,
        config=resolved,
        decision_at=decision_at,
        execution_date=execution_date,
        evidence_kind=persisted_kind,
        status="SKIPPED" if plan["skipped"] else "COMPLETED",
        skip_reason=plan.get("reason"),
        target_holdings=plan.get("target_holdings", []),
        rebalance=plan.get("rebalance", {}),
    )
    run.completed_at = datetime.datetime.now(datetime.UTC)
    run.save()
    return {
        "date": date,
        "model_version": model_version,
        "horizon": horizon,
        "config_hash": config_hash,
        "status": run.status,
        "decision_at": decision_at,
        "execution_date": execution_date,
        "evidence_kind": run.evidence_kind,
        "skip_reason": run.skip_reason,
        "target_holdings_count": len(run.target_holdings or []),
        "rebalance": run.rebalance,
    }


def _load_quotes_for_codes(
    codes: list[str],
    from_date: datetime.datetime,
    to_date: datetime.datetime,
) -> dict[str, dict[str, "object"]]:
    """Load StockDailyQuote open/close/trade_status for codes over a range.

    Returns {stock_code: {date.isoformat(): QuoteView}}. Suspended days keep
    their quote row (trade_status=0) so the NAV engine rolls them forward.
    """
    from app.lib.scoring_engine.scoring_service import normalize_date
    from app.lib.strategy_engine.nav import QuoteView
    from app.model.stock import StockDailyQuote

    quotes = StockDailyQuote.objects(
        code__in=list(codes),
        date__gte=normalize_date(from_date),
        date__lte=normalize_date(to_date),
    )
    by_code: dict[str, dict[str, object]] = {}
    for q in quotes:
        day = normalize_date(q.date).date().isoformat()
        by_code.setdefault(q.code, {})[day] = QuoteView(
            open_price=q.open,
            close_price=q.close,
            trade_status=q.trade_status,
        )
    return by_code


def _benchmark_returns_for_dates(
    from_date: datetime.datetime, to_date: datetime.datetime
) -> dict[str, float]:
    """Same-date tradable-universe equal-weight return per date.

    Uses close vs previous_close over all tradable (trade_status=1) quote rows
    in the window; returns {date.isoformat(): mean return}.
    """
    from app.lib.scoring_engine.scoring_service import normalize_date
    from app.model.stock import StockDailyQuote

    quotes = StockDailyQuote.objects(
        date__gte=normalize_date(from_date),
        date__lte=normalize_date(to_date),
        trade_status=1,
    ).only("date", "code", "close", "previous_close")
    per_date: dict[str, list[float]] = {}
    for q in quotes:
        if not q.close or not q.previous_close:
            continue
        ret = q.close / q.previous_close - 1.0
        per_date.setdefault(normalize_date(q.date).date().isoformat(), []).append(ret)
    return {
        day: round(sum(returns) / len(returns), 8)
        for day, returns in per_date.items()
        if returns
    }


def run_nav(
    *,
    from_date: datetime.datetime,
    to_date: datetime.datetime,
    config: dict | None = None,
    model_version: str | None = None,
    horizon: int | None = None,
) -> dict:
    """Recompute the paper NAV curve over a range of COMPLETED runs.

    Reads COMPLETED StrategyPaperRun docs in [from, to], builds the rebalance
    schedule from their target_holdings, loads quote prices + equal-weight
    benchmark, runs simulate_paper_nav, and writes each curve point back into
    the matching run's nav_snapshot. Pure helpers (schedule_from_runs /
    attach_nav_points) live in strategy_engine.runner.
    """
    from app.lib.strategy_engine.config import validate_strategy_config
    from app.lib.strategy_engine.nav import simulate_paper_nav
    from app.lib.strategy_engine.runner import attach_nav_points, schedule_from_runs
    from app.model.strategy import StrategyPaperRun

    resolved = validate_strategy_config(config or DEFAULT_STRATEGY_CONFIG)
    eff_version = model_version or resolved["score_model_version"]
    eff_horizon = int(horizon or resolved.get("horizon", DEFAULT_HORIZON))
    resolved = validate_strategy_config(
        {**resolved, "score_model_version": eff_version, "horizon": eff_horizon}
    )
    _resolve_model_version(resolved)
    config_hash = strategy_config_hash(resolved)

    query = StrategyPaperRun.objects(
        strategy_name=DEFAULT_STRATEGY_NAME,
        model_version=eff_version,
        horizon=eff_horizon,
        config_hash=config_hash,
        evidence_kind__in=["REPLAY", "FORWARD"],
        status="COMPLETED",
        date__gte=from_date,
        date__lte=to_date,
    ).order_by("date")
    runs = list(query)
    if not runs:
        return {"found_runs": 0, "reason": "no COMPLETED runs in range"}

    schedule = schedule_from_runs(runs)
    if not schedule:
        return {"found_runs": len(runs), "reason": "no runs carry holdings"}
    execution_from = datetime.datetime.fromisoformat(schedule[0]["date"])
    execution_to = datetime.datetime.fromisoformat(schedule[-1]["date"])

    codes = sorted({code for decision in schedule for code in decision["holdings"]})
    prices = _load_quotes_for_codes(codes, execution_from, execution_to)
    benchmark = _benchmark_returns_for_dates(execution_from, execution_to)

    if not prices:
        return {
            "found_runs": len(runs),
            "reason": (
                f"no StockDailyQuote rows for {len(codes)} held codes in "
                "[from, to]; refusing to write a flat cash nav_snapshot"
            ),
        }

    covered_dates = {day for quotes in prices.values() for day in quotes}
    missing_dates = [
        item["date"] for item in schedule if item["date"] not in covered_dates
    ]
    if missing_dates:
        raise ValueError(f"missing execution-day quote coverage: {missing_dates}")

    result = simulate_paper_nav(
        prices=prices,
        schedule=schedule,
        benchmark_returns=benchmark,
        initial_nav=float(resolved.get("initial_nav", 1_000_000.0)),
    )
    attached = attach_nav_points(runs, result["curve"])

    updated = 0
    for run in runs:
        key = run.date.date().isoformat()
        point = attached["points_by_date"].get(key)
        if point is None:
            continue
        run.nav_snapshot = point
        run.save()
        updated += 1

    return {
        "found_runs": len(runs),
        "updated_runs": updated,
        "initial_nav": result["initial_nav"],
        "terminal_nav": result["terminal_nav"],
        "curve_points": len(result["curve"]),
        "benchmark_dates": len(benchmark),
        "config_hash": config_hash,
        "evidence_kinds": sorted(
            {getattr(r, "evidence_kind", "REPLAY") or "REPLAY" for r in runs}
        ),
        "execution_from": execution_from.date().isoformat(),
        "execution_to": execution_to.date().isoformat(),
        "unmatched_dates": [d.date().isoformat() for d in attached["unmatched_dates"]],
    }


def _resolve_start_date():
    """Certification session = the closest trading session to now (never
    backdated; NEXT.1 start_date semantics)."""
    from app.lib.utilities.trading_day_helper import (
        determine_closest_trading_date,
        get_a_stock_market_trade_calendar,
    )

    return determine_closest_trading_date(get_a_stock_market_trade_calendar())


def certify_forward_window(*, model_version: str, horizon: int, config: dict) -> dict:
    """Certify an ACTIVE forward-evidence window (NEXT.1).

    Closes any ACTIVE predecessor on (model_version, horizon); start_date =
    the certification session; config_hash = the validated config hash. The
    score source must be ACTIVE-registered and cover the horizon (same gate as
    a strategy run). Append-only: an existing ACTIVE window is closed, never
    mutated in place.
    """
    from app.model.strategy import StrategyForwardWindow

    resolved = validate_strategy_config(config or DEFAULT_STRATEGY_CONFIG)
    resolved = validate_strategy_config(
        {**resolved, "score_model_version": model_version, "horizon": horizon}
    )
    eff_version = _resolve_model_version(resolved)
    eff_horizon = int(resolved.get("horizon"))
    config_hash = strategy_config_hash(resolved)
    now = datetime.datetime.now(datetime.UTC)

    closed = []
    for w in StrategyForwardWindow.objects(
        model_version=eff_version, horizon=eff_horizon, status="ACTIVE"
    ):
        w.status = "CLOSED"
        w.closed_at = now
        w.save()
        closed.append(w.config_hash)

    start_date = _resolve_start_date()
    window = StrategyForwardWindow(
        model_version=eff_version,
        horizon=eff_horizon,
        config_hash=config_hash,
        start_date=start_date,
        status="ACTIVE",
        decision_at=now,
    )
    window.save()
    return {
        "model_version": eff_version,
        "horizon": eff_horizon,
        "config_hash": config_hash,
        "start_date": start_date,
        "status": window.status,
        "decision_at": now,
        "closed_predecessors": closed,
    }


def close_forward_window(*, model_version: str, horizon: int) -> dict:
    """Explicitly close the ACTIVE window (operator pause / config change)."""
    from app.model.strategy import StrategyForwardWindow

    now = datetime.datetime.now(datetime.UTC)
    closed = []
    for w in StrategyForwardWindow.objects(
        model_version=model_version, horizon=horizon, status="ACTIVE"
    ):
        w.status = "CLOSED"
        w.closed_at = now
        w.save()
        closed.append(w.config_hash)
    return {"closed_windows": closed, "closed_at": now}


def forward_progress(*, model_version: str, horizon: int) -> dict:
    """120-session counter over an ACTIVE (or CLOSED) certified window.

    Counts distinct FORWARD COMPLETED signal dates at/after the window's
    start_date. REPLAY/SKIPPED/FAILED records, job SUCCESS and NAV recomputes
    never count; gaps are reported, never fabricated.
    """
    from app.model.strategy import StrategyForwardWindow, StrategyPaperRun

    window = (
        StrategyForwardWindow.objects(
            model_version=model_version, horizon=horizon, status="ACTIVE"
        ).first()
        or StrategyForwardWindow.objects(
            model_version=model_version, horizon=horizon, status="CLOSED"
        )
        .order_by("-closed_at")
        .first()
    )
    if window is None:
        return {
            "certified": False,
            "count": 0,
            "target": 120,
            "reason": "no certified forward window for this model/horizon",
        }
    runs = list(
        StrategyPaperRun.objects(
            strategy_name=DEFAULT_STRATEGY_NAME,
            model_version=model_version,
            horizon=horizon,
            # Only runs whose stored config matches THIS window's config count
            # toward its 120-session gate (spec: evidence cannot span configs;
            # a same-session re-cert must not inherit predecessor runs).
            config_hash=window.config_hash,
            evidence_kind="FORWARD",
            status="COMPLETED",
            date__gte=window.start_date,
        ).order_by("date")
    )
    dates = sorted({r.date.date().isoformat() for r in runs})
    gaps = []
    leading = 0
    if dates:
        from app.lib.utilities.trading_day_helper import (
            get_a_stock_market_trade_calendar,
        )

        sessions = sorted(
            d.date().isoformat()
            for d in get_a_stock_market_trade_calendar()
            if d.date().isoformat() >= window.start_date.date().isoformat()
        )
        expected = [d for d in sessions if d <= dates[-1]]
        present = set(dates)
        gaps = [d for d in expected if d not in present]
        for d in expected:
            if d in present:
                leading += 1
            else:
                break
    return {
        "certified": True,
        "window_status": window.status,
        "config_hash": window.config_hash,
        "start_date": window.start_date.date().isoformat(),
        "count": len(dates),
        "target": 120,
        "first_date": dates[0] if dates else None,
        "last_date": dates[-1] if dates else None,
        "contiguous_leading_span": leading,
        "gap_dates": gaps,
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

    p_nav = sub.add_parser(
        "nav", help="Recompute paper NAV curve over COMPLETED runs in a range"
    )
    p_nav.add_argument("--from", dest="from_date", required=True)
    p_nav.add_argument("--to", dest="to_date", required=True)
    p_nav.add_argument("--config-json", default=None)
    p_nav.add_argument("--model-version", default=None)
    p_nav.add_argument("--horizon", type=int, choices=[5, 20, 60], default=None)

    p_fwd = sub.add_parser("forward", help="Certified forward evidence window (NEXT.1)")
    fsub = p_fwd.add_subparsers(dest="forward_command", required=True)
    p_cert = fsub.add_parser("certify", help="Open an ACTIVE forward window")
    p_cert.add_argument("--model-version", required=True)
    p_cert.add_argument("--horizon", type=int, choices=[5, 20, 60], required=True)
    p_cert.add_argument("--config-json", default=None)
    p_close = fsub.add_parser("close", help="Close the ACTIVE forward window")
    p_close.add_argument("--model-version", required=True)
    p_close.add_argument("--horizon", type=int, choices=[5, 20, 60], required=True)
    p_prog = fsub.add_parser("progress", help="120-session forward counter")
    p_prog.add_argument("--model-version", required=True)
    p_prog.add_argument("--horizon", type=int, choices=[5, 20, 60], required=True)
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
        if args.dry_run:
            # Preview only: nothing persisted -> record SKIPPED (not SUCCESS),
            # with the resolved score source for traceability.
            status = job_run_helper.STATUS_SKIPPED
        elif result.get("skipped_existing"):
            status = job_run_helper.STATUS_SKIPPED
        elif result.get("status") == "SKIPPED":
            status = job_run_helper.STATUS_SKIPPED
        else:
            status = job_run_helper.STATUS_SUCCESS
        job_run_helper.finish_job_run(
            job_run,
            status=status,
            summary={
                "target": args.date,
                "model_version": result.get("model_version"),
                "config_hash": result.get("config_hash"),
                "evidence_kind": result.get("evidence_kind"),
                "written_total": (
                    1
                    if status == job_run_helper.STATUS_SUCCESS and not args.dry_run
                    else 0
                ),
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
    elif args.command == "nav":
        config = json.loads(args.config_json) if args.config_json else None
        result = run_nav(
            from_date=parse_date(args.from_date),
            to_date=parse_date(args.to_date),
            config=config,
            model_version=args.model_version,
            horizon=args.horizon,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    elif args.command == "forward":
        config = (
            json.loads(args.config_json) if getattr(args, "config_json", None) else None
        )
        if args.forward_command == "certify":
            result = certify_forward_window(
                model_version=args.model_version,
                horizon=args.horizon,
                config=config,
            )
        elif args.forward_command == "close":
            result = close_forward_window(
                model_version=args.model_version, horizon=args.horizon
            )
        else:
            result = forward_progress(
                model_version=args.model_version, horizon=args.horizon
            )
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
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
                        "decision_at": doc.decision_at,
                        "execution_date": doc.execution_date,
                        "evidence_kind": doc.evidence_kind or "REPLAY",
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
