"""Fail-closed scheduled research FORWARD strategy capture.

This is NOT a broker or a trading-paper account. The CronJob remains suspended
until an operator explicitly certifies and pins a StrategySpec window, reviews
the halt mount, and enables daily writes. Holiday runs skip with no plan write.
Backdating, unapproved specs and degraded critical inputs never make evidence.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
from pathlib import Path
from zoneinfo import ZoneInfo

BEIJING = ZoneInfo("Asia/Shanghai")
# Daily ETF/scoring-shadow may take time; scheduled for 22:15 CST.
EARLIEST_HOUR = 19
EARLIEST_MINUTE = 30

logger = logging.getLogger(__name__)


def _as_date(value) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])


def verify_daily_gate(
    *,
    now_beijing: dt.datetime,
    calendar: list,
    report: dict,
    window,
    config_hash: str,
    model_version: str,
    horizon: int,
    execution_date: dt.datetime | None = None,
) -> str:
    """Pure pre-write validation. Returns RUN or SKIP_NON_SESSION.

    A historical catch-up cannot pass because the scheduled date is always
    today's Beijing session and must precede the next session's open.
    """
    if now_beijing.tzinfo is None:
        raise ValueError("now_beijing must be timezone-aware")
    now = now_beijing.astimezone(BEIJING)
    today = now.date()
    dates = {_as_date(d) for d in calendar}
    if not dates or max(dates) <= today:
        raise ValueError("trading calendar missing forward coverage")
    if today not in dates:
        return "SKIP_NON_SESSION"

    if (now.hour, now.minute) < (EARLIEST_HOUR, EARLIEST_MINUTE):
        raise ValueError("strategy may run only after the evening score cutoff")
    if report.get("expected_session") != today.isoformat():
        raise ValueError("readiness did not assess today's closed session")
    checks = {item["id"]: item["status"] for item in report.get("checks", [])}
    # WARN on one of these is not sufficient to record a new strategy plan.
    for check in (
        "quote_freshness",
        "fq_coverage",
        "scoring_freshness",
        "signal_freshness",
        "halt_switch",
        "etf_market_data",
    ):
        if checks.get(check) != "PASS":
            raise ValueError(f"critical readiness check is not PASS: {check}")
    if report.get("overall") == "NOT_READY":
        raise ValueError("paper readiness is NOT_READY")
    if window is None or getattr(window, "status", None) != "ACTIVE":
        raise ValueError("no operator-certified ACTIVE forward window")
    if getattr(window, "model_version", None) != model_version:
        raise ValueError("forward window model differs from pinned StrategySpec")
    if int(getattr(window, "horizon", -1)) != horizon:
        raise ValueError("forward window horizon differs from pinned StrategySpec")
    if getattr(window, "config_hash", None) != config_hash:
        raise ValueError("forward window hash differs from pinned StrategySpec")
    if getattr(window, "hash_kind", None) != "spec":
        raise ValueError("forward window must pin hash_kind=spec")
    if _as_date(window.start_date) > today:
        raise ValueError("forward window begins after the scheduled session")
    if execution_date is None:
        raise ValueError("missing next execution session")
    # Mandatory: the plan must be persisted before the next execution open.
    from app.lib.strategy_engine.runner import _session_open_utc

    if now.astimezone(dt.timezone.utc) >= _session_open_utc(execution_date):
        raise ValueError("causal forward cutoff passed; refusing a retrospective run")
    return "RUN"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Guarded, same-day, spec-pinned FORWARD research run"
    )
    parser.add_argument(
        "--spec",
        required=True,
        help="operator-approved immutable StrategySpec JSON mounted read-only",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    from app.jobs import strategy_runner
    from app.jobs.paper_readiness import _database
    from app.lib import paper_readiness
    from app.lib.strategy_engine.spec import spec_hash, spec_to_legacy_config
    from app.lib.utilities import job_run_helper
    from app.model.strategy import StrategyPaperRun

    args = build_parser().parse_args(argv)
    with Path(args.spec).open(encoding="utf-8") as stream:
        spec = json.load(stream)
    config = spec_to_legacy_config(spec)
    identity = spec_hash(spec)
    model_version = config["score_model_version"]
    horizon = int(config["horizon"])
    now = dt.datetime.now(BEIJING)
    today = now.date()

    # A configured path is not sufficient: the actual persistent file must
    # already exist, be an ordinary readable JSON file with explicit bool OFF.
    # No job-run DB write (or strategy plan) may precede this safety gate.
    from app.lib.strategy_engine.halt import assert_persisted_forward_halt_off

    assert_persisted_forward_halt_off()

    # No schema fallback, implicit strategy, unsafe --replace or backdate flag.
    strategy_runner._init_db()
    context = job_run_helper.JobRunContext(
        job_name="research_strategy_forward_daily",
        job_family="strategy_daily",
        trigger="cron",
        source="k8s-cronjob",
        scheduled_at=job_run_helper.compute_daily_schedule_at(22, 15),
    )
    job_run = job_run_helper.create_job_run(context)
    try:
        inputs = paper_readiness.load_inputs(
            _database(),
            model_version=model_version,
            horizon=horizon,
            now_beijing=now.replace(tzinfo=None),
        )
        report = paper_readiness.evaluate(inputs)
        window = strategy_runner._active_window(model_version, horizon)
        session_dates = {_as_date(day) for day in inputs.calendar}
        next_session = (
            strategy_runner._execution_date(dt.datetime.combine(today, dt.time()))
            if today in session_dates
            else None
        )
        verdict = verify_daily_gate(
            now_beijing=now,
            calendar=inputs.calendar,
            report=report,
            window=window,
            config_hash=identity,
            model_version=model_version,
            horizon=horizon,
            execution_date=next_session,
        )
        if verdict == "SKIP_NON_SESSION":
            result = {"status": verdict, "date": today.isoformat()}
            job_run_helper.finish_job_run(
                job_run, status=job_run_helper.STATUS_SKIPPED, summary=result
            )
            print(json.dumps(result))
            return 0

        target = dt.datetime.combine(today, dt.time())
        existing = StrategyPaperRun.objects(
            strategy_name=strategy_runner.DEFAULT_STRATEGY_NAME,
            date=target,
            model_version=model_version,
            horizon=horizon,
            config_hash=identity,
        ).first()
        if existing is not None and existing.status == "COMPLETED":
            if existing.evidence_kind != "FORWARD" or existing.hash_kind != "spec":
                raise ValueError(
                    "existing completed plan is not matching FORWARD evidence"
                )
            result = {"status": "ALREADY_COMPLETED", "date": today.isoformat()}
            job_run_helper.finish_job_run(
                job_run, status=job_run_helper.STATUS_SKIPPED, summary=result
            )
            print(json.dumps(result))
            return 0

        # run_strategy revalidates halt, scores, registry and causal timing;
        # no replace flag, no automatic certify, no trading account writes.
        rebalance = spec.get("rebalance") or {}
        policy = {
            key: rebalance[key]
            for key in ("exit_buffer", "min_hold_sessions")
            if key in rebalance
        }
        result = strategy_runner.run_strategy(
            date=target,
            config=config,
            strategy_spec_hash=identity,
            rebalance_policy=policy or None,
        )
        if (
            result.get("status") != "COMPLETED"
            or result.get("evidence_kind") != "FORWARD"
        ):
            raise ValueError(
                "strategy did not complete as FORWARD; no success may be claimed"
            )
        job_run_helper.finish_job_run(
            job_run,
            status=job_run_helper.STATUS_SUCCESS,
            summary={
                "target": today.isoformat(),
                "model_version": model_version,
                "config_hash": identity,
                "hash_kind": "spec",
                "evidence_kind": "FORWARD",
                "written_total": 1,
                **(
                    {"eligibility": result["eligibility"]}
                    if "eligibility" in result
                    else {}
                ),
            },
        )
        print(json.dumps(result, ensure_ascii=False, default=str))
        return 0
    except Exception as exc:
        job_run_helper.finish_job_run(
            job_run, status=job_run_helper.STATUS_FAILED, error_message=str(exc)
        )
        logger.exception("research FORWARD daily capture failed")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
