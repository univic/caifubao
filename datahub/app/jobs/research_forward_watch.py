"""Read-only Research FORWARD gap watcher, suitable for a suspended CronJob.

Checks unique immutable FORWARD COMPLETED signal sessions from a certified
window. Missed sessions remain gaps; no replay or backfill is ever written.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
from zoneinfo import ZoneInfo

from app.lib.utilities import job_run_helper

logger = logging.getLogger(__name__)
BEIJING = ZoneInfo("Asia/Shanghai")
WATCHED_JOB_NAME = "research_strategy_forward_daily"


def _date(value) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])


def evaluate_forward_gap(
    *,
    now: dt.datetime,
    calendar: list,
    window: dict | None,
    runs: list[dict],
    job_runs: list[dict],
) -> dict:
    """Pure session-level evidence check, not a portfolio/NAV checker."""
    if now.tzinfo is None:
        raise ValueError("now must be timezone aware")
    today = now.astimezone(BEIJING).date()
    sessions = sorted({_date(s) for s in calendar})
    if not sessions or max(sessions) <= today:
        return {
            "healthy": False,
            "status": "CALENDAR_MISSING_FUTURE",
            "date": str(today),
        }
    # Mon-Fri scheduled cron may hit an exchange holiday. Nothing is due.
    if today not in sessions:
        return {"healthy": True, "status": "NON_TRADING_DAY", "date": str(today)}
    if now.astimezone(BEIJING).hour < 23:
        return {"healthy": False, "status": "NOT_DUE", "date": str(today)}
    if not window or window.get("status") != "ACTIVE":
        return {"healthy": False, "status": "NO_ACTIVE_WINDOW", "date": str(today)}
    start_date = _date(window["start_date"])
    if start_date > today:
        return {"healthy": False, "status": "FUTURE_WINDOW", "date": str(today)}
    if window.get("hash_kind") != "spec" or not window.get("config_hash"):
        return {
            "healthy": False,
            "status": "INVALID_WINDOW_IDENTITY",
            "date": str(today),
        }
    expected = [date for date in sessions if start_date <= date <= today]
    seen: set[dt.date] = set()
    valid_session_occurrences: dict[dt.date, int] = {}
    invalid = 0
    for run in runs:
        run_date = _date(run["date"])
        if not start_date <= run_date <= today:
            continue
        if (
            run.get("status") != "COMPLETED"
            or run.get("evidence_kind") != "FORWARD"
            or run.get("config_hash") != window["config_hash"]
            or run.get("hash_kind") != "spec"
            or run.get("model_version") != window.get("model_version")
            or int(run.get("horizon") or -1) != int(window.get("horizon") or -2)
        ):
            invalid += 1
            continue
        seen.add(run_date)
        valid_session_occurrences[run_date] = (
            valid_session_occurrences.get(run_date, 0) + 1
        )
    duplicate_dates = sorted(
        date.isoformat()
        for date, count in valid_session_occurrences.items()
        if count > 1
    )
    missing = [date.isoformat() for date in expected if date not in seen]

    # DatahubJobRun has the name/summary of the app, NOT the Kubernetes Job's
    # controller ownerReference. A manually created Job cloned from a CronJob
    # produces the SAME app-level job name, trigger and SUCCESS summary.
    # Never call that evidence a verified unattended CronJob execution; only
    # a Kubernetes observer checking the Job->CronJob owner UID can attest it.
    def _attempt_today(run: dict) -> bool:
        if (run.get("summary") or {}).get("target") == today.isoformat():
            return True
        started_at = run.get("started_at")
        if not isinstance(started_at, dt.datetime):
            return False
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=dt.timezone.utc)
        return started_at.astimezone(BEIJING).date() == today

    failed_job_attempts = sum(
        1
        for run in job_runs
        if run.get("status") in ("FAILED", "DEGRADED") and _attempt_today(run)
    )
    recent_job = any(
        run.get("status") == "SUCCESS"
        and (run.get("summary") or {}).get("target") == today.isoformat()
        and (run.get("summary") or {}).get("config_hash") == window["config_hash"]
        for run in job_runs
    )
    healthy = not missing and not duplicate_dates and not failed_job_attempts
    status = (
        "MISSING_FORWARD_SESSIONS"
        if missing
        else "DUPLICATE_FORWARD_SESSIONS"
        if duplicate_dates
        else "CAPTURE_JOB_FAILURE"
        if failed_job_attempts
        else "COMPLETE"
    )
    # A legitimate, independently certified operator rescue keeps the
    # evidence complete, but is NOT proof the unattended CronJob worked.
    # Reports need both dimensions to avoid greenwashing a manual rescue.
    automation_healthy = False  # Kubernetes ownerRef proof is unavailable here.
    return {
        "healthy": healthy,
        "status": status,
        "date": today.isoformat(),
        "model_version": window.get("model_version"),
        "horizon": window.get("horizon"),
        "hash_kind": "spec",
        "config_hash": window["config_hash"],
        "expected_count": len(expected),
        "forward_count": len(seen),
        "missing_dates": missing,
        "duplicate_forward_dates": duplicate_dates,
        "invalid_runs_excluded": invalid,
        # Retain the old key but refuse to claim an automated success without
        # trusted CronJob ownerRef; expose the actual JobRun status separately.
        "automated_job_success_today": False,
        "job_run_success_today": recent_job,
        "automation_healthy": automation_healthy,
        "scheduler_attestation": "NOT_VERIFIED_BY_KUBERNETES",
        "capture_source_today": (
            "JOB_RUN_ORIGIN_UNVERIFIED" if recent_job else "MANUAL_OR_UNVERIFIED"
        ),
        "failed_capture_jobs_today": failed_job_attempts,
        "forward_target_sessions": 120,
    }


def load_report(*, model_version: str, horizon: int) -> dict:
    from app.jobs.strategy_runner import _init_db
    from app.model.stock import FinanceMarket
    from app.model.strategy import StrategyForwardWindow, StrategyPaperRun

    _init_db()
    market = FinanceMarket.objects(name="ChinaAStock").first()
    calendar = list(getattr(market, "trade_calendar", None) or [])
    window_row = StrategyForwardWindow.objects(
        status="ACTIVE", model_version=model_version, horizon=horizon
    ).first()
    window = None
    if window_row is not None:
        window = {
            "status": window_row.status,
            "start_date": window_row.start_date,
            "config_hash": window_row.config_hash,
            "hash_kind": getattr(window_row, "hash_kind", None) or "legacy",
            "model_version": window_row.model_version,
            "horizon": window_row.horizon,
        }
    runs = (
        [
            {
                "date": x.date,
                "status": x.status,
                "evidence_kind": x.evidence_kind,
                "config_hash": x.config_hash,
                "hash_kind": getattr(x, "hash_kind", None) or "legacy",
                "model_version": x.model_version,
                "horizon": x.horizon,
            }
            for x in StrategyPaperRun.objects(
                model_version=model_version,
                horizon=horizon,
                date__gte=window["start_date"],
            )
        ]
        if window is not None
        else []
    )
    # Session record is not guaranteed to be a CronJob. Only today's recent
    # attempts are relevant to the operator's automation coverage.
    utc_today = dt.datetime.now(dt.timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    job_runs = [
        {"status": x.status, "summary": x.summary, "started_at": x.started_at}
        for x in job_run_helper.DatahubJobRun.objects(
            job_name=WATCHED_JOB_NAME,
            started_at__gte=(utc_today - dt.timedelta(days=1)).replace(tzinfo=None),
        )
    ]
    return evaluate_forward_gap(
        now=dt.datetime.now(BEIJING),
        calendar=calendar,
        window=window,
        runs=runs,
        job_runs=job_runs,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--horizon", required=True, type=int, choices=[5, 20, 60])
    parser.add_argument("--webhook", default=None)
    args = parser.parse_args(argv)
    report = load_report(model_version=args.model_version, horizon=args.horizon)
    print(json.dumps(report, ensure_ascii=False))
    webhook = args.webhook or os.getenv("HEALTH_WEBHOOK_URL")
    if webhook and not report["healthy"]:
        from app.jobs.health_watcher import _post_webhook

        _post_webhook(webhook, report)
    return 0 if report["healthy"] else 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
