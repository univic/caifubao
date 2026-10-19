from __future__ import annotations

import argparse
import datetime
import json
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from app.lib.utilities import job_run_helper
from app.lib.utilities import pipeline_degradation
from app.lib.signal_factory import SignalUpdateError

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
DEPENDENCY_JOB_NAME = "datahub_quote_stock_daily"
DEPENDENCY_JOB_HOUR = 18
DEPENDENCY_JOB_MINUTE = 0
# A DEGRADED upstream run is only usable when its validated coverage clears
# this floor and the MA factor phase actually wrote rows.
DEPENDENCY_QUOTE_COVERAGE_FLOOR = pipeline_degradation.QUOTE_COVERAGE_FLOOR


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
        "skipped_codes": [],
        "skipped_signal_count": 0,
        "all_skipped": False,
        "failed_count": 0,
        "failed_codes": [],
        "codes": selected_codes if dry_run else selected_codes[:20],
    }
    if dry_run:
        return result

    if mode == MODE_STALE and hasattr(service, "update_market"):
        batch_result = service.update_market(
            market=market, selected_codes=selected_codes
        )
        result["written_count"] = int(batch_result.get("written_count", 0))
        result["skipped_count"] = int(batch_result.get("skipped_count", 0))
        result["skipped_codes"] = list(batch_result.get("skipped_codes", []))
        result["skipped_signal_count"] = int(
            batch_result.get("skipped_signal_count", 0)
        )
        result["failed_count"] = int(batch_result.get("failed_count", 0))
        result["failed_codes"] = list(batch_result.get("failed_codes", []))
        result["all_skipped"] = bool(
            result["pulled_count"]
            and result["skipped_count"] == result["pulled_count"]
            and result["written_count"] == 0
        )
        if result["all_skipped"]:
            logger.warning(
                "%s signal run skipped every selected code (pulled=%d); no signal "
                "was written. Check the upstream factor path before trusting this "
                "run as current.",
                signal,
                result["pulled_count"],
            )
        return result

    for code in selected_codes:
        try:
            update_result = service.update_code(code, force=mode == MODE_FORCE)
        except Exception as exc:
            result["failed_count"] += 1
            result["failed_codes"].append(code)
            logger.exception("%s signal update failed: code=%s", signal, code)
            result["message"] = str(exc)
            continue

        if update_result.get("code") == "SKIP":
            # update_code already logged this skip with its reason.
            result["skipped_count"] += 1
            result["skipped_codes"].append(code)
            result["skipped_signal_count"] += len(
                update_result.get("skipped_signals") or []
            )
            continue
        if update_result.get("code") != "GOOD":
            result["failed_count"] += 1
            result["failed_codes"].append(code)
            continue
        result["skipped_signal_count"] += len(
            update_result.get("skipped_signals") or []
        )
        result["written_count"] += int(update_result.get("written_count", 0))

    if result["failed_count"]:
        raise SignalUpdateError(
            "signal update failed for codes: " + ", ".join(result["failed_codes"]),
            failed_codes=result["failed_codes"],
            written_count=result["written_count"],
        )
    result["all_skipped"] = bool(
        result["pulled_count"]
        and result["skipped_count"] == result["pulled_count"]
        and result["written_count"] == 0
    )
    return result


def _quote_validation_evidence(record) -> dict[str, Any]:
    """Extract the persisted ``quote_validation`` dict from an upstream run.

    The quote processor records it in the data-integrity phase stats; the
    quote runner also copies it to the run summary. Either location is
    accepted so old and new records both work.
    """
    phase_stats = record.phase_stats or {}
    quote_phase = phase_stats.get("check_stock_data_integrity") or {}
    validation = quote_phase.get("quote_validation")
    if not validation:
        validation = (getattr(record, "summary", None) or {}).get("quote_validation")
    return validation or {}


def _dependency_evidence(record) -> dict[str, Any]:
    """Collect the observable evidence a dependency decision is based on."""
    phase_stats = record.phase_stats or {}
    quote_phase = phase_stats.get("check_stock_data_integrity") or {}
    ma_phase = phase_stats.get("update_ma_factor") or {}
    validation = _quote_validation_evidence(record)
    quote_validated = int(quote_phase.get("validated_count", 0) or 0)
    ma_written = int(ma_phase.get("written_count", 0) or 0)
    coverage = validation.get("coverage_rate")
    return {
        "status": getattr(record, "status", None),
        "quote_validated_count": quote_validated,
        "ma_factor_written_count": ma_written,
        "coverage_rate": float(coverage) if coverage is not None else None,
        "validated_symbols": validation.get("validated_symbols"),
        "attempted_symbols": validation.get("attempted_symbols"),
        "failed_count": validation.get("failed_count"),
        "failed_codes": list(validation.get("failed_codes") or []),
    }


def _degraded_dependency_decision(record) -> dict[str, Any]:
    """Decide whether a DEGRADED upstream quote run is usable downstream.

    Degraded runs are accepted only with positive coverage evidence (at or
    above the floor) *and* persisted MA factors; a degraded run with no factor
    evidence is rejected instead of silently computing signals from stale
    factors.
    """
    evidence = _dependency_evidence(record)
    coverage = evidence["coverage_rate"]
    accepted = bool(
        coverage is not None
        and coverage >= DEPENDENCY_QUOTE_COVERAGE_FLOOR
        and evidence["ma_factor_written_count"] > 0
    )
    return {
        "accepted": accepted,
        "path": "degraded" if accepted else "rejected",
        "upstream_status": evidence["status"],
        "coverage_rate": coverage,
        "validated_symbols": evidence["validated_symbols"],
        "attempted_symbols": evidence["attempted_symbols"],
        "failed_count": evidence["failed_count"],
        "failed_codes": evidence["failed_codes"],
        "ma_factor_written_count": evidence["ma_factor_written_count"],
    }


def _evaluate_dependency() -> dict[str, Any]:
    """Evaluate the upstream quote gate and return the decision + evidence.

    Always uses the upstream job's schedule time (DEPENDENCY_JOB_HOUR/MINUTE),
    regardless of this job's own scheduled_at, to correctly match the upstream
    job's recorded scheduled_at value.

    Three paths satisfy the dependency:

    1. A SUCCESS record for today (the historical gate).
    2. A DEGRADED record for today whose persisted ``quote_validation`` coverage
       is at or above the floor *and* whose MA factor phase wrote rows. A
       bounded validation gap keeps the chain usable; a degraded run without
       factor evidence is rejected.
    3. A record (RUNNING or FAILED) whose persisted phases cover everything
       signals need: the stock data-integrity phase (validated_count > 0)
       **and** the MA factor phase (written_count > 0). This covers runs whose
       quotes and factors were persisted but whose process died before
       recording completion (deadline expiry, OOM, node loss) — the record is
       RUNNING until the reaper marks it FAILED, yet the underlying data is
       real and signals can safely be computed from it. Accepting RUNNING is
       safe because the MA-factor evidence proves the upstream phases that
       signals depend on have all completed. Note the upstream quote job may
       still be running its own tail phases (signals/scoring) concurrently;
       signal writes are idempotent upserts, so the overlap is safe.
    """
    upstream_scheduled_at = job_run_helper.compute_daily_schedule_at(
        DEPENDENCY_JOB_HOUR, DEPENDENCY_JOB_MINUTE
    )

    latest = job_run_helper.latest_job_run(
        job_family=DEPENDENCY_JOB_FAMILY,
        job_name=DEPENDENCY_JOB_NAME,
        target="stock",
        include_factors=True,
        scheduled_at=upstream_scheduled_at,
        statuses=[job_run_helper.STATUS_SUCCESS, job_run_helper.STATUS_DEGRADED],
    )
    if latest is not None:
        if latest.status == job_run_helper.STATUS_DEGRADED:
            decision = _degraded_dependency_decision(latest)
            logger.info(
                "Upstream quote run %s is DEGRADED (coverage=%s, "
                "ma_factor_written=%s, failed=%s): %s",
                DEPENDENCY_JOB_NAME,
                decision["coverage_rate"],
                decision["ma_factor_written_count"],
                decision["failed_count"],
                "accepted" if decision["accepted"] else "rejected",
            )
            return decision
        return {"accepted": True, "path": "success", "upstream_status": latest.status}

    record = job_run_helper.latest_job_run(
        job_family=DEPENDENCY_JOB_FAMILY,
        job_name=DEPENDENCY_JOB_NAME,
        target="stock",
        include_factors=True,
        scheduled_at=upstream_scheduled_at,
        statuses=[job_run_helper.STATUS_RUNNING, job_run_helper.STATUS_FAILED],
    )
    if record is None:
        return {"accepted": False, "path": "missing", "upstream_status": None}
    evidence = _dependency_evidence(record)
    accepted = (
        evidence["quote_validated_count"] > 0
        and evidence["ma_factor_written_count"] > 0
    )
    return {
        "accepted": accepted,
        "path": "crash_recovery" if accepted else "rejected",
        "upstream_status": evidence["status"],
        "quote_validated_count": evidence["quote_validated_count"],
        "ma_factor_written_count": evidence["ma_factor_written_count"],
    }


def _check_dependency() -> bool:
    """Check whether the upstream quote job is ready for signal generation."""
    return _evaluate_dependency()["accepted"]


def dependency_decision(skip: bool = False) -> dict[str, Any]:
    """Return the dependency gate decision plus the evidence behind it.

    ``dependency_ready`` keeps the historical boolean surface; ``main`` records
    this dict in the job summary so an accepted degraded run (or a rejected
    one) is auditable.
    """
    if skip:
        logger.warning(
            "Dependency check bypassed by --skip-dependency-check; the runner "
            "will not verify today's upstream quote+factor run."
        )
        return {"accepted": True, "path": "bypassed"}
    return _evaluate_dependency()


def dependency_ready(skip: bool = False) -> bool:
    """Decide whether signals may run, honouring an explicit operator bypass.

    The daily gate protects the incremental path: it proves today's upstream
    quotes and factors exist. A historical rebuild (force/full-history) does not
    depend on today's session at all, so an operator may bypass the gate
    explicitly. The bypass is deliberately opt-in, logged, and recorded in the
    job run.
    """
    return dependency_decision(skip=skip)["accepted"]


SCHEDULED_TRIGGERS = ("cron", "startup")


def validate_bypass(args: argparse.Namespace) -> None:
    """Reject a dependency bypass outside an explicit operator historical run."""
    if not args.skip_dependency_check:
        return
    if args.trigger in SCHEDULED_TRIGGERS:
        raise ValueError(
            "--skip-dependency-check is not allowed for scheduled triggers "
            f"(trigger={args.trigger!r}); it is operator-only."
        )
    if args.mode != MODE_FORCE:
        raise ValueError(
            "--skip-dependency-check requires --mode force: it exists for "
            "historical rebuilds, not for the incremental stale path."
        )


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
        "--skip-dependency-check",
        action="store_true",
        help=(
            "Operator-only: bypass the daily upstream dependency check. Use it "
            "for historical rebuilds (force/full-history), never for the "
            "scheduled daily path; the bypass is recorded in the job run."
        ),
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
    validate_bypass(args)

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

    # Check upstream dependency (operator bypass for historical rebuilds)
    dependency = dependency_decision(args.skip_dependency_check)
    if not dependency["accepted"]:
        logger.warning(
            "Dependency check failed: no usable %s (stock+factors) run found "
            "at scheduled_at=%s (decision=%s). Skipping signal run.",
            DEPENDENCY_JOB_NAME,
            scheduled_at,
            dependency,
        )
        context = job_run_helper.JobRunContext(
            job_name=args.job_name,
            job_family=args.job_family,
            trigger=args.trigger,
            source=args.source,
            scheduled_at=scheduled_at,
            extra={
                "signal": args.signal,
                "mode": args.mode,
                "dependency_check_bypassed": bool(args.skip_dependency_check),
            },
        )
        job_run_helper.mark_job_run_skipped(
            context=context,
            summary={
                "reason": "dependency_failed",
                "dependency_job_family": DEPENDENCY_JOB_FAMILY,
                "dependency_job_name": DEPENDENCY_JOB_NAME,
                "dependency_target": "stock",
                "dependency_include_factors": True,
                "dependency": dependency,
                "signal": args.signal,
                "mode": args.mode,
                "dependency_check_bypassed": bool(args.skip_dependency_check),
            },
        )
        print(
            json.dumps(
                {
                    "status": "SKIPPED",
                    "reason": f"No successful {DEPENDENCY_JOB_NAME} (stock+factors) job for today",
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
        extra={
            "signal": args.signal,
            "mode": args.mode,
            "dependency_check_bypassed": bool(args.skip_dependency_check),
        },
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
            "dependency": dependency,
            "dependency_check_bypassed": bool(args.skip_dependency_check),
            "pulled_total": result.get("pulled_count", 0),
            "written_total": result.get("written_count", 0),
            "skipped_count": result.get("skipped_count", 0),
            "skipped_codes": result.get("skipped_codes", []),
            "skipped_signal_count": result.get("skipped_signal_count", 0),
            "all_skipped": result.get("all_skipped", False),
            "failed_count": result.get("failed_count", 0),
            "failed_codes": result.get("failed_codes", []),
        }

        job_run_helper.finish_job_run(job_run, status="SUCCESS", summary=summary)
        print(json.dumps(result, default=str, ensure_ascii=False, indent=2))

    except Exception as exc:
        # Preserve signals persisted before the failure so the FAILED record
        # still shows real data was written (downstream scoring gates on
        # written_total > 0).
        job_run_helper.finish_job_run(
            job_run,
            status="FAILED",
            summary={
                "signal": args.signal,
                "mode": args.mode,
                "dependency": dependency,
                "written_total": int(getattr(exc, "written_count", 0) or 0),
                "failed_codes": list(getattr(exc, "failed_codes", [])),
                "dependency_check_bypassed": bool(args.skip_dependency_check),
            },
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    main()
