from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo
from typing import Any

from mongoengine.errors import NotUniqueError
from pymongo.errors import DuplicateKeyError, OperationFailure

from app.model.datahub_job_run import (
    JOB_NAME_STARTUP_CATCHUP,
    STATUS_FAILED,
    STATUS_RUNNING,
    STATUS_SKIPPED,
    STATUS_SUCCESS,
    DatahubJobRun,
)


logger = logging.getLogger(__name__)


BEIJING_TZ_NAME = "Asia/Shanghai"
BEIJING_TZ = ZoneInfo(BEIJING_TZ_NAME)
UTC = datetime.timezone.utc

DEFAULT_QUOTE_JOB_FAMILY = "quote_daily"
DEFAULT_QUOTE_JOB_NAME = "datahub_quote_daily"
DEFAULT_QUOTE_JOB_TRIGGER = "cron"
DEFAULT_QUOTE_JOB_SOURCE = "k8s-cronjob"
DEFAULT_QUOTE_CATCHUP_JOB_NAME = JOB_NAME_STARTUP_CATCHUP
DEFAULT_QUOTE_CATCHUP_TRIGGER = "startup"
DEFAULT_QUOTE_CATCHUP_SOURCE = "datahub-service"
DEFAULT_QUOTE_JOB_HOUR = 18
DEFAULT_QUOTE_JOB_MINUTE = 10
DEFAULT_INDEX_JOB_NAME = "datahub_quote_index_daily"
DEFAULT_INDEX_JOB_HOUR = 18
DEFAULT_INDEX_JOB_MINUTE = 0


@dataclass(frozen=True)
class JobRunContext:
    job_name: str
    job_family: str
    trigger: str
    source: str
    scheduled_at: datetime.datetime | None = None
    target: str | None = None
    include_factors: bool = False
    extra: dict[str, Any] = field(default_factory=dict)


class JobRunClaimExistsError(RuntimeError):
    """An identical RUNNING job run already holds the uniqueness claim."""


def utc_now_naive() -> datetime.datetime:
    return datetime.datetime.now(UTC).replace(tzinfo=None)


def beijing_now() -> datetime.datetime:
    return datetime.datetime.now(BEIJING_TZ)


def normalize_datetime(value: datetime.datetime | None) -> datetime.datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def compute_daily_schedule_at(
    hour: int,
    minute: int,
    *,
    reference_time: datetime.datetime | None = None,
    timezone_name: str = BEIJING_TZ_NAME,
) -> datetime.datetime:
    timezone = ZoneInfo(timezone_name)
    reference = reference_time or datetime.datetime.now(timezone)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone)
    scheduled = reference.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return normalize_datetime(scheduled)


def create_job_run(context: JobRunContext) -> DatahubJobRun:
    job_run = DatahubJobRun(
        job_family=context.job_family,
        job_name=context.job_name,
        trigger=context.trigger,
        source=context.source,
        target=context.target,
        include_factors=context.include_factors,
        scheduled_at=normalize_datetime(context.scheduled_at),
        status=STATUS_RUNNING,
        started_at=utc_now_naive(),
        phase_stats={},
        summary={},
        extra=context.extra,
    )
    try:
        job_run.save()
    except (DuplicateKeyError, NotUniqueError) as error:
        # A unique partial index guards the startup catch-up against the
        # check-then-create race across overlapping processes/rollouts: the
        # losing caller must not run the job, so this is a hard stop.
        raise JobRunClaimExistsError(
            "an active RUNNING job run already holds the claim for "
            f"job_family={context.job_family} "
            f"job_name={context.job_name} "
            f"scheduled_at={context.scheduled_at}"
        ) from error
    except OperationFailure:
        # mongoengine ensures indexes on the first collection touch of a
        # process. If that ensure fails for a NON-duplicate reason, the first
        # save raises before writing anything, while every later touch in the
        # same process succeeds because the collection is then cached. Retry
        # once so a failed index creation degrades the race protection instead
        # of crashing whichever runner happened to touch the model first.
        # (A duplicate-blocked index build raises DuplicateKeyError, which is
        # claimed above and never retried here.)
        logger.warning(
            "First save of job run failed during index ensure; retrying once"
        )
        job_run.save()
    return job_run


def finish_job_run(
    job_run: DatahubJobRun,
    *,
    status: str,
    summary: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> DatahubJobRun:
    summary = summary or {}
    updates = {
        "set__completed_at": utc_now_naive(),
        "set__status": status,
        "set__failed_phase": summary.get("failed_phase"),
        "set__pulled_total": int(summary.get("pulled_total", 0) or 0),
        "set__written_total": int(summary.get("written_total", 0) or 0),
        "set__phase_stats": summary.get("phase_stats", {}),
        "set__summary": summary,
        "set__error_message": error_message,
    }
    if summary.get("target"):
        updates["set__target"] = summary.get("target")
    if summary.get("include_factors") is not None:
        updates["set__include_factors"] = bool(summary.get("include_factors"))
    job_run.update(**updates)
    return job_run.reload()


def mark_job_run_skipped(
    context: JobRunContext,
    *,
    summary: dict[str, Any] | None = None,
) -> DatahubJobRun:
    job_run = create_job_run(context)
    return finish_job_run(job_run, status=STATUS_SKIPPED, summary=summary)


def update_job_run_progress(
    job_run: DatahubJobRun,
    *,
    failed_phase: str | None = None,
    pulled_total: int = 0,
    written_total: int = 0,
    phase_stats: dict[str, Any] | None = None,
) -> DatahubJobRun:
    """Best-effort persistence of a still-running job's partial progress.

    Called after each completed phase so that a process killed mid-run
    (activeDeadlineSeconds expiry, OOM, node loss) still leaves evidence of
    which phases completed and how much data was written. Without this, a
    killed run would otherwise look like it wrote nothing (the 2026-08-28
    quote-run incident: quotes and factors were persisted, yet the run record
    stayed at pulled=0/written=0 until the reaper marked it FAILED).

    Never raises: progress persistence must not break the data job itself.
    """
    updates = {
        "set__failed_phase": failed_phase,
        "set__pulled_total": int(pulled_total or 0),
        "set__written_total": int(written_total or 0),
        "set__phase_stats": phase_stats or {},
    }
    try:
        job_run.update(**updates)
        return job_run.reload()
    except Exception:
        logger.exception(
            "Failed to persist job run progress (job=%s); continuing", job_run.job_name
        )
        return job_run


def latest_job_run(
    *,
    job_family: str,
    scheduled_at: datetime.datetime | None = None,
    statuses: list[str] | None = None,
    job_name: str | None = None,
    target: str | None = None,
    include_factors: bool | None = None,
) -> DatahubJobRun | None:
    filters: dict = {"job_family": job_family}
    if job_name is not None:
        filters["job_name"] = job_name
    if target is not None:
        filters["target"] = target
    if include_factors is not None:
        filters["include_factors"] = include_factors
    query = DatahubJobRun.objects(**filters)
    if scheduled_at is not None:
        query = query.filter(scheduled_at=normalize_datetime(scheduled_at))
    if statuses:
        query = query.filter(status__in=statuses)
    return query.order_by("-started_at").first()


def has_active_job_run(
    *,
    job_family: str,
    scheduled_at: datetime.datetime | None = None,
    max_age_minutes: int = 180,
) -> bool:
    job_run = latest_job_run(
        job_family=job_family,
        scheduled_at=scheduled_at,
        statuses=[STATUS_RUNNING, STATUS_SUCCESS],
    )
    if not job_run:
        return False
    if job_run.status == STATUS_SUCCESS:
        return True
    started_at = job_run.started_at
    if not started_at:
        return False
    age = utc_now_naive() - started_at
    return age.total_seconds() < max_age_minutes * 60


DEFAULT_STALE_RUNNING_MAX_AGE_MINUTES = 240
STALE_RUNNING_MAX_AGE_MINUTES_BY_JOB_NAME = {
    # The catch-up runs inside the unbounded deployment process (no
    # activeDeadlineSeconds) and must survive rollouts: only reap its records
    # once they are older than any plausible full-market run (24h).
    JOB_NAME_STARTUP_CATCHUP: 1440,
}


def _reap_stale_running_group(
    *,
    filters: dict[str, Any],
    max_age_minutes: int,
    reference: datetime.datetime,
) -> int:
    cutoff = reference - datetime.timedelta(minutes=max_age_minutes)
    marked_at = utc_now_naive()
    updated = 0

    def _mark(filters_extra: dict[str, Any], note: str) -> int:
        return int(
            DatahubJobRun.objects(
                status=STATUS_RUNNING,
                started_at__lt=cutoff,
                **filters,
                **filters_extra,
            ).update(
                set__status=STATUS_FAILED,
                set__completed_at=marked_at,
                set__error_message=(
                    "Marked FAILED by startup cleanup "
                    f"{marked_at.isoformat()}Z: stale RUNNING record with no "
                    f"completion recorded within {max_age_minutes} minutes; the "
                    "original process likely died before finishing the run "
                    f"({note})."
                ),
            )
            or 0
        )

    # Runs that already persisted data before dying deserve a distinct note:
    # their written/phase stats are the only surviving evidence of a partial
    # or complete write, and downstream gates may rely on them.
    updated += _mark(
        {"written_total__gt": 0},
        "data was already written before the process died",
    )
    updated += _mark({}, "no data was written")
    if updated:
        logger.info(
            "Startup cleanup marked %s stale RUNNING job run(s) as FAILED "
            "(filters=%s, started before %s)",
            updated,
            filters or "any job",
            cutoff.isoformat(),
        )
    return updated


def mark_stale_running_job_runs_failed(
    max_age_minutes: int = DEFAULT_STALE_RUNNING_MAX_AGE_MINUTES,
    now: datetime.datetime | None = None,
) -> int:
    """Reap RUNNING records whose process died without finishing the run.

    Anything still RUNNING past its window is an orphan: it would otherwise
    pollute latest-run queries forever. The window must exceed every live
    run's deadline — cron pods are killed by activeDeadlineSeconds (max 3h
    for scoring, so the default is 4h) — and job names with unbounded or
    longer windows get their own entry in
    ``STALE_RUNNING_MAX_AGE_MINUTES_BY_JOB_NAME``. The original document
    fields are preserved; only status, completed_at, and an explanatory
    error_message are set.
    """
    reference = now or utc_now_naive()
    overridden = STALE_RUNNING_MAX_AGE_MINUTES_BY_JOB_NAME
    updated = 0
    if overridden:
        updated += _reap_stale_running_group(
            filters={"job_name__nin": sorted(overridden)},
            max_age_minutes=max_age_minutes,
            reference=reference,
        )
    else:
        updated += _reap_stale_running_group(
            filters={},
            max_age_minutes=max_age_minutes,
            reference=reference,
        )
    for name, minutes in overridden.items():
        updated += _reap_stale_running_group(
            filters={"job_name": name},
            max_age_minutes=minutes,
            reference=reference,
        )
    return updated
