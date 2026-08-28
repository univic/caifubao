from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo
from typing import Any

from mongoengine.errors import NotUniqueError
from pymongo.errors import DuplicateKeyError

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


def mark_stale_running_job_runs_failed(
    max_age_minutes: int = 180,
    now: datetime.datetime | None = None,
) -> int:
    """Reap RUNNING records whose process died without finishing the run.

    Mirrors the max-age window of ``has_active_job_run``: anything still
    RUNNING after ``max_age_minutes`` cannot be a live run protected by the
    deployment (cron pods are killed at 2h via activeDeadlineSeconds), so it
    is an orphan that would otherwise pollute latest-run queries forever.
    The original document fields are preserved; only status, completed_at,
    and an explanatory error_message are set.
    """
    reference = now or utc_now_naive()
    cutoff = reference - datetime.timedelta(minutes=max_age_minutes)
    marked_at = utc_now_naive()
    updated = DatahubJobRun.objects(
        status=STATUS_RUNNING,
        started_at__lt=cutoff,
    ).update(
        set__status=STATUS_FAILED,
        set__completed_at=marked_at,
        set__error_message=(
            "Marked FAILED by startup cleanup "
            f"{marked_at.isoformat()}Z: stale RUNNING record with no "
            f"completion recorded within {max_age_minutes} minutes; the "
            "original process likely died before finishing the run."
        ),
    )
    updated = int(updated or 0)
    if updated:
        logger.info(
            "Startup cleanup marked %s stale RUNNING job run(s) as FAILED "
            "(started before %s)",
            updated,
            cutoff.isoformat(),
        )
    return updated
