from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo
from typing import Any

from app.model.datahub_job_run import (
    STATUS_RUNNING,
    STATUS_SKIPPED,
    STATUS_SUCCESS,
    DatahubJobRun,
)


BEIJING_TZ_NAME = "Asia/Shanghai"
BEIJING_TZ = ZoneInfo(BEIJING_TZ_NAME)
UTC = datetime.timezone.utc

DEFAULT_QUOTE_JOB_FAMILY = "quote_daily"
DEFAULT_QUOTE_JOB_NAME = "datahub_quote_daily"
DEFAULT_QUOTE_JOB_TRIGGER = "cron"
DEFAULT_QUOTE_JOB_SOURCE = "k8s-cronjob"
DEFAULT_QUOTE_CATCHUP_JOB_NAME = "datahub_quote_startup_catchup"
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


def latest_job_run(
    *,
    job_family: str,
    scheduled_at: datetime.datetime | None = None,
    statuses: list[str] | None = None,
) -> DatahubJobRun | None:
    query = DatahubJobRun.objects(job_family=job_family)
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
