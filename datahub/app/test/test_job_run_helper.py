import datetime
from zoneinfo import ZoneInfo

import pytest
from pymongo.errors import DuplicateKeyError


def test_compute_daily_schedule_at_normalizes_to_utc():
    from app.lib.utilities.job_run_helper import compute_daily_schedule_at

    scheduled = compute_daily_schedule_at(
        18,
        10,
        reference_time=datetime.datetime(
            2026, 4, 14, 18, 20, tzinfo=ZoneInfo("Asia/Shanghai")
        ),
    )

    assert scheduled == datetime.datetime(2026, 4, 14, 10, 10)


class _DuplicateSaveDocument:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def save(self):
        raise DuplicateKeyError("E11000 duplicate key error")


def test_create_job_run_translates_duplicate_claim(monkeypatch):
    import app.lib.utilities.job_run_helper as job_run_helper
    from app.lib.utilities.job_run_helper import JobRunContext

    monkeypatch.setattr(job_run_helper, "DatahubJobRun", _DuplicateSaveDocument)
    context = JobRunContext(
        job_name="datahub_quote_startup_catchup",
        job_family="quote_daily",
        trigger="startup",
        source="datahub-service",
        scheduled_at=datetime.datetime(2026, 4, 14, 10, 10),
    )

    with pytest.raises(job_run_helper.JobRunClaimExistsError) as excinfo:
        job_run_helper.create_job_run(context)

    message = str(excinfo.value)
    assert "quote_daily" in message
    assert "datahub_quote_startup_catchup" in message
    assert "2026-04-14 10:10" in message


class _RecordingQuerySet:
    def __init__(self, store):
        self.store = store

    def update(self, **updates):
        self.store["updates"] = updates
        return self.store.get("update_result", 0)


class _FakeJobRunManager:
    store: dict = {}

    @classmethod
    def objects(cls, **filters):
        cls.store["filters"] = filters
        return _RecordingQuerySet(cls.store)


def test_mark_stale_running_job_runs_failed_reaps_only_stale_running(
    monkeypatch,
):
    import app.lib.utilities.job_run_helper as job_run_helper

    store = {"update_result": 2}
    monkeypatch.setattr(job_run_helper, "DatahubJobRun", _FakeJobRunManager)
    _FakeJobRunManager.store = store

    now = datetime.datetime(2026, 4, 14, 12, 0)
    updated = job_run_helper.mark_stale_running_job_runs_failed(now=now)

    assert updated == 2
    filters = store["filters"]
    assert filters["status"] == "RUNNING"
    assert filters["started_at__lt"] == datetime.datetime(2026, 4, 14, 9, 0)
    updates = store["updates"]
    assert updates["set__status"] == "FAILED"
    assert updates["set__completed_at"] is not None
    assert "180 minutes" in updates["set__error_message"]
    assert "startup cleanup" in updates["set__error_message"]


def test_mark_stale_running_job_runs_failed_respects_custom_window(
    monkeypatch,
):
    import app.lib.utilities.job_run_helper as job_run_helper

    store = {"update_result": 0}
    monkeypatch.setattr(job_run_helper, "DatahubJobRun", _FakeJobRunManager)
    _FakeJobRunManager.store = store

    now = datetime.datetime(2026, 4, 14, 12, 0)
    updated = job_run_helper.mark_stale_running_job_runs_failed(
        max_age_minutes=30, now=now
    )

    assert updated == 0
    assert store["filters"]["started_at__lt"] == datetime.datetime(2026, 4, 14, 11, 30)
    assert "30 minutes" in store["updates"]["set__error_message"]


def test_compute_daily_schedule_at_uses_same_calendar_day():
    from app.lib.utilities.job_run_helper import compute_daily_schedule_at

    scheduled = compute_daily_schedule_at(
        18,
        10,
        reference_time=datetime.datetime(
            2026, 4, 14, 0, 5, tzinfo=ZoneInfo("Asia/Shanghai")
        ),
    )

    assert scheduled == datetime.datetime(2026, 4, 14, 10, 10)
