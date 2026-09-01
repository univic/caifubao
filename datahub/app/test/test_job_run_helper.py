import datetime
from zoneinfo import ZoneInfo

import pytest
from mongoengine.errors import NotUniqueError
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
    def __init__(self, error):
        self.error = error

    def save(self):
        raise self.error


@pytest.mark.parametrize(
    "duplicate_error",
    [
        DuplicateKeyError("E11000 duplicate key error"),
        NotUniqueError("Not unique error"),
    ],
    ids=["pymongo", "mongoengine"],
)
def test_create_job_run_translates_duplicate_claim(monkeypatch, duplicate_error):
    import app.lib.utilities.job_run_helper as job_run_helper
    from app.lib.utilities.job_run_helper import JobRunContext

    monkeypatch.setattr(
        job_run_helper,
        "DatahubJobRun",
        lambda **kwargs: _DuplicateSaveDocument(duplicate_error),
    )
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
    def __init__(self, store, filters):
        self.store = store
        self.filters = filters

    def update(self, **updates):
        self.store.setdefault("groups", []).append(
            {"filters": self.filters, "updates": updates}
        )
        results = self.store.setdefault("update_results", [])
        return results.pop(0) if results else 0


class _FakeJobRunManager:
    """Captures every objects(filters).update(updates) group the reaper runs."""

    store: dict = {}

    @classmethod
    def objects(cls, **filters):
        return _RecordingQuerySet(cls.store, filters)


def test_mark_stale_running_job_runs_failed_reaps_only_stale_running(
    monkeypatch,
):
    import app.lib.utilities.job_run_helper as job_run_helper

    # Each reaped group runs two updates: records that wrote data first
    # (written_total > 0), then records without writes.
    store = {"update_results": [2, 0, 0, 0]}
    monkeypatch.setattr(job_run_helper, "DatahubJobRun", _FakeJobRunManager)
    _FakeJobRunManager.store = store

    now = datetime.datetime(2026, 4, 14, 12, 0)
    updated = job_run_helper.mark_stale_running_job_runs_failed(now=now)

    assert updated == 2
    # Default group: every job name except the unbounded catch-up, reaped
    # after 4 hours (must exceed the largest activeDeadlineSeconds, 3h).
    written_group = store["groups"][0]
    assert written_group["filters"] == {
        "status": "RUNNING",
        "started_at__lt": datetime.datetime(2026, 4, 14, 8, 0),
        "job_name__nin": ["datahub_quote_startup_catchup"],
        "written_total__gt": 0,
    }
    updates = written_group["updates"]
    assert updates["set__status"] == "FAILED"
    assert updates["set__completed_at"] is not None
    assert "240 minutes" in updates["set__error_message"]
    assert "startup cleanup" in updates["set__error_message"]
    assert "data was already written" in updates["set__error_message"]

    empty_group = store["groups"][1]
    assert empty_group["filters"] == {
        "status": "RUNNING",
        "started_at__lt": datetime.datetime(2026, 4, 14, 8, 0),
        "job_name__nin": ["datahub_quote_startup_catchup"],
    }
    assert "no data was written" in empty_group["updates"]["set__error_message"]


def test_mark_stale_running_job_runs_failed_gives_catchup_a_wider_window(
    monkeypatch,
):
    import app.lib.utilities.job_run_helper as job_run_helper

    store = {"update_results": [0, 0, 0, 1]}
    monkeypatch.setattr(job_run_helper, "DatahubJobRun", _FakeJobRunManager)
    _FakeJobRunManager.store = store

    now = datetime.datetime(2026, 4, 14, 12, 0)
    updated = job_run_helper.mark_stale_running_job_runs_failed(now=now)

    assert updated == 1
    # Second group: the catch-up is only reaped once it cannot be a live
    # unbounded deployment run (24h). Its written-first update is groups[2].
    catchup_group = store["groups"][2]
    assert catchup_group["filters"] == {
        "status": "RUNNING",
        "started_at__lt": datetime.datetime(2026, 4, 13, 12, 0),
        "job_name": "datahub_quote_startup_catchup",
        "written_total__gt": 0,
    }
    assert "1440 minutes" in catchup_group["updates"]["set__error_message"]


def test_mark_stale_running_job_runs_failed_respects_custom_window(
    monkeypatch,
):
    import app.lib.utilities.job_run_helper as job_run_helper

    store = {"update_results": [0, 0, 0, 0]}
    monkeypatch.setattr(job_run_helper, "DatahubJobRun", _FakeJobRunManager)
    _FakeJobRunManager.store = store

    now = datetime.datetime(2026, 4, 14, 12, 0)
    updated = job_run_helper.mark_stale_running_job_runs_failed(
        max_age_minutes=30, now=now
    )

    assert updated == 0
    written_group = store["groups"][0]
    assert written_group["filters"]["started_at__lt"] == datetime.datetime(
        2026, 4, 14, 11, 30
    )
    assert "30 minutes" in written_group["updates"]["set__error_message"]
    # The catch-up keeps its own 24h window regardless of the custom default.
    assert "1440 minutes" in store["groups"][2]["updates"]["set__error_message"]


def test_mark_stale_running_job_runs_failed_can_scope_to_family(monkeypatch):
    import app.lib.utilities.job_run_helper as job_run_helper

    store = {"update_results": [0, 0, 0, 0]}
    monkeypatch.setattr(job_run_helper, "DatahubJobRun", _FakeJobRunManager)
    _FakeJobRunManager.store = store

    job_run_helper.mark_stale_running_job_runs_failed(
        now=datetime.datetime(2026, 4, 14, 12, 0),
        job_family="data_sync_daily",
    )

    assert all(
        group["filters"]["job_family"] == "data_sync_daily" for group in store["groups"]
    )


class _FlakySaveDocument:
    """First save hits a failed index ensure; the retry succeeds."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.saved = 0

    def save(self):
        self.saved += 1
        if self.saved == 1:
            raise __import__("pymongo").errors.OperationFailure("IndexKeySpecsConflict")


def test_create_job_run_retries_once_after_failed_index_ensure(monkeypatch):
    import app.lib.utilities.job_run_helper as job_run_helper
    from app.lib.utilities.job_run_helper import JobRunContext

    monkeypatch.setattr(job_run_helper, "DatahubJobRun", _FlakySaveDocument)
    context = JobRunContext(
        job_name="datahub_signal_daily",
        job_family="signal_daily",
        trigger="cron",
        source="k8s-cronjob",
    )

    job_run = job_run_helper.create_job_run(context)

    assert job_run.saved == 2


def test_datahub_job_run_index_specs_are_distinct():
    from app.model.datahub_job_run import DatahubJobRun

    indexes = DatahubJobRun._meta["indexes"]
    partial = next(
        spec
        for spec in indexes
        if isinstance(spec, dict) and "partialFilterExpression" in spec
    )
    # Explicit distinct name: without it pymongo would derive the same
    # auto-generated name as the plain key-pattern index and MongoDB rejects
    # creation with IndexKeySpecsConflict (code 86).
    assert partial["name"] != "job_family_1_scheduled_at_1"
    assert partial["unique"] is True
    assert partial["fields"] == ["job_family", "scheduled_at"]
    assert partial["partialFilterExpression"] == {
        "status": "RUNNING",
        "job_name": "datahub_quote_startup_catchup",
    }
    plain = [spec for spec in indexes if spec == ("job_family", "scheduled_at")]
    assert plain, "plain lookup index must remain for non-catchup queries"


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


class _RecordingJobRun:
    """Fake DatahubJobRun capturing progress updates."""

    def __init__(self, job_name="datahub_quote_stock_daily", fail_update=False):
        self.job_name = job_name
        self.fail_update = fail_update
        self.updates = []
        self.reloaded = 0

    def update(self, **updates):
        if self.fail_update:
            raise RuntimeError("mongo gone")
        self.updates.append(updates)
        return self

    def reload(self):
        self.reloaded += 1
        return self


def test_update_job_run_progress_persists_partial_state():
    import app.lib.utilities.job_run_helper as job_run_helper

    job_run = _RecordingJobRun()
    phase_stats = {
        "check_stock_data_integrity": {"written_count": 5209, "validated_count": 5209}
    }

    result = job_run_helper.update_job_run_progress(
        job_run,
        pulled_total=5550,
        written_total=5209,
        phase_stats=phase_stats,
    )

    assert result is job_run
    assert job_run.updates[-1] == {
        "set__failed_phase": None,
        "set__pulled_total": 5550,
        "set__written_total": 5209,
        "set__phase_stats": phase_stats,
    }
    assert job_run.reloaded == 1


def test_update_job_run_progress_never_raises_on_failure():
    import app.lib.utilities.job_run_helper as job_run_helper

    job_run = _RecordingJobRun(fail_update=True)

    # Progress persistence is best-effort: a DB failure must not break the
    # data job itself.
    result = job_run_helper.update_job_run_progress(
        job_run,
        pulled_total=1,
        written_total=2,
        phase_stats={},
    )

    assert result is job_run
    assert job_run.reloaded == 0
