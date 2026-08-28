"""Tests for scoring_runner dependency gating."""


class _FakeRunRecord:
    """Minimal stand-in for a DatahubJobRun used by _check_dependency."""

    def __init__(self, status="SUCCESS", written_total=0):
        self.status = status
        self.written_total = written_total


def test_scoring_dependency_accepts_success_record(monkeypatch):
    import app.jobs.scoring_runner as scoring_runner

    def fake_latest_job_run(**kwargs):
        if kwargs.get("statuses") == ["SUCCESS"]:
            return _FakeRunRecord(status="SUCCESS")
        raise AssertionError("success path must not fall through to the record query")

    monkeypatch.setattr(
        "app.jobs.scoring_runner.job_run_helper.latest_job_run", fake_latest_job_run
    )

    assert scoring_runner._check_dependency() is True


def test_scoring_dependency_accepts_partially_written_failed_run(monkeypatch):
    import app.jobs.scoring_runner as scoring_runner

    records = [
        None,  # no SUCCESS record
        _FakeRunRecord(status="FAILED", written_total=5192),
    ]

    def fake_latest_job_run(**kwargs):
        if kwargs.get("statuses") == ["SUCCESS"]:
            return records[0]
        if kwargs.get("statuses") == ["RUNNING", "FAILED"]:
            return records[1]
        raise AssertionError("unexpected query")

    monkeypatch.setattr(
        "app.jobs.scoring_runner.job_run_helper.latest_job_run", fake_latest_job_run
    )

    # The signal run failed after persisting signals; scoring may still
    # proceed on the real data instead of being skipped.
    assert scoring_runner._check_dependency() is True


def test_scoring_dependency_accepts_running_upstream_with_written_evidence(
    monkeypatch,
):
    import app.jobs.scoring_runner as scoring_runner

    records = [
        None,
        # Still RUNNING but progress persistence already recorded real writes.
        _FakeRunRecord(status="RUNNING", written_total=5192),
    ]

    def fake_latest_job_run(**kwargs):
        if kwargs.get("statuses") == ["SUCCESS"]:
            return records[0]
        if kwargs.get("statuses") == ["RUNNING", "FAILED"]:
            return records[1]
        raise AssertionError("unexpected query")

    monkeypatch.setattr(
        "app.jobs.scoring_runner.job_run_helper.latest_job_run", fake_latest_job_run
    )

    assert scoring_runner._check_dependency() is True


def test_scoring_dependency_rejects_run_without_written_signals(monkeypatch):
    import app.jobs.scoring_runner as scoring_runner

    records = [
        None,
        _FakeRunRecord(status="RUNNING", written_total=0),
    ]

    def fake_latest_job_run(**kwargs):
        if kwargs.get("statuses") == ["SUCCESS"]:
            return records[0]
        if kwargs.get("statuses") == ["RUNNING", "FAILED"]:
            return records[1]
        raise AssertionError("unexpected query")

    monkeypatch.setattr(
        "app.jobs.scoring_runner.job_run_helper.latest_job_run", fake_latest_job_run
    )

    # A live signal run with no persisted writes must never be raced.
    assert scoring_runner._check_dependency() is False


def test_scoring_dependency_rejects_missing_record(monkeypatch):
    import app.jobs.scoring_runner as scoring_runner

    monkeypatch.setattr(
        "app.jobs.scoring_runner.job_run_helper.latest_job_run", lambda **kwargs: None
    )

    assert scoring_runner._check_dependency() is False
