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


def test_scoring_dependency_rechecks_success_during_status_transition(monkeypatch):
    import app.jobs.scoring_runner as scoring_runner

    records = iter([None, None, _FakeRunRecord(status="SUCCESS")])

    monkeypatch.setattr(
        "app.jobs.scoring_runner.job_run_helper.latest_job_run",
        lambda **kwargs: next(records),
    )

    assert scoring_runner._check_dependency() is True


def test_scoring_dependency_waits_for_running_upstream_then_succeeds(monkeypatch):
    import app.jobs.scoring_runner as scoring_runner

    records = iter(
        [
            None,
            _FakeRunRecord(status="RUNNING", written_total=0),
            _FakeRunRecord(status="SUCCESS"),
        ]
    )
    sleeps = []

    monkeypatch.setattr(
        "app.jobs.scoring_runner.job_run_helper.latest_job_run",
        lambda **kwargs: next(records),
    )
    monkeypatch.setattr(scoring_runner.time, "monotonic", lambda: 0)
    monkeypatch.setattr(scoring_runner.time, "sleep", sleeps.append)

    assert (
        scoring_runner._wait_for_dependency(
            timeout_seconds=60,
            poll_interval_seconds=5,
        )
        is True
    )
    assert sleeps == [5]


def test_scoring_dependency_wait_is_bounded(monkeypatch):
    import app.jobs.scoring_runner as scoring_runner

    records = iter(
        [
            None,
            _FakeRunRecord(status="RUNNING", written_total=0),
        ]
    )
    clock = iter([0, 60])
    sleeps = []

    monkeypatch.setattr(
        "app.jobs.scoring_runner.job_run_helper.latest_job_run",
        lambda **kwargs: next(records),
    )
    monkeypatch.setattr(scoring_runner.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(scoring_runner.time, "sleep", sleeps.append)

    assert (
        scoring_runner._wait_for_dependency(
            timeout_seconds=60,
            poll_interval_seconds=5,
        )
        is False
    )
    assert sleeps == []


def test_main_compare_parser_wires_arguments(monkeypatch):
    """compare subcommand must require both versions + window + horizon and
    pass them straight to run_compare (task 3.3 operator tool)."""
    import app.jobs.scoring_runner as scoring_runner

    captured = {}

    def fake_run_compare(args):
        captured.update(
            candidate=args.candidate_model_version,
            baseline=args.baseline_model_version,
            from_date=args.from_date,
            to_date=args.to_date,
            horizon=args.horizon,
            fmt=args.format,
        )

    monkeypatch.setattr(scoring_runner, "run_compare", fake_run_compare)
    scoring_runner.main(
        [
            "compare",
            "--candidate-model-version",
            "flip_wide_v1",
            "--baseline-model-version",
            "score_v2_202605b",
            "--from",
            "2026-01-01",
            "--to",
            "2026-06-30",
            "--horizon",
            "20",
            "--format",
            "json",
        ]
    )
    assert captured == {
        "candidate": "flip_wide_v1",
        "baseline": "score_v2_202605b",
        "from_date": "2026-01-01",
        "to_date": "2026-06-30",
        "horizon": 20,
        "fmt": "json",
    }


def test_main_compare_requires_both_versions(monkeypatch):
    import pytest

    import app.jobs.scoring_runner as scoring_runner

    monkeypatch.setattr(
        scoring_runner,
        "run_compare",
        lambda args: (_ for _ in ()).throw(AssertionError("must not run")),
    )
    with pytest.raises(SystemExit) as excinfo:
        scoring_runner.main(
            [
                "compare",
                "--candidate-model-version",
                "flip_wide_v1",
                "--from",
                "2026-01-01",
                "--to",
                "2026-06-30",
                "--horizon",
                "20",
            ]
        )
    assert excinfo.value.code == 2  # argparse usage error
