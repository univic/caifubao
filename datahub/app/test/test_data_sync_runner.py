import datetime

import pytest


def test_data_sync_runner_without_command_prints_help(capsys):
    from app.jobs import data_sync_runner

    data_sync_runner.main([])

    assert "Data Sync Runner" in capsys.readouterr().out


def test_data_sync_runner_accepts_scheduled_metadata(monkeypatch):
    from app.jobs import data_sync_runner

    contexts = []
    scheduled_at = datetime.datetime(2026, 5, 19, 10, 30, tzinfo=datetime.UTC)

    monkeypatch.setattr(data_sync_runner, "_init_db_connection", lambda: None)
    monkeypatch.setattr(
        data_sync_runner.job_run_helper,
        "mark_stale_running_job_runs_failed",
        lambda **_: 0,
    )
    run_args = []
    monkeypatch.setattr(
        data_sync_runner,
        "run_sync",
        lambda args: (
            run_args.append(args)
            or {
                "status": "GOOD",
                "total_read": 0,
                "total_upserted": 0,
                "collections_synced": 0,
                "elapsed_seconds": 0,
            }
        ),
    )
    monkeypatch.setattr(
        data_sync_runner.job_run_helper,
        "create_job_run",
        lambda context: contexts.append(context) or object(),
    )
    monkeypatch.setattr(
        data_sync_runner.job_run_helper,
        "finish_job_run",
        lambda *_, **__: None,
    )
    monkeypatch.setattr(
        data_sync_runner.job_run_helper,
        "compute_daily_schedule_at",
        lambda hour, minute, timezone_name: scheduled_at,
    )

    data_sync_runner.main(
        [
            "run",
            "--trigger",
            "cron",
            "--source",
            "k8s-cronjob",
            "--scheduled-hour",
            "18",
            "--scheduled-minute",
            "30",
        ]
    )

    assert contexts[0].scheduled_at == scheduled_at
    assert run_args[0].full is False
    assert run_args[0].overlap_days == 3
    assert contexts[0].extra["full"] is False
    assert contexts[0].extra["overlap_days"] == 3


def test_sigterm_handler_marks_run_failed(monkeypatch):
    from app.jobs import data_sync_runner

    finished = []
    job_run = object()
    monkeypatch.setattr(
        data_sync_runner.job_run_helper,
        "finish_job_run",
        lambda *args, **kwargs: finished.append((args, kwargs)),
    )

    handler = data_sync_runner._make_termination_handler(job_run)

    with pytest.raises(SystemExit) as exc_info:
        handler(data_sync_runner.signal.SIGTERM, None)

    assert exc_info.value.code == 128 + data_sync_runner.signal.SIGTERM
    assert finished[0][0] == (job_run,)
    assert finished[0][1]["status"] == "FAILED"
    assert "SIGTERM" in finished[0][1]["error_message"]


def test_sigterm_handler_exits_when_status_persistence_fails(monkeypatch):
    from app.jobs import data_sync_runner

    monkeypatch.setattr(
        data_sync_runner.job_run_helper,
        "finish_job_run",
        lambda *_, **__: (_ for _ in ()).throw(RuntimeError("mongo unavailable")),
    )

    handler = data_sync_runner._make_termination_handler(object())

    with pytest.raises(SystemExit) as exc_info:
        handler(data_sync_runner.signal.SIGTERM, None)

    assert exc_info.value.code == 128 + data_sync_runner.signal.SIGTERM


def test_runner_reaps_stale_records_before_start(monkeypatch):
    from app.jobs import data_sync_runner

    reaped = []
    monkeypatch.setattr(data_sync_runner, "_init_db_connection", lambda: None)
    monkeypatch.setattr(
        data_sync_runner.job_run_helper,
        "mark_stale_running_job_runs_failed",
        lambda **kwargs: reaped.append(kwargs),
    )
    monkeypatch.setattr(
        data_sync_runner.job_run_helper,
        "create_job_run",
        lambda context: object(),
    )
    monkeypatch.setattr(
        data_sync_runner.job_run_helper,
        "finish_job_run",
        lambda *_, **__: None,
    )
    monkeypatch.setattr(
        data_sync_runner,
        "run_sync",
        lambda args: {
            "status": "GOOD",
            "total_read": 0,
            "total_upserted": 0,
            "collections_synced": 0,
            "elapsed_seconds": 0,
        },
    )

    data_sync_runner.main(["run"])

    assert reaped == [{"job_family": "data_sync"}]


def test_full_sync_uses_extended_stale_cleanup_window(monkeypatch):
    from app.jobs import data_sync_runner

    cleanup_calls = []
    monkeypatch.setattr(data_sync_runner, "_init_db_connection", lambda: None)
    monkeypatch.setattr(
        data_sync_runner.job_run_helper,
        "mark_stale_running_job_runs_failed",
        lambda **kwargs: cleanup_calls.append(kwargs),
    )
    monkeypatch.setattr(
        data_sync_runner.job_run_helper,
        "create_job_run",
        lambda context: object(),
    )
    monkeypatch.setattr(
        data_sync_runner.job_run_helper,
        "finish_job_run",
        lambda *_, **__: None,
    )
    monkeypatch.setattr(
        data_sync_runner,
        "run_sync",
        lambda args: {
            "status": "GOOD",
            "total_read": 0,
            "total_upserted": 0,
            "collections_synced": 0,
            "elapsed_seconds": 0,
        },
    )

    data_sync_runner.main(["run", "--full"])

    assert cleanup_calls == [{"job_family": "data_sync_full", "max_age_minutes": 1440}]
