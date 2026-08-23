import datetime


def test_data_sync_runner_accepts_scheduled_metadata(monkeypatch):
    from app.jobs import data_sync_runner

    contexts = []
    scheduled_at = datetime.datetime(2026, 5, 19, 10, 30)

    monkeypatch.setattr(data_sync_runner, "_init_db_connection", lambda: None)
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
