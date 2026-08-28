import datetime


def test_run_startup_quote_catchup_skips_when_not_needed(monkeypatch):
    from app.jobs.quote_catchup import run_startup_quote_catchup

    monkeypatch.setattr(
        "app.jobs.quote_catchup.should_run_startup_quote_catchup",
        lambda: (False, {"reason": "already_current"}),
    )

    result = run_startup_quote_catchup()

    assert result["status"] == "SKIPPED"
    assert result["reason"] == "already_current"


def test_run_startup_quote_catchup_invokes_quote_job(monkeypatch):
    from app.jobs.quote_catchup import run_startup_quote_catchup

    captured = {}

    def fake_should_run():
        return True, {
            "scheduled_at": datetime.datetime(2026, 4, 14, 10, 10),
            "latest_trading_day": datetime.datetime(2026, 4, 14),
        }

    def fake_run_quote_job(target, *, job_metadata=None, **kwargs):
        captured["target"] = target
        captured["job_metadata"] = job_metadata
        return {"status": "SUCCESS", "target": target}

    monkeypatch.setattr(
        "app.jobs.quote_catchup.should_run_startup_quote_catchup",
        fake_should_run,
    )
    monkeypatch.setattr("app.jobs.quote_catchup.run_quote_job", fake_run_quote_job)

    result = run_startup_quote_catchup()

    assert captured["target"] == "all"
    assert captured["job_metadata"].job_family == "quote_daily"
    assert captured["job_metadata"].trigger == "startup"
    assert result["status"] == "SUCCESS"


def test_run_startup_quote_catchup_skips_when_claim_exists(monkeypatch):
    import app.lib.utilities.job_run_helper as job_run_helper
    from app.jobs.quote_catchup import run_startup_quote_catchup

    def fake_should_run():
        return True, {
            "scheduled_at": datetime.datetime(2026, 4, 14, 10, 10),
            "latest_trading_day": datetime.datetime(2026, 4, 14),
        }

    def claim_conflict(target, *, job_metadata=None, **kwargs):
        raise job_run_helper.JobRunClaimExistsError(
            "an active RUNNING job run already holds the claim"
        )

    monkeypatch.setattr(
        "app.jobs.quote_catchup.should_run_startup_quote_catchup",
        fake_should_run,
    )
    monkeypatch.setattr("app.jobs.quote_catchup.run_quote_job", claim_conflict)

    result = run_startup_quote_catchup()

    assert result["status"] == "SKIPPED"
    assert result["reason"] == "already_claimed_by_active_run"
