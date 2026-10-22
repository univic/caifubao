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

    def fake_run_quote_job(
        target, *, include_factors=False, job_metadata=None, **kwargs
    ):
        captured["target"] = target
        captured["include_factors"] = include_factors
        captured["job_metadata"] = job_metadata
        return {"status": "SUCCESS", "target": target}

    monkeypatch.setattr(
        "app.jobs.quote_catchup.should_run_startup_quote_catchup",
        fake_should_run,
    )
    monkeypatch.setattr("app.jobs.quote_catchup.run_quote_job", fake_run_quote_job)

    result = run_startup_quote_catchup()

    assert captured["target"] == "all"
    # The catch-up must run the factor phases too: the downstream signal gate
    # requires MA-factor evidence, and a quote-only catch-up leaves the day
    # looking missing even though quotes were repaired.
    assert captured["include_factors"] is True
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


def test_should_run_records_the_canonical_quote_slot(monkeypatch):
    """Catch-up must claim the same daily slot the signal gate looks for."""
    import app.jobs.quote_catchup as quote_catchup
    from app.lib.utilities import job_run_helper

    monkeypatch.setattr(quote_catchup, "_is_enabled", lambda: True)
    monkeypatch.setattr(quote_catchup.mongo_watcher, "get_db_connection", lambda: None)
    monkeypatch.setattr(
        quote_catchup,
        "get_a_stock_market_trade_calendar",
        lambda: [datetime.datetime(2026, 4, 14)],
    )
    monkeypatch.setattr(quote_catchup, "is_trading_day", lambda calendar, now: True)
    monkeypatch.setattr(
        quote_catchup,
        "determine_most_recent_previous_complete_trading_day",
        lambda calendar, now: datetime.datetime(2026, 4, 14),
    )
    monkeypatch.setattr(quote_catchup, "_quote_status_is_lagging", lambda day: True)
    monkeypatch.setattr(quote_catchup, "_has_active_quote_job", lambda at: False)

    now = job_run_helper.beijing_now().replace(
        year=2026, month=4, day=14, hour=18, minute=30, second=0, microsecond=0
    )
    should_run, context = quote_catchup.should_run_startup_quote_catchup(now)

    assert should_run is True
    assert context["scheduled_at"] == job_run_helper.compute_daily_schedule_at(
        quote_catchup.DEFAULT_CATCHUP_SCHEDULED_HOUR,
        quote_catchup.DEFAULT_CATCHUP_SCHEDULED_MINUTE,
        reference_time=now,
    )
    assert (
        quote_catchup.DEFAULT_CATCHUP_SCHEDULED_HOUR,
        quote_catchup.DEFAULT_CATCHUP_SCHEDULED_MINUTE,
    ) == (18, 0)


def test_should_not_run_before_the_cron_trigger_window(monkeypatch):
    import app.jobs.quote_catchup as quote_catchup

    monkeypatch.setattr(quote_catchup, "_is_enabled", lambda: True)
    monkeypatch.setattr(quote_catchup.mongo_watcher, "get_db_connection", lambda: None)
    monkeypatch.setattr(
        quote_catchup,
        "get_a_stock_market_trade_calendar",
        lambda: [datetime.datetime(2026, 4, 14)],
    )
    monkeypatch.setattr(quote_catchup, "is_trading_day", lambda calendar, now: True)

    now = datetime.datetime(2026, 4, 14, 18, 5)
    should_run, context = quote_catchup.should_run_startup_quote_catchup(now)

    assert should_run is False
    assert context["reason"] == "before_window"
