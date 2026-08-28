from app.jobs.quote_runner import (
    TARGET_ALL,
    TARGET_INDEX,
    TARGET_STOCK,
    QuoteJobMetadata,
    parse_args,
    run_quote_job,
)


def _summary(job_name, written_total):
    return {
        "job_name": job_name,
        "status": "SUCCESS",
        "failed_phase": None,
        "pulled_total": written_total + 1,
        "written_total": written_total,
        "validated_total": written_total + 2,
        "as_of_date": "2026-08-21T00:00:00",
        "phase_stats": {"phase": {"written_count": written_total}},
    }


class FakeDatahub:
    def __init__(self):
        self.calls = []

    def start_index_job(self):
        self.calls.append("index")
        return _summary("index_market_sync", 10)

    def start_stock_quote_job(self):
        self.calls.append("stock_quote")
        return _summary("stock_quote_sync", 20)

    def start_stock_job(self):
        self.calls.append("stock_with_factors")
        return _summary("stock_market_sync", 30)


def test_parse_args_defaults_to_stock_quote_only():
    args = parse_args([])

    assert args.target == TARGET_STOCK
    assert args.include_factors is False


def test_parse_args_accepts_frozen_as_of_date():
    args = parse_args(["--as-of-date", "2026-08-21"])

    assert args.as_of_date == "2026-08-21"


def test_run_index_quote_job():
    datahub = FakeDatahub()

    result = run_quote_job(TARGET_INDEX, datahub_factory=lambda: datahub)

    assert datahub.calls == ["index"]
    assert result["target"] == TARGET_INDEX
    assert result["written_total"] == 10


def test_run_stock_quote_job_excludes_factor_phases_by_default():
    datahub = FakeDatahub()

    result = run_quote_job(TARGET_STOCK, datahub_factory=lambda: datahub)

    assert datahub.calls == ["stock_quote"]
    assert result["target"] == TARGET_STOCK
    assert result["include_factors"] is False
    assert result["written_total"] == 20
    assert result["validated_total"] == 22
    assert result["as_of_date"] == "2026-08-21T00:00:00"


def test_run_stock_job_can_include_factor_phases():
    datahub = FakeDatahub()

    result = run_quote_job(
        TARGET_STOCK,
        include_factors=True,
        datahub_factory=lambda: datahub,
    )

    assert datahub.calls == ["stock_with_factors"]
    assert result["include_factors"] is True
    assert result["written_total"] == 30


def test_run_all_combines_index_and_stock_quote_jobs():
    datahub = FakeDatahub()

    result = run_quote_job(TARGET_ALL, datahub_factory=lambda: datahub)

    assert datahub.calls == ["index", "stock_quote"]
    assert result["target"] == TARGET_ALL
    assert result["status"] == "SUCCESS"
    assert result["written_total"] == 30
    assert result["validated_total"] == 34
    assert result["as_of_date"] == "2026-08-21T00:00:00"
    assert len(result["results"]) == 2


def test_run_all_failure_records_completed_index_and_partial_stock(monkeypatch):
    captured = {}

    class FailingDatahub(FakeDatahub):
        def __init__(self):
            super().__init__()
            self.last_job_summary = None

        def start_index_job(self):
            result = super().start_index_job()
            self.last_job_summary = result
            return result

        def start_stock_quote_job(self):
            self.calls.append("stock_quote")
            self.last_job_summary = {
                **_summary("stock_quote_sync", 4),
                "status": "FAILED",
                "failed_phase": "quotes",
            }
            raise RuntimeError("stock failed")

    monkeypatch.setattr(
        "app.jobs.quote_runner.job_run_helper.create_job_run", lambda context: object()
    )
    monkeypatch.setattr(
        "app.jobs.quote_runner.job_run_helper.finish_job_run",
        lambda job_run, **kwargs: captured.update(kwargs),
    )
    datahub = FailingDatahub()

    with __import__("pytest").raises(RuntimeError, match="stock failed"):
        run_quote_job(
            TARGET_ALL,
            datahub_factory=lambda: datahub,
            job_metadata=QuoteJobMetadata(
                job_name="datahub_quote_daily",
                job_family="quote_daily",
                trigger="manual",
                source="test",
            ),
        )

    assert captured["status"] == "FAILED"
    assert captured["summary"]["written_total"] == 14
    assert captured["summary"]["results"][0]["target"] == TARGET_INDEX
    assert captured["summary"]["results"][1]["target"] == TARGET_STOCK


def test_run_all_construction_failure_keeps_index_as_of_date(monkeypatch):
    captured = {}

    class ConstructionFailingDatahub(FakeDatahub):
        def __init__(self):
            super().__init__()
            self.last_job_summary = None

        def start_stock_quote_job(self):
            self.calls.append("stock_quote")
            self.last_job_summary = None
            raise RuntimeError("construction failed")

    monkeypatch.setattr(
        "app.jobs.quote_runner.job_run_helper.create_job_run", lambda context: object()
    )
    monkeypatch.setattr(
        "app.jobs.quote_runner.job_run_helper.finish_job_run",
        lambda job_run, **kwargs: captured.update(kwargs),
    )

    with __import__("pytest").raises(RuntimeError, match="construction failed"):
        run_quote_job(
            TARGET_ALL,
            datahub_factory=ConstructionFailingDatahub,
            job_metadata=QuoteJobMetadata(
                job_name="datahub_quote_daily",
                job_family="quote_daily",
                trigger="manual",
                source="test",
            ),
        )

    assert captured["summary"]["as_of_date"] == "2026-08-21T00:00:00"
    assert captured["summary"]["written_total"] == 10


def test_datahub_reuses_one_start_time_for_all_processors():
    from app.lib.datahub import Datahub

    captured = []

    class FakeProcessor:
        def __init__(self, **kwargs):
            captured.append(kwargs)

    datahub = Datahub()
    datahub.processor_registry = {
        "ChinaAStock_daily": {"processor_object": FakeProcessor}
    }

    datahub._build_processor()
    datahub._build_processor()

    assert captured[0]["run_started_at"] == captured[1]["run_started_at"]


def test_datahub_clears_previous_summary_before_processor_construction():
    from app.lib.datahub import Datahub

    class BrokenProcessor:
        def __init__(self, **kwargs):
            raise RuntimeError("construction failed")

    datahub = Datahub()
    datahub.last_job_summary = _summary("index_market_sync", 10)
    datahub.processor_registry = {
        "ChinaAStock_daily": {"processor_object": BrokenProcessor}
    }

    with __import__("pytest").raises(RuntimeError, match="construction failed"):
        datahub.start_stock_quote_job()

    assert datahub.last_job_summary is None


def test_run_quote_job_propagates_claim_error_without_finishing(monkeypatch):
    import pytest

    from app.lib.utilities.job_run_helper import (
        JobRunClaimExistsError,
        JobRunContext,
    )

    datahub = FakeDatahub()
    finished = []

    def claiming_create_job_run(context):
        assert isinstance(context, JobRunContext)
        raise JobRunClaimExistsError("claim held by an active RUNNING run")

    def unexpected_finish(job_run, **kwargs):
        finished.append(job_run)

    monkeypatch.setattr(
        "app.jobs.quote_runner.job_run_helper.create_job_run",
        claiming_create_job_run,
    )
    monkeypatch.setattr(
        "app.jobs.quote_runner.job_run_helper.finish_job_run",
        unexpected_finish,
    )

    metadata = QuoteJobMetadata(
        job_name="datahub_quote_stock_daily",
        job_family="quote_daily",
        trigger="cron",
        source="k8s-cronjob",
    )

    # The claim is lost before the try block in run_quote_job, so the error
    # must propagate untouched: no job work, no finish_job_run overwrite.
    with pytest.raises(JobRunClaimExistsError):
        run_quote_job(
            TARGET_STOCK,
            datahub_factory=lambda: datahub,
            job_metadata=metadata,
        )

    assert datahub.calls == []
    assert finished == []


def test_reap_stale_running_job_runs_is_best_effort(monkeypatch):
    from app.jobs import quote_runner

    calls = []

    class FakeWatcher:
        @staticmethod
        def get_db_connection():
            calls.append("connect")

    monkeypatch.setattr(
        "app.lib.db_watcher.mongoengine_tool.mongo_watcher", FakeWatcher
    )
    monkeypatch.setattr(
        "app.lib.utilities.job_run_helper.mark_stale_running_job_runs_failed",
        lambda: calls.append("reap"),
    )

    quote_runner._reap_stale_running_job_runs()
    assert calls == ["connect", "reap"]

    def broken_reap():
        raise RuntimeError("index ensure blew up")

    monkeypatch.setattr(
        "app.lib.utilities.job_run_helper.mark_stale_running_job_runs_failed",
        broken_reap,
    )
    # A failed cleanup must never block the job.
    quote_runner._reap_stale_running_job_runs()


def test_run_quote_job_records_job_run(monkeypatch):
    from app.lib.utilities.job_run_helper import JobRunContext

    datahub = FakeDatahub()
    captured = {}

    class FakeJobRun:
        pass

    def fake_create_job_run(context):
        captured["start"] = context
        return FakeJobRun()

    def fake_finish_job_run(job_run, *, status, summary, error_message=None):
        captured["finish"] = {
            "status": status,
            "summary": summary,
            "error_message": error_message,
        }
        return job_run

    monkeypatch.setattr(
        "app.jobs.quote_runner.job_run_helper.create_job_run",
        fake_create_job_run,
    )
    monkeypatch.setattr(
        "app.jobs.quote_runner.job_run_helper.finish_job_run",
        fake_finish_job_run,
    )

    result = run_quote_job(
        TARGET_STOCK,
        datahub_factory=lambda: datahub,
        job_metadata=QuoteJobMetadata(
            job_name="datahub_quote_daily",
            job_family="quote_daily",
            trigger="cron",
            source="k8s-cronjob",
            scheduled_at=None,
        ),
    )

    assert isinstance(captured["start"], JobRunContext)
    assert captured["start"].job_name == "datahub_quote_daily"
    assert captured["finish"]["status"] == "SUCCESS"
    assert result["target"] == TARGET_STOCK
