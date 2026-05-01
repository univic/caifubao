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
    assert len(result["results"]) == 2


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
