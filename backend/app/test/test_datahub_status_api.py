import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock


def _build_stock(code):
    stock = MagicMock()
    stock.code = code
    return stock


def _build_asset_status(code, latest_data_date):
    status = MagicMock()
    status.code = code
    status.latest_data_date = latest_data_date
    return status


def _make_healthy_pipeline_status():
    """Return a mock pipeline_status dict with all jobs SUCCESS."""
    return {
        "jobs": {
            "quote_daily": {
                "label": "个股行情拉取",
                "status": "SUCCESS",
                "started_at": "2026-04-02T10:10:00",
                "completed_at": "2026-04-02T10:15:00",
                "error_message": None,
                "skipped_reason": None,
                "dependency_job_family": None,
                "pulled_total": 5000,
                "written_total": 5000,
                "failed_phase": None,
            },
            "signal_daily": {
                "label": "信号生成",
                "status": "SUCCESS",
                "started_at": "2026-04-02T10:30:00",
                "completed_at": "2026-04-02T10:32:00",
                "error_message": None,
                "skipped_reason": None,
                "dependency_job_family": None,
                "pulled_total": 5000,
                "written_total": 5000,
                "failed_phase": None,
            },
            "scoring_daily": {
                "label": "评分计算",
                "status": "SUCCESS",
                "started_at": "2026-04-02T10:35:00",
                "completed_at": "2026-04-02T10:45:00",
                "error_message": None,
                "skipped_reason": None,
                "dependency_job_family": None,
                "pulled_total": 5000,
                "written_total": 5000,
                "failed_phase": None,
            },
        },
        "overall_healthy": True,
        "summary": "ALL_JOBS_SUCCESS",
        "signal_run_today": True,
        "scoring_run_today": True,
    }


def test_get_datahub_status_classifies_asset_status(client, monkeypatch):
    from app.api.v1 import datahub_status

    current_dt = datetime.datetime(2026, 4, 3, 10, 0, 0)
    latest_complete = datetime.date(2026, 4, 2)
    previous_complete = datetime.date(2026, 4, 1)

    index_codes = ["sh000001", "sh000002", "sh000003", "sh000004"]
    stock_codes = ["sz000001", "sz000002", "sz000003", "sz000004"]

    monkeypatch.setattr(
        datahub_status,
        "_resolve_reference_dates",
        lambda current_dt=None: {
            "latest_complete_trading_day": latest_complete,
            "previous_complete_trading_day": previous_complete,
        },
    )
    stock_index_model = SimpleNamespace(name="stock_index_model")
    individual_stock_model = SimpleNamespace(name="individual_stock_model")
    monkeypatch.setattr(datahub_status, "StockIndex", stock_index_model)
    monkeypatch.setattr(datahub_status, "IndividualStock", individual_stock_model)
    monkeypatch.setattr(
        datahub_status,
        "_get_codes",
        lambda stock_model: (
            index_codes if stock_model is stock_index_model else stock_codes
        ),
    )
    monkeypatch.setattr(
        datahub_status,
        "_get_latest_asset_status",
        lambda object_type, **kwargs: (
            datetime.datetime(2026, 4, 2, 0, 0, 0),
            3,
        ),
    )
    monkeypatch.setattr(
        datahub_status,
        "_get_quote_status_data_count",
        lambda object_type, **kwargs: (
            len(index_codes) if object_type == "stock_index" else len(stock_codes)
        ),
    )

    index_asset_statuses = [
        _build_asset_status("sh000001", datetime.datetime(2026, 4, 2, 0, 0, 0)),
        _build_asset_status("sh000002", datetime.datetime(2026, 4, 1, 0, 0, 0)),
        _build_asset_status("sh000003", datetime.datetime(2026, 3, 31, 0, 0, 0)),
    ]
    stock_asset_statuses = [
        _build_asset_status("sz000001", datetime.datetime(2026, 4, 2, 0, 0, 0)),
        _build_asset_status("sz000002", datetime.datetime(2026, 4, 1, 0, 0, 0)),
        _build_asset_status("sz000003", datetime.datetime(2026, 3, 28, 0, 0, 0)),
    ]

    monkeypatch.setattr(
        datahub_status,
        "DataAssetStatus",
        SimpleNamespace(
            objects=lambda **kwargs: MagicMock(
                only=lambda *args, **kw: MagicMock(
                    filter=lambda **kw: (
                        index_asset_statuses
                        if kwargs["object_type"] == "stock_index"
                        else stock_asset_statuses
                    )
                )
            )
        ),
    )
    fixed_datetime = type(
        "FixedDatetime",
        (datetime.datetime,),
        {"now": classmethod(lambda cls, tz=None: current_dt)},
    )
    monkeypatch.setattr(datahub_status.datetime, "datetime", fixed_datetime)

    # Mock the new structured pipeline status
    monkeypatch.setattr(
        datahub_status,
        "_get_pipeline_status",
        lambda today_start: _make_healthy_pipeline_status(),
    )

    response = client.get("/api/datahub/status")

    assert response.status_code == 200
    payload = response.get_json()

    # ── reference dates (unchanged) ───────────────────────────────────
    assert payload["reference_dates"]["latest_complete_trading_day"] == "2026-04-02"
    assert payload["reference_dates"]["previous_complete_trading_day"] == "2026-04-01"

    # ── index category (unchanged) ────────────────────────────────────
    assert payload["index"]["total_count"] == 4
    assert payload["index"]["asset_status_records_count"] == 3
    assert payload["index"]["freshness_deprecated"] is True
    assert payload["index"]["up_to_date_count"] == 1
    assert payload["index"]["lag_1_day_count"] == 1
    assert payload["index"]["expired_count"] == 1
    assert payload["index"]["no_data_count"] == 1

    # ── stock category (unchanged) ────────────────────────────────────
    assert payload["stock"]["total_count"] == 4
    assert payload["stock"]["up_to_date_count"] == 1
    assert payload["stock"]["lag_1_day_count"] == 1
    assert payload["stock"]["expired_count"] == 1
    assert payload["stock"]["no_data_count"] == 1

    # ── legacy top-level booleans (backward compatible) ───────────────
    assert payload["signal_run_today"] is True
    assert payload["scoring_run_today"] is True

    # ── new pipeline section ──────────────────────────────────────────
    assert "pipeline" in payload
    pipe = payload["pipeline"]
    assert pipe["overall_healthy"] is True
    assert pipe["summary"] == "ALL_JOBS_SUCCESS"
    assert "quote_daily" in pipe["jobs"]
    assert "signal_daily" in pipe["jobs"]
    assert "scoring_daily" in pipe["jobs"]
    assert pipe["jobs"]["signal_daily"]["status"] == "SUCCESS"
    assert pipe["jobs"]["scoring_daily"]["status"] == "SUCCESS"
    assert pipe["signal_run_today"] is True  # nested inside pipeline too
    assert pipe["scoring_run_today"] is True

    # ── new freshness section ─────────────────────────────────────────
    assert "freshness" in payload
    fresh = payload["freshness"]
    # With all jobs healthy, 4 stocks total, 1 up-to-date → STALE
    assert fresh["grade"] == "STALE"
    assert "details" in fresh
    assert fresh["details"]["up_to_date_ratio"] == 0.25
    assert fresh["details"]["trading_days_behind"] == 0
    assert fresh["details"]["quote_date"] == "2026-04-02T00:00:00"


def test_get_datahub_status_failed_pipeline_returns_error_grade(client, monkeypatch):
    """When critical jobs fail, the freshness grade should be ERROR."""
    from app.api.v1 import datahub_status

    current_dt = datetime.datetime(2026, 4, 3, 10, 0, 0)
    latest_complete = datetime.date(2026, 4, 2)
    previous_complete = datetime.date(2026, 4, 1)
    stock_codes = ["sz000001", "sz000002", "sz000003", "sz000004"]
    index_codes = ["sh000001", "sh000002", "sh000003", "sh000004"]

    monkeypatch.setattr(
        datahub_status,
        "_resolve_reference_dates",
        lambda current_dt=None: {
            "latest_complete_trading_day": latest_complete,
            "previous_complete_trading_day": previous_complete,
        },
    )
    stock_index_model = SimpleNamespace(name="stock_index_model")
    individual_stock_model = SimpleNamespace(name="individual_stock_model")
    monkeypatch.setattr(datahub_status, "StockIndex", stock_index_model)
    monkeypatch.setattr(datahub_status, "IndividualStock", individual_stock_model)
    monkeypatch.setattr(
        datahub_status,
        "_get_codes",
        lambda stock_model: (
            index_codes if stock_model is stock_index_model else stock_codes
        ),
    )
    monkeypatch.setattr(
        datahub_status,
        "_get_latest_asset_status",
        lambda object_type, **kwargs: (
            datetime.datetime(2026, 4, 2, 0, 0, 0),
            3,
        ),
    )
    monkeypatch.setattr(
        datahub_status,
        "_get_quote_status_data_count",
        lambda object_type, **kwargs: 4,
    )

    stock_asset_statuses = [
        _build_asset_status("sz000001", datetime.datetime(2026, 4, 2, 0, 0, 0)),
        _build_asset_status("sz000002", datetime.datetime(2026, 4, 2, 0, 0, 0)),
        _build_asset_status("sz000003", datetime.datetime(2026, 4, 2, 0, 0, 0)),
    ]
    index_asset_statuses = []

    monkeypatch.setattr(
        datahub_status,
        "DataAssetStatus",
        SimpleNamespace(
            objects=lambda **kwargs: MagicMock(
                only=lambda *args, **kw: MagicMock(
                    filter=lambda **kw: (
                        index_asset_statuses
                        if kwargs["object_type"] == "stock_index"
                        else stock_asset_statuses
                    )
                )
            )
        ),
    )

    fixed_datetime = type(
        "FixedDatetime",
        (datetime.datetime,),
        {"now": classmethod(lambda cls, tz=None: current_dt)},
    )
    monkeypatch.setattr(datahub_status.datetime, "datetime", fixed_datetime)

    # Mock FAILED pipeline — quote failed, signal skipped, scoring skipped
    monkeypatch.setattr(
        datahub_status,
        "_get_pipeline_status",
        lambda today_start: {
            "jobs": {
                "quote_daily": {
                    "label": "个股行情拉取",
                    "status": "FAILED",
                    "started_at": "2026-04-02T10:10:00",
                    "completed_at": "2026-04-02T10:11:00",
                    "error_message": "Connection timeout",
                    "skipped_reason": None,
                    "dependency_job_family": None,
                    "pulled_total": 0,
                    "written_total": 0,
                    "failed_phase": "quote_pull",
                },
                "signal_daily": {
                    "label": "信号生成",
                    "status": "SKIPPED",
                    "started_at": "2026-04-02T10:30:00",
                    "completed_at": "2026-04-02T10:30:01",
                    "error_message": None,
                    "skipped_reason": "dependency_failed",
                    "dependency_job_family": "quote_daily",
                    "pulled_total": 0,
                    "written_total": 0,
                    "failed_phase": None,
                },
                "scoring_daily": {
                    "label": "评分计算",
                    "status": "SKIPPED",
                    "started_at": "2026-04-02T10:35:00",
                    "completed_at": "2026-04-02T10:35:01",
                    "error_message": None,
                    "skipped_reason": "dependency_failed",
                    "dependency_job_family": "signal_daily",
                    "pulled_total": 0,
                    "written_total": 0,
                    "failed_phase": None,
                },
            },
            "overall_healthy": False,
            "summary": "CRITICAL_FAILURE",
            "signal_run_today": False,
            "scoring_run_today": False,
        },
    )

    # Clear cache so the second test gets a fresh payload
    with datahub_status._status_cache_lock:
        datahub_status._status_cache = {"expires_at": 0, "payload": None}

    response = client.get("/api/datahub/status")

    assert response.status_code == 200
    payload = response.get_json()

    # Legacy booleans reflect failures
    assert payload["signal_run_today"] is False
    assert payload["scoring_run_today"] is False

    # Pipeline section details
    pipe = payload["pipeline"]
    assert pipe["overall_healthy"] is False
    assert pipe["summary"] == "CRITICAL_FAILURE"
    assert pipe["jobs"]["quote_daily"]["status"] == "FAILED"
    assert pipe["jobs"]["quote_daily"]["error_message"] == "Connection timeout"
    assert pipe["jobs"]["signal_daily"]["status"] == "SKIPPED"
    assert pipe["jobs"]["signal_daily"]["skipped_reason"] == "dependency_failed"
    assert pipe["jobs"]["scoring_daily"]["status"] == "SKIPPED"

    # Freshness grade should be ERROR
    assert payload["freshness"]["grade"] == "ERROR"
    assert "流水线失败" in payload["freshness"]["reason"]
