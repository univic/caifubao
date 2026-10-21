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

    monkeypatch.setattr(
        datahub_status,
        "_check_pipeline_run_today",
        lambda today_start: {
            "signal_run_today": True,
            "scoring_run_today": True,
        },
    )
    # The job-run lookup degrades to None when the collection is unavailable;
    # the status endpoint's existing fields must not depend on it.
    monkeypatch.setattr(datahub_status, "_get_latest_job_run", lambda job_name: None)

    response = client.get("/api/datahub/status")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["reference_dates"]["latest_complete_trading_day"] == "2026-04-02"
    assert payload["reference_dates"]["previous_complete_trading_day"] == "2026-04-01"
    assert payload["index"]["total_count"] == 4
    assert payload["index"]["asset_status_records_count"] == 3
    assert payload["index"]["freshness_deprecated"] is True
    assert payload["index"]["up_to_date_count"] == 1
    assert payload["index"]["lag_1_day_count"] == 1
    assert payload["index"]["expired_count"] == 1
    assert payload["index"]["no_data_count"] == 1
    assert payload["stock"]["total_count"] == 4
    assert payload["stock"]["up_to_date_count"] == 1
    assert payload["stock"]["lag_1_day_count"] == 1
    assert payload["stock"]["expired_count"] == 1
    assert payload["stock"]["no_data_count"] == 1
    assert payload["quote_run"] is None
    assert payload["index_run"] is None


def _build_job_run(job_name, status, validation):
    started_at = datetime.datetime(2026, 4, 2, 10, 10, 0)
    return {
        "job_name": job_name,
        "status": status,
        "started_at": started_at,
        "completed_at": started_at + datetime.timedelta(minutes=20),
        "error_message": None,
        "summary": {"quote_validation": validation}
        if job_name == "datahub_quote_stock_daily"
        else {"index_validation": validation},
        "phase_stats": {},
    }


def test_get_datahub_status_exposes_a_degraded_quote_run(client, monkeypatch):
    from app.api.v1 import datahub_status

    datahub_status._status_cache["payload"] = None
    datahub_status._status_cache["expires_at"] = 0

    degraded_validation = {
        "attempted_symbols": 5223,
        "validated_symbols": 5221,
        "failed_count": 2,
        "failed_codes": ["sh600001", "sz000002"],
        "coverage_rate": 0.999617,
        "degraded": True,
    }
    index_validation = {
        "attempted_symbols": 6,
        "validated_symbols": 5,
        "failed_count": 0,
        "failed_codes": [],
        "skipped_indices": ["sh000015"],
        "coverage_rate": 0.833333,
        "degraded": True,
    }
    runs = {
        "datahub_quote_stock_daily": _build_job_run(
            "datahub_quote_stock_daily", "DEGRADED", degraded_validation
        ),
        "datahub_quote_index_daily": _build_job_run(
            "datahub_quote_index_daily", "DEGRADED", index_validation
        ),
    }

    monkeypatch.setattr(
        datahub_status,
        "_resolve_reference_dates",
        lambda: {
            "latest_complete_trading_day": datetime.date(2026, 4, 2),
            "previous_complete_trading_day": datetime.date(2026, 4, 1),
        },
    )
    monkeypatch.setattr(
        datahub_status, "_build_category_status", lambda *args, **kwargs: {}
    )
    monkeypatch.setattr(
        datahub_status,
        "_check_pipeline_run_today",
        lambda today_start: {"signal_run_today": True, "scoring_run_today": True},
    )
    monkeypatch.setattr(
        datahub_status, "_get_latest_job_run", lambda job_name: runs.get(job_name)
    )

    response = client.get("/api/datahub/status")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["quote_run"]["status"] == "DEGRADED"
    assert payload["quote_run"]["degraded"] is True
    assert payload["quote_run"]["failed_codes"] == ["sh600001", "sz000002"]
    assert payload["quote_run"]["failed_count"] == 2
    assert payload["quote_run"]["coverage_rate"] == 0.999617
    assert payload["quote_run"]["validated_symbols"] == 5221
    assert payload["index_run"]["status"] == "DEGRADED"
    assert payload["index_run"]["skipped_indices"] == ["sh000015"]
    assert payload["index_run"]["coverage_rate"] == 0.833333


def test_pipeline_run_today_counts_only_success(monkeypatch):
    from app.api.v1 import datahub_status

    queries = []

    class FakeCollection:
        def count_documents(self, query):
            queries.append(query)
            return 0

    class FakeDb:
        datahub_job_runs = FakeCollection()

    monkeypatch.setattr(datahub_status, "get_db", lambda: FakeDb())

    result = datahub_status._check_pipeline_run_today(
        datetime.datetime(2026, 4, 2, 0, 0, 0)
    )

    # A DEGRADED run is usable but never a clean pipeline success today.
    assert result == {"signal_run_today": False, "scoring_run_today": False}
    assert len(queries) == 2
    assert all(query["status"] == "SUCCESS" for query in queries)
