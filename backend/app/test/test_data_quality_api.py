import datetime
from types import SimpleNamespace


def setup_function():
    from app.api.v1 import data_quality

    data_quality._clear_items_cache()


def _stock(code, name):
    return SimpleNamespace(
        code=code,
        name=name,
        object_type="individual_stock",
        active_status=0,
    )


def _meta(code, name, dt, object_type="individual_stock"):
    asset_type = "quote" if name == "daily_quote" else "factor"
    return SimpleNamespace(
        code=code,
        object_type=object_type,
        asset_type=asset_type,
        asset_name=name,
        latest_data_date=dt,
        data_count=120,
        status="OK",
        status_reason=None,
        last_calculated_at=datetime.datetime(2026, 4, 12, 10, 0),
    )


def _asset(code, name, dt=None, status="OK"):
    row = _meta(code, name, dt or datetime.datetime(2026, 4, 10))
    row.status = status
    if status == "NO_DATA":
        row.latest_data_date = None
        row.data_count = 0
        row.status_reason = "no_source_data"
    if status == "NOT_APPLICABLE":
        row.latest_data_date = None
        row.data_count = 0
        row.status_reason = "capability_disabled"
    return row


def test_data_quality_summary_and_items(client, monkeypatch):
    from app.api.v1 import data_quality

    stocks = [
        _stock("sh600000", "浦发银行"),
        _stock("sz000001", "平安银行"),
        _stock("bj430047", "北交所样本"),
    ]
    quote_date = datetime.datetime(2026, 4, 10)
    stale_date = datetime.datetime(2026, 4, 9)
    asset_rows = [
        _asset("sh600000", "daily_quote", quote_date),
        _asset("sh600000", "FQ_FACTOR", quote_date),
        *[
            _asset("sh600000", name, quote_date)
            for name in data_quality.MA_FACTOR_NAMES
        ],
        _asset("sz000001", "daily_quote", quote_date),
        _asset("sz000001", "FQ_FACTOR", stale_date, "STALE"),
        _asset("sz000001", "MA_10", quote_date),
        _asset("sz000001", "MA_20", stale_date, "STALE"),
        _asset("sz000001", "MA_30", status="NO_DATA"),
        _asset("sz000001", "MA_60", status="NO_DATA"),
        _asset("sz000001", "MA_120", status="NO_DATA"),
    ]

    monkeypatch.setattr(
        data_quality,
        "_load_active_stocks",
        lambda query_text="": [
            stock
            for stock in stocks
            if data_quality._is_data_quality_supported_stock(stock)
        ],
    )
    monkeypatch.setattr(
        data_quality,
        "_load_asset_status_map",
        lambda codes: {(row.code, row.asset_name): row for row in asset_rows},
    )
    monkeypatch.setattr(
        data_quality,
        "_build_stock_scope",
        lambda: {
            "total_active": 3,
            "excluded_unsupported": 1,
            "effective_total": 2,
            "unsupported_markets": ["BSE"],
        },
    )
    monkeypatch.setattr(
        data_quality,
        "_resolve_expected_quote_date",
        lambda current_dt=None: quote_date,
    )
    monkeypatch.setattr(
        data_quality.datetime,
        "datetime",
        type(
            "FixedDatetime",
            (datetime.datetime,),
            {
                "now": classmethod(
                    lambda cls, tz=None: datetime.datetime(2026, 4, 12, 10, 30)
                )
            },
        ),
    )

    summary_response = client.get("/api/data-quality/summary")
    assert summary_response.status_code == 200
    summary = summary_response.get_json()
    assert summary["status"] == "ERROR"
    assert summary["generated_at"] == "2026-04-12T10:30:00+08:00"
    assert summary["coverage"]["overall"]["total"] == 2
    assert summary["coverage"]["overall"]["ok"] == 1
    assert summary["coverage"]["overall"]["missing"] == 1
    assert summary["coverage"]["quote"]["ok"] == 2
    assert summary["scope"]["excluded_unsupported"] == 1
    assert summary["coverage"]["fq_factor"]["stale"] == 1
    assert summary["coverage"]["ma_factor"]["missing"] == 1

    items_response = client.get("/api/data-quality/items?status=abnormal")
    assert items_response.status_code == 200
    items_payload = items_response.get_json()
    assert items_payload["total"] == 1
    assert items_payload["items"][0]["code"] == "sz000001"
    assert "FQ_FACTOR stale" in items_payload["items"][0]["issues"]
    assert "MA_120 missing" in items_payload["items"][0]["issues"]


def test_data_quality_excludes_bj_stocks(client, monkeypatch):
    from app.api.v1 import data_quality

    stocks = [
        _stock("sh600000", "浦发银行"),
        _stock("bj430047", "北交所样本"),
    ]
    quote_date = datetime.datetime(2026, 4, 10)
    asset_rows = [
        _asset("sh600000", "daily_quote", quote_date),
        _asset("sh600000", "FQ_FACTOR", quote_date),
        *[
            _asset("sh600000", name, quote_date)
            for name in data_quality.MA_FACTOR_NAMES
        ],
        _asset("bj430047", "daily_quote", quote_date),
        _asset("bj430047", "FQ_FACTOR", quote_date),
        *[
            _asset("bj430047", name, quote_date)
            for name in data_quality.MA_FACTOR_NAMES
        ],
    ]

    monkeypatch.setattr(
        data_quality,
        "_load_active_stocks",
        lambda query_text="": [
            stock
            for stock in stocks
            if data_quality._is_data_quality_supported_stock(stock)
        ],
    )
    monkeypatch.setattr(
        data_quality,
        "_load_asset_status_map",
        lambda codes: {(row.code, row.asset_name): row for row in asset_rows},
    )
    monkeypatch.setattr(
        data_quality,
        "_build_stock_scope",
        lambda: {
            "total_active": 2,
            "excluded_unsupported": 1,
            "effective_total": 1,
            "unsupported_markets": ["BSE"],
        },
    )
    monkeypatch.setattr(
        data_quality,
        "_resolve_expected_quote_date",
        lambda current_dt=None: quote_date,
    )

    summary_response = client.get("/api/data-quality/summary")
    assert summary_response.status_code == 200
    summary = summary_response.get_json()
    assert summary["coverage"]["overall"]["total"] == 1
    assert summary["coverage"]["overall"]["ok"] == 1

    items_response = client.get("/api/data-quality/items?status=all")
    assert items_response.status_code == 200
    items_payload = items_response.get_json()
    assert items_payload["total"] == 1
    assert [item["code"] for item in items_payload["items"]] == ["sh600000"]


def test_data_quality_flags_market_stale_when_latest_quote_lags_expected_date(
    client, monkeypatch
):
    from app.api.v1 import data_quality

    stocks = [_stock("sh600000", "浦发银行")]
    quote_date = datetime.datetime(2026, 4, 10)
    expected_quote_date = datetime.date(2026, 4, 13)
    asset_rows = [
        _asset("sh600000", "daily_quote", quote_date),
        _asset("sh600000", "FQ_FACTOR", quote_date),
        *[
            _asset("sh600000", name, quote_date)
            for name in data_quality.MA_FACTOR_NAMES
        ],
    ]

    monkeypatch.setattr(
        data_quality, "_load_active_stocks", lambda query_text="": stocks
    )
    monkeypatch.setattr(
        data_quality,
        "_load_asset_status_map",
        lambda codes: {(row.code, row.asset_name): row for row in asset_rows},
    )
    monkeypatch.setattr(
        data_quality,
        "_build_stock_scope",
        lambda: {
            "total_active": 1,
            "excluded_unsupported": 0,
            "effective_total": 1,
            "unsupported_markets": ["BSE"],
        },
    )
    monkeypatch.setattr(
        data_quality,
        "_resolve_expected_quote_date",
        lambda current_dt=None: expected_quote_date,
    )

    summary_response = client.get("/api/data-quality/summary")
    assert summary_response.status_code == 200
    summary = summary_response.get_json()
    assert summary["status"] == "ERROR"
    assert summary["expected_quote_date"] == "2026-04-13"
    assert summary["coverage"]["overall"]["stale"] == 1
    assert summary["coverage"]["quote"]["stale"] == 1
    assert summary["coverage"]["quote"]["ok"] == 0

    items_response = client.get("/api/data-quality/items?status=abnormal")
    assert items_response.status_code == 200
    items_payload = items_response.get_json()
    assert items_payload["total"] == 1
    assert items_payload["items"][0]["status"] == "STALE"
    assert items_payload["items"][0]["quote_status"] == "STALE"
    assert "quote stale" in items_payload["items"][0]["issues"]


def test_data_quality_blocks_derived_factors_when_quote_lags_expected_date(
    client, monkeypatch
):
    from app.api.v1 import data_quality

    stocks = [_stock("sh600000", "浦发银行")]
    quote_date = datetime.datetime(2026, 4, 10)
    expected_quote_date = datetime.date(2026, 4, 13)
    asset_rows = [
        _asset("sh600000", "daily_quote", quote_date),
        _asset("sh600000", "FQ_FACTOR", quote_date),
        *[
            _asset("sh600000", name, quote_date)
            for name in data_quality.MA_FACTOR_NAMES
        ],
    ]

    monkeypatch.setattr(
        data_quality, "_load_active_stocks", lambda query_text="": stocks
    )
    monkeypatch.setattr(
        data_quality,
        "_load_asset_status_map",
        lambda codes: {(row.code, row.asset_name): row for row in asset_rows},
    )
    monkeypatch.setattr(
        data_quality,
        "_build_stock_scope",
        lambda: {
            "total_active": 1,
            "excluded_unsupported": 0,
            "effective_total": 1,
            "unsupported_markets": ["BSE"],
        },
    )
    monkeypatch.setattr(
        data_quality,
        "_resolve_expected_quote_date",
        lambda current_dt=None: expected_quote_date,
    )

    summary_response = client.get("/api/data-quality/summary")
    assert summary_response.status_code == 200
    summary = summary_response.get_json()
    assert summary["status"] == "ERROR"
    assert summary["coverage"]["quote"]["stale"] == 1
    assert summary["coverage"]["fq_factor"]["total"] == 0
    assert summary["coverage"]["fq_factor"]["blocked"] == 1
    assert summary["coverage"]["fq_factor"]["ok_rate"] == 0
    assert summary["coverage"]["ma_factor"]["total"] == 0
    assert summary["coverage"]["ma_factor"]["blocked"] == 1
    assert summary["coverage"]["ma_factor"]["ok_rate"] == 0

    items_response = client.get("/api/data-quality/items?status=abnormal")
    assert items_response.status_code == 200
    items_payload = items_response.get_json()
    assert items_payload["total"] == 1
    item = items_payload["items"][0]
    assert item["status"] == "STALE"
    assert item["quote_status"] == "STALE"
    assert item["fq_factor_status"] == data_quality.STATUS_BLOCKED_BY_QUOTE
    assert all(
        status == data_quality.STATUS_BLOCKED_BY_QUOTE
        for status in item["ma_statuses"].values()
    )
    assert "FQ_FACTOR stale" not in item["issues"]
    assert all("MA_" not in issue for issue in item["issues"])


def test_data_quality_expected_quote_date_normalizes_timezone(monkeypatch):
    from app.api.v1 import data_quality

    captured = {}

    class FakeMarketQuery:
        @staticmethod
        def first():
            return SimpleNamespace(
                trade_calendar=[
                    datetime.datetime(2026, 4, 10),
                    datetime.datetime(2026, 4, 13),
                ]
            )

    class FakeFinanceMarket:
        @staticmethod
        def objects(**kwargs):
            return FakeMarketQuery()

    def fake_previous_complete_trading_day(trade_calendar, current_dt):
        captured["current_dt"] = current_dt
        return trade_calendar[-1]

    monkeypatch.setattr(data_quality, "FinanceMarket", FakeFinanceMarket)
    monkeypatch.setattr(
        data_quality,
        "determine_most_recent_previous_complete_trading_day",
        fake_previous_complete_trading_day,
    )

    result = data_quality._resolve_expected_quote_date(
        datetime.datetime(
            2026,
            4,
            13,
            18,
            0,
            tzinfo=datetime.timezone.utc,
        )
    )

    assert result == datetime.datetime(2026, 4, 13)
    assert captured["current_dt"] == datetime.datetime(2026, 4, 14, 2, 0)
    assert captured["current_dt"].tzinfo is None


def test_data_quality_ignores_ma_windows_without_enough_quotes():
    from app.api.v1 import data_quality

    stock = _stock("sh688001", "上市不足 120 日样本")
    quote_date = datetime.datetime(2026, 4, 10)
    asset_rows = [
        _asset(stock.code, "daily_quote", quote_date),
        _asset(stock.code, "FQ_FACTOR", quote_date),
        _asset(stock.code, "MA_10", quote_date),
        _asset(stock.code, "MA_20", quote_date),
        _asset(stock.code, "MA_30", quote_date),
        _asset(stock.code, "MA_60", quote_date),
        _asset(stock.code, "MA_120", status="NOT_APPLICABLE"),
    ]
    asset_status_map = {(row.code, row.asset_name): row for row in asset_rows}

    item = data_quality._build_quality_item(stock, asset_status_map)

    assert item["status"] == "OK"
    assert item["ma_statuses"]["MA_60"] == "OK"
    assert item["ma_statuses"]["MA_120"] == data_quality.STATUS_NOT_APPLICABLE
    assert "MA_120 missing" not in item["issues"]
    assert data_quality._build_factor_coverage([item], "ma_dates")["ok"] == 1


def test_data_quality_capability_fallback_excludes_bse_9_prefix():
    from app.api.v1 import data_quality

    assert not data_quality._is_data_quality_supported_stock(
        _stock("bj920118", "北交所样本")
    )
    assert not data_quality._is_data_quality_supported_stock(
        _stock("920118", "北交所裸代码样本")
    )
    assert data_quality._is_data_quality_supported_stock(_stock("sh600000", "浦发银行"))


def test_data_quality_accepts_asset_status_object_type():
    from app.api.v1 import data_quality

    quote_asset = _meta(
        "sh600000",
        "daily_quote",
        datetime.datetime(2026, 4, 10),
        object_type=None,
    )
    assert data_quality._is_target_asset(quote_asset)
