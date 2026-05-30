import datetime as dt

from app.jobs import parquet_export_runner as runner


def test_build_query_omits_date_when_unbounded():
    assert runner._build_query("date", None, None) == {}


def test_build_query_uses_inclusive_date_bounds():
    from_date = dt.datetime(2026, 5, 1)
    to_date = dt.datetime(2026, 5, 25)

    assert runner._build_query("date", from_date, to_date) == {
        "date": {"$gte": from_date, "$lte": to_date}
    }


def test_normalize_doc_adds_trade_date_and_drops_private_fields():
    config = runner.DATASETS["daily_quotes"]
    doc = {
        "_id": object(),
        "_cls": "StockDailyQuote",
        "stock": object(),
        "code": "sh600000",
        "date": dt.datetime(2026, 5, 25, 15),
        "close": 10.2,
    }

    normalized = runner._normalize_doc(doc, config)

    assert normalized == {
        "code": "sh600000",
        "date": dt.datetime(2026, 5, 25, 15),
        "close": 10.2,
        "trade_date": "2026-05-25",
    }


def test_object_key_uses_partitioned_layout():
    assert (
        runner._object_key("data-lake", "china-a/daily_quotes", "2026-05-25")
        == "data-lake/china-a/daily_quotes/trade_date=2026-05-25/part-2026-05-25.parquet"
    )
