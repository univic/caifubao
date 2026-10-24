"""Tests for tushare ETF ingestion, normalization and FQ wiring."""

import datetime

import pandas
import pytest


def _fund_history_frame():
    return pandas.DataFrame(
        [
            {
                "ts_code": "510300.SH",
                "trade_date": "20240102",
                "open": 3.0,
                "high": 3.1,
                "low": 2.9,
                "close": 3.05,
                "pre_close": 3.0,
                "change": 0.05,
                "pct_chg": 1.67,
                "vol": 100000,
                "amount": 305000.0,
            },
            {
                "ts_code": "510300.SH",
                "trade_date": "20240103",
                "open": 3.06,
                "high": 3.12,
                "low": 3.0,
                "close": 3.1,
                "pre_close": 3.05,
                "change": 0.05,
                "pct_chg": 1.64,
                "vol": 120000,
                "amount": 370000.0,
            },
        ]
    )


def _fund_basic_frame():
    return pandas.DataFrame(
        [
            {
                "ts_code": "510300.SH",
                "name": "沪深300ETF",
                "fund_type": "股票型",
                "benchmark": "沪深300指数收益率",
                "m_fee": 0.5,
                "list_date": "20120528",
                "status": "L",
                "delist_date": None,
            },
            {
                "ts_code": "159915.SZ",
                "name": "创业板ETF",
                "fund_type": "股票型",
                "benchmark": "创业板指数收益率",
                "m_fee": 0.5,
                "list_date": "20110920",
                "status": "D",
                "delist_date": "20240101",
            },
        ]
    )


def test_normalize_fund_universe_maps_codes_and_metadata():
    from app.lib.datahub.data_source.handler import zh_etf_daily

    rows = zh_etf_daily.normalize_fund_universe(_fund_basic_frame())

    by_code = {row["code"]: row for row in rows}
    assert set(by_code) == {"sh510300", "sz159915"}
    assert by_code["sh510300"]["management_fee"] == 0.5
    assert by_code["sh510300"]["list_date"] == datetime.datetime(2012, 5, 28)
    assert by_code["sz159915"]["status"] == "D"


def test_normalize_fund_universe_rejects_missing_ts_code():
    from app.lib.datahub.data_source.handler import zh_etf_daily

    with pytest.raises(ValueError, match="ts_code"):
        zh_etf_daily.normalize_fund_universe(pandas.DataFrame([{"name": "x"}]))


def test_normalize_fund_history_units_and_ordering():
    from app.lib.datahub.data_source.handler import zh_etf_daily

    frame = zh_etf_daily.normalize_tushare_fund_history(
        _fund_history_frame(), "sh510300"
    )

    first = frame.iloc[0]
    assert first["code"] == "sh510300"
    assert first["date"] == datetime.datetime(2024, 1, 2)
    # 千元 -> 元
    assert first["trade_amount"] == pytest.approx(305000.0 * 1000)
    assert first["volume"] == 100000
    assert first["previous_close"] == pytest.approx(3.0)
    assert first["change_rate"] == pytest.approx(1.67)
    assert first["trade_status"] == 1


def test_normalize_fund_snapshot_stamps_requested_date():
    from app.lib.datahub.data_source.handler import zh_etf_daily

    rows = zh_etf_daily.normalize_tushare_fund_snapshot(
        _fund_history_frame(), "20240105"
    )

    assert len(rows) == 2
    assert all(row["date"] == datetime.datetime(2024, 1, 5) for row in rows)
    assert {row["code"] for row in rows} == {"sh510300"}


def test_fund_interface_fetches_and_paces(monkeypatch):
    from app.lib.datahub.data_source.interface import tushare_interface as ti

    calls = []

    class FakePro:
        def fund_basic(self, market):
            calls.append(("basic", market))
            return _fund_basic_frame()

        def fund_daily(self, **kwargs):
            calls.append(("daily", kwargs))
            return _fund_history_frame()

        def fund_adj(self, **kwargs):
            calls.append(("adj", kwargs))
            return pandas.DataFrame(
                [
                    {
                        "ts_code": "510300.SH",
                        "trade_date": "20240102",
                        "adj_factor": 2.0,
                    },
                    {
                        "ts_code": "510300.SH",
                        "trade_date": "20240103",
                        "adj_factor": 2.0,
                    },
                ]
            )

    monkeypatch.setattr(ti, "_get_pro", lambda: FakePro())
    monkeypatch.setattr(ti.time, "sleep", lambda seconds: None)

    assert len(ti.fund_basic_all()) == 2
    assert len(ti.fund_daily_by_trade_date("20240102")) == 2
    assert len(ti.tushare_fund_daily("510300.SH", "20240102", "20240103")) == 2
    assert len(ti.fund_adj("510300.SH", "20240102", "20240103")) == 2
    assert len(ti.fund_adj_by_trade_date("20240102")) == 2
    assert any(call[0] == "basic" for call in calls)


def test_fund_adj_empty_window_fails_loudly(monkeypatch):
    from app.lib.datahub.data_source.interface import tushare_interface as ti

    class FakePro:
        def fund_adj(self, **kwargs):
            return pandas.DataFrame()

    monkeypatch.setattr(ti, "_get_pro", lambda: FakePro())
    monkeypatch.setattr(ti.time, "sleep", lambda seconds: None)

    with pytest.raises(RuntimeError, match="fund_adj returned no rows"):
        ti.fund_adj("510300.SH", "20240102", "20240103")


def test_etf_fq_service_uses_fund_adj_source(monkeypatch):
    from app.lib.datahub import etf_market
    from app.lib.datahub.data_source.interface import tushare_interface as ti

    seen = {}

    def fake_snapshot(trade_date):
        seen["trade_date"] = trade_date
        return pandas.DataFrame(
            [{"ts_code": "510300.SH", "trade_date": trade_date, "adj_factor": 2.0}]
        )

    monkeypatch.setattr(ti, "fund_adj_by_trade_date", fake_snapshot)

    service = etf_market.EtfFQFactorService()
    frame = service._load_market_adj_factor_snapshot(datetime.datetime(2024, 1, 2))

    assert seen["trade_date"] == "20240102"
    assert list(frame["adj_factor"]) == [2.0]


def test_quote_operation_sets_code_date_and_does_not_touch_fq(monkeypatch):
    from bson import ObjectId

    from app.lib.datahub import etf_market
    from app.model.stock import ExchangeTradedFund

    service = etf_market.EtfMarketService()
    fund = ExchangeTradedFund(code="sh510300", name="沪深300ETF")
    fund.id = ObjectId()
    row = {
        "date": datetime.datetime(2024, 1, 2),
        "code": "sh510300",
        "open": 3.0,
        "high": 3.1,
        "low": 2.9,
        "close": 3.05,
        "previous_close": 3.0,
        "volume": 100,
        "trade_amount": 305000.0,
        "change_rate": 1.67,
        "change_amount": 0.05,
        "trade_status": 1,
        "isST": 0,
    }

    operation = service._quote_operation(fund, row)

    assert operation._filter == {
        "code": "sh510300",
        "date": datetime.datetime(2024, 1, 2),
    }
    assert operation._doc["$set"]["code"] == "sh510300"
    assert operation._doc["$set"]["close"] == 3.05
    assert operation._upsert is True
    assert "fq_factor" not in operation._doc["$set"]
    assert "close_hfq" not in operation._doc["$set"]


def test_sync_daily_wires_quotes_then_fq(monkeypatch):
    from app.lib.datahub import etf_market
    from app.model.stock import ExchangeTradedFund

    service = etf_market.EtfMarketService()
    fund = ExchangeTradedFund(code="sh510300", name="沪深300ETF")
    calls = {}

    monkeypatch.setattr(service, "sync_universe", lambda: {"written_count": 1})
    monkeypatch.setattr(service, "_fund_index", lambda: {"sh510300": fund})
    monkeypatch.setattr(
        etf_market.zh_etf_daily,
        "normalize_tushare_fund_snapshot",
        lambda raw, trade_date: [
            {"date": datetime.datetime(2024, 1, 2), "code": "sh510300"}
        ],
    )
    monkeypatch.setattr(
        etf_market.tushare_interface,
        "fund_daily_by_trade_date",
        lambda trade_date: object(),
    )
    monkeypatch.setattr(
        service, "_write_quotes", lambda funds, rows: calls.setdefault("quotes", 1)
    )
    monkeypatch.setattr(
        service,
        "_refresh_quote_statuses",
        lambda codes, expected: calls.setdefault("status", codes),
    )
    monkeypatch.setattr(
        service.fq_service,
        "update_codes_from_market_snapshots",
        lambda code_dates: (
            calls.setdefault("fq", sorted(code_dates)) and {"written_count": 1}
        ),
    )

    summary = service.sync_daily(datetime.datetime(2024, 1, 2))

    assert summary["quotes_written"] == 1
    assert summary["fq"] == {"written_count": 1}
    assert calls["status"] == ["sh510300"]
    assert calls["fq"] == ["sh510300"]


def test_non_exchange_fund_codes_are_skipped():
    from app.lib.datahub.data_source.handler import zh_etf_daily

    basic = pandas.DataFrame(
        [
            {"ts_code": "158008.OF", "name": "场外基金", "status": "L"},
            {"ts_code": "510300.SH", "name": "沪深300ETF", "status": "L"},
        ]
    )
    snapshot = pandas.DataFrame(
        [
            {
                "ts_code": "158008.OF",
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "pre_close": 1.0,
                "vol": 1,
                "amount": 1,
                "change": 0.0,
                "pct_chg": 0.0,
            },
            {
                "ts_code": "510300.SH",
                "open": 4.0,
                "high": 4.1,
                "low": 3.9,
                "close": 4.05,
                "pre_close": 4.0,
                "vol": 10,
                "amount": 100,
                "change": 0.05,
                "pct_chg": 1.25,
            },
        ]
    )

    assert [row["code"] for row in zh_etf_daily.normalize_fund_universe(basic)] == [
        "sh510300"
    ]
    rows = zh_etf_daily.normalize_tushare_fund_snapshot(snapshot, "20240102")
    assert [row["code"] for row in rows] == ["sh510300"]


def test_fund_adj_paginates_in_ten_year_windows(monkeypatch):
    from app.lib.datahub.data_source.interface import tushare_interface as ti

    calls = []

    class FakePro:
        def fund_adj(self, **kwargs):
            calls.append(kwargs)
            return pandas.DataFrame(
                [
                    {
                        "ts_code": "510050.SH",
                        "trade_date": kwargs["start_date"],
                        "adj_factor": 1.0,
                    }
                ]
            )

    monkeypatch.setattr(ti, "_get_pro", lambda: FakePro())
    monkeypatch.setattr(ti.time, "sleep", lambda seconds: None)

    frame = ti.fund_adj("510050.SH", "20040101", "20261023")

    assert len(frame) == 3
    assert [call["start_date"] for call in calls] == [
        "20040101",
        "20140101",
        "20240101",
    ]


def test_fund_adj_cap_truncation_fails_loudly(monkeypatch):
    from app.lib.datahub.data_source.interface import tushare_interface as ti

    class FakePro:
        def fund_adj(self, **kwargs):
            return pandas.DataFrame(
                [{"ts_code": "510300.SH", "trade_date": "20160204", "adj_factor": 1.0}]
                * 2600
            )

    monkeypatch.setattr(ti, "_get_pro", lambda: FakePro())
    monkeypatch.setattr(ti.time, "sleep", lambda seconds: None)

    with pytest.raises(RuntimeError, match="window truncated"):
        ti.fund_adj("510300.SH", "20040101", "20051231")


def test_universe_values_map_listed_and_terminated_status():
    from app.lib.datahub.etf_market import EtfMarketService

    listed = EtfMarketService._universe_values(
        {"code": "sh510300", "name": "x", "status": "L"}
    )
    terminated = EtfMarketService._universe_values(
        {"code": "sh510301", "name": "y", "status": "D"}
    )

    assert listed["active_status"] == 0
    assert listed["object_type"] == "etf"
    assert terminated["active_status"] == 2


def test_write_quotes_bulk_writes_selected_funds():
    from app.lib.datahub import etf_market
    from app.model.stock import ExchangeTradedFund

    class FakeCollection:
        def __init__(self):
            self.calls = []

        def bulk_write(self, operations, ordered):
            self.calls.append((list(operations), ordered))

    from app.model.stock import EtfDailyQuote

    class FakeQuoteModel:
        def __init__(self):
            self.collection = FakeCollection()

        def __call__(self):
            return EtfDailyQuote()

        def _get_collection(self):
            return self.collection

    fund = ExchangeTradedFund(code="sh510300", name="x")
    service = etf_market.EtfMarketService(quote_model=FakeQuoteModel())
    row = {
        "date": datetime.datetime(2024, 1, 2),
        "code": "sh510300",
        "open": 3.0,
        "high": 3.1,
        "low": 2.9,
        "close": 3.05,
        "previous_close": 3.0,
        "volume": 100,
        "trade_amount": 305000.0,
        "change_rate": 1.67,
        "change_amount": 0.05,
        "trade_status": 1,
    }

    written = service._write_quotes(
        {"sh510300": fund}, [row, dict(row, code="sz159915")]
    )

    # The second row has no fund in the map and must be dropped.
    assert written == 1
    operations, ordered = service.quote_model.collection.calls[0]
    assert len(operations) == 1
    assert ordered is False
    assert operations[0]._doc["$set"]["code"] == "sh510300"
    assert "isST" not in operations[0]._doc["$set"]
    assert "peTTM" not in operations[0]._doc["$set"]


def test_sync_daily_rejects_unknown_snapshot_codes(monkeypatch):
    from app.lib.datahub import etf_market

    service = etf_market.EtfMarketService()
    monkeypatch.setattr(service, "sync_universe", lambda: {"written_count": 0})
    monkeypatch.setattr(service, "_fund_index", lambda: {})
    monkeypatch.setattr(
        etf_market.zh_etf_daily,
        "normalize_tushare_fund_snapshot",
        lambda raw, trade_date: [
            {"date": datetime.datetime(2024, 1, 2), "code": "sh510300"}
        ],
    )
    monkeypatch.setattr(
        etf_market.tushare_interface,
        "fund_daily_by_trade_date",
        lambda trade_date: object(),
    )

    with pytest.raises(RuntimeError, match="missing from the ETF universe"):
        service.sync_daily(datetime.datetime(2024, 1, 2))


def test_backfill_code_rejects_unknown_fund():
    from app.lib.datahub import etf_market

    class FakeFundModel:
        def objects(self, **kwargs):
            class Query:
                def first(self):
                    return None

            return Query()

    service = etf_market.EtfMarketService(fund_model=FakeFundModel())

    with pytest.raises(RuntimeError, match="fund not found"):
        service.backfill_code("sh999999")


def test_backfill_all_collects_failures(monkeypatch):
    from app.lib.datahub import etf_market

    service = etf_market.EtfMarketService()
    monkeypatch.setattr(
        service, "_active_funds", lambda: {"sh510300": object(), "sz159915": object()}
    )

    def fake_backfill(code):
        if code == "sz159915":
            raise RuntimeError("boom")
        return {"code": code, "status": "GOOD", "quotes_written": 2}

    monkeypatch.setattr(service, "backfill_code", fake_backfill)

    summary = service.backfill_all()

    assert summary["failed_count"] == 1
    assert summary["failed_codes"] == ["sz159915"]
    assert summary["written_count"] == 2


def test_truncation_guard_accepts_a_complete_at_cap_window():
    from app.lib.datahub.data_source.interface import tushare_interface as ti

    frame = pandas.DataFrame(
        [{"trade_date": "20040101", "adj_factor": 1.0}] * ti.FUND_ADJ_MAX_ROWS
    )

    # A full window that starts at the requested date is complete, not truncated.
    ti._assert_window_not_truncated(
        frame,
        window_start_date="20040101",
        window_end_date="20131231",
        row_cap=ti.FUND_ADJ_MAX_ROWS,
        label="fund_adj",
    )
