"""Tests for ChinaAStock processor helpers."""

from __future__ import annotations

import os

from unittest import TestCase
from unittest.mock import Mock, patch
import datetime
from zoneinfo import ZoneInfo
from types import SimpleNamespace

import pandas
from bson import ObjectId


class DummyBulkWriteError(Exception):
    """Minimal exception shim exposing pymongo-like details."""

    def __init__(self, details):
        super().__init__("bulk write error")
        self.details = details


class DummyMongoEngineBulkWriteError(Exception):
    """Minimal exception shim exposing mongoengine-style args payload."""

    def __init__(self, details):
        super().__init__(details)


class EmptyQuerySet(list):
    def count(self):
        return 0


class DummyQuerySet(list):
    def count(self):
        return len(self)


class TestChinaAStockHelpers(TestCase):
    @staticmethod
    def _stock(code):
        return SimpleNamespace(
            code=code,
            name=code,
            active_status=0,
            data_capabilities=SimpleNamespace(
                daily_quote=True, fq_factor=True, ma_factor=True
            ),
            save=Mock(),
        )

    def test_quote_as_of_date_is_frozen_after_first_resolution(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(
            name="ChinaAStock",
            trade_calendar=[
                datetime.datetime(2026, 8, 21),
                datetime.datetime(2026, 8, 24),
                datetime.datetime(2026, 8, 25),
            ],
        )
        processor.today = datetime.date(2026, 8, 24)
        processor.run_started_at = datetime.datetime(
            2026, 8, 24, 15, 59, tzinfo=ZoneInfo("Asia/Shanghai")
        )
        processor.explicit_as_of_date = False
        processor.most_recent_trading_day = None

        processor.perform_date_check()
        processor.run_started_at = datetime.datetime(
            2026, 8, 25, 18, 0, tzinfo=ZoneInfo("Asia/Shanghai")
        )
        processor.perform_date_check()

        self.assertEqual(
            processor.most_recent_trading_day, datetime.datetime(2026, 8, 21)
        )

    def test_explicit_as_of_date_must_be_a_completed_trading_day(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(
            name="ChinaAStock",
            trade_calendar=[
                datetime.datetime(2026, 8, 21),
                datetime.datetime(2026, 8, 24),
            ],
        )
        processor.today = datetime.date(2026, 8, 24)
        processor.run_started_at = datetime.datetime(
            2026, 8, 24, 15, 0, tzinfo=ZoneInfo("Asia/Shanghai")
        )
        processor.explicit_as_of_date = True
        processor.most_recent_trading_day = datetime.datetime(2026, 8, 24)

        with self.assertRaisesRegex(ValueError, "completed market trading day"):
            processor.perform_date_check()

    def test_stock_quote_operation_is_an_idempotent_upsert(self):
        from app.lib.datahub.processors.china_a_stock import (
            _build_stock_quote_upsert_operation,
        )
        from app.model.stock import IndividualStock

        stock = IndividualStock(code="sh600519", name="Kweichow Moutai")
        stock.id = ObjectId()
        operation = _build_stock_quote_upsert_operation(
            stock,
            pandas.Series(
                {
                    "date": pandas.Timestamp("2026-08-21"),
                    "code": stock.code,
                    "open": 1.0,
                    "high": 2.0,
                    "low": 0.5,
                    "close": 1.5,
                    "previous_close": 1.0,
                }
            ),
        )

        self.assertTrue(operation._upsert)
        self.assertNotIn("_cls", operation._filter)
        self.assertEqual(operation._filter["code"], stock.code)
        self.assertEqual(operation._filter["date"], pandas.Timestamp("2026-08-21"))
        self.assertEqual(operation._doc["$set"]["close"], 1.5)

    def test_one_day_update_writes_from_market_snapshot(self):
        from app.lib.datahub.data_source.handler import zh_a_daily
        from app.lib.datahub.processors.china_a_stock import ChinaAStock
        from app.model.stock import StockDailyQuote

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock", trade_calendar=[])
        processor.most_recent_trading_day = pandas.Timestamp("2026-08-21")
        processor.perform_date_check = lambda: None
        processor.check_data_freshness = lambda stock, **kwargs: "UPD"
        processor.perform_stock_name_check = Mock()
        processor.update_active_status = Mock()
        processor.get_hist_quote_data = Mock(
            return_value={
                "code": "GOOD",
                "written_count": 1,
                "validated_count": 1,
                "freshness_status": "OK",
            }
        )
        processor.handle_new_quote = Mock()
        stock = SimpleNamespace(
            code="sh600519",
            name="Kweichow Moutai",
            active_status=0,
            data_capabilities=SimpleNamespace(
                daily_quote=True, fq_factor=True, ma_factor=True
            ),
            save=Mock(),
        )
        remote = pandas.DataFrame(
            [
                {
                    "code": stock.code,
                    "name": stock.name,
                    "close": 100.0,
                    "open": 99.0,
                    "high": 101.0,
                    "low": 98.0,
                    "volume": 1000,
                    "previous_close": 99.5,
                    "trade_amount": 1e8,
                    "turnover_rate": 0.5,
                    "change_rate": 0.5,
                    "change_amount": 0.5,
                }
            ]
        )

        quote_collection = Mock()
        quote_collection.bulk_write.return_value = Mock(
            upserted_count=1, modified_count=0
        )

        def fake_build_op(stock_obj, row):
            return Mock(
                _filter={"code": stock_obj.code, "date": row["date"]},
                _doc={"$set": dict(row)},
                _upsert=True,
            )

        with (
            patch.dict(
                os.environ,
                {zh_a_daily.STOCK_UNIVERSE_SOURCE_ENV: "tushare"},
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.progress_bar",
                return_value=lambda *args: None,
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock._build_stock_quote_upsert_operation",
                side_effect=fake_build_op,
            ),
            patch.object(
                StockDailyQuote, "_get_collection", return_value=quote_collection
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
                return_value={},
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.aggregate_stats_by_code",
                return_value={
                    "sh600519": {
                        "first_data_date": None,
                        "latest_data_date": pandas.Timestamp("2026-08-21"),
                        "data_count": 1,
                    }
                },
            ) as aggregate,
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.bulk_upsert_asset_status",
                return_value=1,
            ) as bulk_upsert,
        ):
            result = processor.check_data_integrity(
                obj_type="stock",
                local_data_list=DummyQuerySet([stock]),
                remote_data_df=remote,
                hist_handler="get_hist_stock_quote_data",
                allow_update=True,
            )

        # UPD（差 1 天）+ tushare universe → 快照写入，不拉历史
        processor.get_hist_quote_data.assert_not_called()
        quote_collection.bulk_write.assert_called_once()
        operations = quote_collection.bulk_write.call_args[0][0]
        self.assertEqual(len(operations), 1)
        self.assertEqual(operations[0]._filter["date"], pandas.Timestamp("2026-08-21"))
        # 快照行 + 批量 freshness 刷新各一次
        aggregate.assert_called_once()
        bulk_upsert.assert_called_once()
        self.assertEqual(result["written_count"], 1)

    def test_deeper_gap_or_suspended_uses_history(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        # 默认 universe 源为 spot：UPD+交易中 也走历史（快照路径仅限 tushare）
        for freshness, close in [
            ("INC", 100.0),
            ("FULL", 100.0),
            ("UPD", 0.0),
            ("UPD", 100.0),
        ]:
            with self.subTest(freshness=freshness, close=close):
                processor = object.__new__(ChinaAStock)
                processor.market = SimpleNamespace(
                    name="ChinaAStock", trade_calendar=[]
                )
                processor.most_recent_trading_day = pandas.Timestamp("2026-08-21")
                processor.perform_date_check = lambda: None
                processor.check_data_freshness = lambda stock, f=freshness, **kwargs: f
                processor.perform_stock_name_check = Mock()
                processor.update_active_status = Mock()
                # INC/FULL 成功拉取 -> OK；停牌 UPD（close=0）无数据 -> STALE
                stale = close == 0.0
                processor.get_hist_quote_data = Mock(
                    return_value={
                        "code": "GOOD",
                        "written_count": 0 if stale else 5,
                        "validated_count": 0 if stale else 5,
                        "freshness_status": "STALE" if stale else "OK",
                    }
                )
                stock = SimpleNamespace(
                    code="sh600519",
                    name="Kweichow Moutai",
                    active_status=0,
                    data_capabilities=SimpleNamespace(
                        daily_quote=True, fq_factor=True, ma_factor=True
                    ),
                    save=Mock(),
                )
                remote = pandas.DataFrame(
                    [
                        {
                            "code": stock.code,
                            "name": stock.name,
                            "close": close,
                            "open": 0,
                            "high": 0,
                            "low": 0,
                            "volume": 0,
                        }
                    ]
                )
                with (
                    patch(
                        "app.lib.datahub.processors.china_a_stock.progress_bar",
                        return_value=lambda *args: None,
                    ),
                    patch(
                        "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
                        return_value={},
                    ),
                ):
                    processor.check_data_integrity(
                        obj_type="stock",
                        local_data_list=DummyQuerySet([stock]),
                        remote_data_df=remote,
                        hist_handler="get_hist_stock_quote_data",
                        allow_update=True,
                    )
                # INC/FULL（差 >1 天）与停牌 UPD 均走历史，不用快照
                processor.get_hist_quote_data.assert_called_once()

    def test_build_snapshot_quote_row_coerces_non_numeric_cells(self):
        from app.lib.datahub.processors.china_a_stock import (
            _build_snapshot_quote_row,
        )

        row = _build_snapshot_quote_row(
            {
                "open": "1271.01",
                "high": "1313.8",
                "low": "1270.33",
                "close": 1304.66,
                "previous_close": "1272.83",
                "volume": "4844000",
                "trade_amount": "6299794000.0",
                "turnover_rate": 0.5,
                "change_rate": 2.5,
                "change_amount": 31.83,
            },
            datetime.datetime(2026, 8, 25),
            "sh600519",
        )

        self.assertEqual(row["close"], 1304.66)
        self.assertEqual(row["trade_status"], 1)
        self.assertEqual(row["date"], datetime.datetime(2026, 8, 25))
        self.assertEqual(row["code"], "sh600519")

        row = _build_snapshot_quote_row(
            {
                "close": "-",
                "high": None,
                "low": "",
                "previous_close": "0.00",
                "volume": "--",
                "trade_amount": "1e8",
            },
            datetime.datetime(2026, 8, 25),
            "sh600519",
        )

        # 非数值单元格被强转为 0，不抛 TypeError
        self.assertEqual(row["close"], 0.0)
        self.assertEqual(row["volume"], 0.0)
        self.assertEqual(row["previous_close"], 0.0)
        self.assertEqual(row["trade_amount"], 100000000.0)

    def test_flush_batched_quote_updates_writes_and_refreshes_freshness(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock
        from app.model.stock import StockDailyQuote

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock", trade_calendar=[])
        processor.most_recent_trading_day = pandas.Timestamp("2026-08-21")
        stock = SimpleNamespace(code="sh600519", name="贵州茅台")

        collection = Mock()
        collection.bulk_write.return_value = Mock(upserted_count=1, modified_count=0)
        captured = {}

        def fake_build(stock_obj, row):
            captured["row"] = row
            operation = Mock()
            operation._doc = {"$set": dict(row)}
            return operation

        upserted_records = []

        def fake_bulk_upsert(records, **kwargs):
            upserted_records.extend(records)
            return len(records)

        with (
            patch(
                "app.lib.datahub.processors.china_a_stock._build_stock_quote_upsert_operation",
                side_effect=fake_build,
            ),
            patch.object(StockDailyQuote, "_get_collection", return_value=collection),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.aggregate_stats_by_code",
                return_value={
                    "sh600519": {
                        "first_data_date": None,
                        "latest_data_date": pandas.Timestamp("2026-08-21"),
                        "data_count": 1,
                    }
                },
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.bulk_upsert_asset_status",
                side_effect=fake_bulk_upsert,
            ),
        ):
            written_count, statuses = processor._flush_batched_quote_updates(
                [(stock, {"close": 1.5})],
                {},
            )

        self.assertEqual(written_count, 1)
        self.assertEqual(statuses, {"sh600519": "OK"})
        self.assertEqual(len(upserted_records), 1)
        self.assertEqual(upserted_records[0]["code"], "sh600519")
        self.assertEqual(upserted_records[0]["asset_name"], "daily_quote")
        self.assertEqual(upserted_records[0]["status"], "OK")
        self.assertEqual(captured["row"]["close"], 1.5)

    def test_flush_batched_quote_updates_write_failure_propagates(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock
        from app.model.stock import StockDailyQuote

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock", trade_calendar=[])
        processor.most_recent_trading_day = pandas.Timestamp("2026-08-21")
        stock = SimpleNamespace(code="sh600519", name="贵州茅台")

        collection = Mock()
        collection.bulk_write.side_effect = RuntimeError("boom")

        with (
            patch(
                "app.lib.datahub.processors.china_a_stock._build_stock_quote_upsert_operation",
                return_value=Mock(),
            ),
            patch.object(StockDailyQuote, "_get_collection", return_value=collection),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.aggregate_stats_by_code"
            ) as aggregate,
        ):
            with self.assertRaisesRegex(RuntimeError, "boom"):
                processor._flush_batched_quote_updates([(stock, {"close": 1.0})], {})

        # fail-closed：写入失败时不刷新 freshness
        aggregate.assert_not_called()

    def test_partial_quote_validation_failure_fails_the_phase(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock", trade_calendar=[])
        processor.most_recent_trading_day = pandas.Timestamp("2026-08-21")
        processor.perform_date_check = lambda: None
        processor.check_data_freshness = lambda stock, **kwargs: "FULL"
        processor.perform_stock_name_check = Mock()
        processor.update_active_status = Mock()
        processor.get_hist_quote_data = Mock(
            side_effect=[
                {
                    "code": "GOOD",
                    "written_count": 1,
                    "validated_count": 1,
                    "freshness_status": "OK",
                },
                {
                    "code": "GOOD",
                    "written_count": 1,
                    "validated_count": 1,
                    "freshness_status": "STALE",
                },
            ]
        )
        stocks = DummyQuerySet([self._stock("sh600000"), self._stock("sh600004")])
        remote = pandas.DataFrame(
            [{"code": stock.code, "name": stock.name, "close": 1} for stock in stocks]
        )

        with (
            patch(
                "app.lib.datahub.processors.china_a_stock.progress_bar",
                return_value=lambda *args: None,
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
                return_value={},
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "sh600004"):
                processor.check_data_integrity(
                    obj_type="stock",
                    local_data_list=stocks,
                    remote_data_df=remote,
                    hist_handler="get_hist_stock_quote_data",
                    allow_update=True,
                )

    def test_local_active_stock_missing_from_spot_participates_in_validation(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock", trade_calendar=[])
        processor.most_recent_trading_day = pandas.Timestamp("2026-08-21")
        processor.perform_date_check = lambda: None
        processor.check_data_freshness = lambda stock, **kwargs: "GOOD"
        processor.perform_stock_name_check = Mock()
        processor.update_active_status = Mock()
        present = self._stock("sh600000")
        missing = self._stock("sh600004")
        remote = pandas.DataFrame(
            [{"code": present.code, "name": present.name, "close": 1}]
        )

        from app.model.stock import StockDailyQuote as _QuoteModel

        with (
            patch(
                "app.lib.datahub.processors.china_a_stock.progress_bar",
                return_value=lambda *args: None,
            ),
            patch.object(_QuoteModel, "_get_collection", return_value=Mock()),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
                return_value={},
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.aggregate_stats_by_code",
                return_value={
                    "sh600000": {
                        "first_data_date": None,
                        "latest_data_date": pandas.Timestamp("2026-08-21"),
                        "data_count": 1,
                    },
                    "sh600004": {
                        "first_data_date": None,
                        "latest_data_date": pandas.Timestamp("2026-08-20"),
                        "data_count": 1,
                    },
                },
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.bulk_upsert_asset_status",
                return_value=2,
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, missing.code):
                processor.check_data_integrity(
                    obj_type="stock",
                    local_data_list=DummyQuerySet([present, missing]),
                    remote_data_df=remote,
                    hist_handler="get_hist_stock_quote_data",
                    allow_update=True,
                )

    def test_good_quote_recalculates_stored_freshness(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock", trade_calendar=[])
        processor.most_recent_trading_day = pandas.Timestamp("2026-08-21")
        processor.perform_date_check = lambda: None
        processor.check_data_freshness = lambda stock, **kwargs: "GOOD"
        processor.perform_stock_name_check = Mock()
        processor.update_active_status = Mock()
        stock = self._stock("sh600000")
        remote = pandas.DataFrame(
            [{"code": stock.code, "name": stock.name, "close": 1}]
        )

        from app.model.stock import StockDailyQuote as _QuoteModel

        with (
            patch(
                "app.lib.datahub.processors.china_a_stock.progress_bar",
                return_value=lambda *args: None,
            ),
            patch.object(_QuoteModel, "_get_collection", return_value=Mock()),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
                return_value={},
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.aggregate_stats_by_code",
                return_value={
                    "sh600000": {
                        "first_data_date": None,
                        "latest_data_date": pandas.Timestamp("2026-08-21"),
                        "data_count": 1,
                    }
                },
            ) as aggregate,
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.bulk_upsert_asset_status",
                return_value=1,
            ) as bulk_upsert,
        ):
            processor.check_data_integrity(
                obj_type="stock",
                local_data_list=DummyQuerySet([stock]),
                remote_data_df=remote,
                hist_handler="get_hist_stock_quote_data",
                allow_update=True,
            )

        # GOOD 股票仍然批量重算 freshness 并 upsert
        aggregate.assert_called_once()
        bulk_upsert.assert_called_once()
        records = bulk_upsert.call_args[0][0]
        self.assertEqual([record["code"] for record in records], ["sh600000"])
        self.assertEqual(records[0]["status"], "OK")

    def test_suspended_good_stock_with_stale_refresh_is_tolerated(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock", trade_calendar=[])
        processor.most_recent_trading_day = pandas.Timestamp("2026-08-21")
        processor.perform_date_check = lambda: None
        processor.check_data_freshness = lambda stock, **kwargs: "GOOD"
        processor.perform_stock_name_check = Mock()
        processor.update_active_status = Mock()
        stock = self._stock("sh600000")
        remote = pandas.DataFrame(
            [{"code": stock.code, "name": stock.name, "close": 0}]
        )

        from app.model.stock import StockDailyQuote as _QuoteModel

        with (
            patch(
                "app.lib.datahub.processors.china_a_stock.progress_bar",
                return_value=lambda *args: None,
            ),
            patch.object(_QuoteModel, "_get_collection", return_value=Mock()),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
                return_value={},
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.aggregate_stats_by_code",
                return_value={
                    "sh600000": {
                        "first_data_date": None,
                        "latest_data_date": pandas.Timestamp("2026-08-20"),
                        "data_count": 1,
                    }
                },
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.bulk_upsert_asset_status",
                return_value=1,
            ) as bulk_upsert,
        ):
            # 停牌股票批量重算出 STALE 属于停牌豁免场景，不应导致 phase 失败
            processor.check_data_integrity(
                obj_type="stock",
                local_data_list=DummyQuerySet([stock]),
                remote_data_df=remote,
                hist_handler="get_hist_stock_quote_data",
                allow_update=True,
            )

        bulk_upsert.assert_called_once()
        records = bulk_upsert.call_args[0][0]
        self.assertEqual(records[0]["status"], "STALE")

    def test_none_history_result_is_reported_as_failure_and_no_data(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock", trade_calendar=[])
        stock = self._stock("sh600000")
        query = Mock()
        query.only.return_value.first.return_value = stock
        stock_model = SimpleNamespace(objects=Mock(return_value=query))

        with (
            patch(
                "app.lib.datahub.processors.china_a_stock.IndividualStock",
                stock_model,
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.zh_a_daily.get_zh_a_stock_hist_daily_quote",
                return_value=None,
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.refresh_quote_status",
                return_value={"status": "NO_DATA"},
            ) as refresh,
        ):
            result = processor.get_hist_stock_quote_data(
                stock.code, end_date="2026-08-21"
            )

        self.assertEqual(result["code"], "FAIL")
        self.assertEqual(result["freshness_status"], "NO_DATA")
        refresh.assert_called_once()

    def test_empty_stock_bootstrap_accumulates_written_quotes(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock")
        processor.perform_date_check = lambda: None
        processor.handle_new_stock = lambda **kwargs: {
            "code": "GOOD",
            "written_count": 3,
            "validated_count": 3,
            "freshness_status": "OK",
        }
        remote = pandas.DataFrame(
            [{"code": "sh600519", "name": "Kweichow Moutai", "close": 1}]
        )

        with patch(
            "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
            return_value={},
        ):
            result = processor.check_data_integrity(
                obj_type="stock",
                local_data_list=EmptyQuerySet(),
                remote_data_df=remote,
                hist_handler="get_hist_stock_quote_data",
                allow_update=True,
            )

        self.assertEqual(result["written_count"], 3)

    def test_stock_bootstrap_fails_when_every_quote_write_is_zero(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock")
        processor.perform_date_check = lambda: None
        processor.handle_new_stock = lambda **kwargs: {
            "code": "FAIL",
            "written_count": 0,
            "validated_count": 0,
            "freshness_status": "NO_DATA",
        }
        remote = pandas.DataFrame(
            [{"code": "sh600519", "name": "Kweichow Moutai", "close": 1}]
        )

        with (
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
                return_value={},
            ),
            self.assertRaisesRegex(RuntimeError, "wrote zero quote rows"),
        ):
            processor.check_data_integrity(
                obj_type="stock",
                local_data_list=EmptyQuerySet(),
                remote_data_df=remote,
                hist_handler="get_hist_stock_quote_data",
                allow_update=True,
            )

    def test_empty_stock_spot_response_fails_bootstrap(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock")
        processor.perform_date_check = lambda: None

        with self.assertRaisesRegex(RuntimeError, "spot list is empty"):
            processor.check_data_integrity(
                obj_type="stock",
                local_data_list=EmptyQuerySet(),
                remote_data_df=pandas.DataFrame(),
                hist_handler="get_hist_stock_quote_data",
                allow_update=True,
            )

    def test_zero_close_stock_is_not_marked_inactive_or_failed_when_suspended(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock", trade_calendar=[])
        processor.most_recent_trading_day = pandas.Timestamp("2026-08-21")
        processor.perform_date_check = lambda: None
        processor.check_data_freshness = lambda stock, **kwargs: "INC"
        processor.perform_stock_name_check = Mock()
        processor.get_hist_quote_data = Mock(
            return_value={
                "code": "GOOD",
                "written_count": 0,
                "validated_count": 0,
                "freshness_status": "STALE",
            }
        )
        stock = self._stock("sh600000")
        remote = pandas.DataFrame(
            [{"code": stock.code, "name": stock.name, "close": 0}]
        )

        with (
            patch(
                "app.lib.datahub.processors.china_a_stock.progress_bar",
                return_value=lambda *args: None,
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
                return_value={},
            ),
        ):
            result = processor.check_data_integrity(
                obj_type="stock",
                local_data_list=DummyQuerySet([stock]),
                remote_data_df=remote,
                hist_handler="get_hist_stock_quote_data",
                allow_update=True,
            )

        self.assertEqual(stock.active_status, 0)
        self.assertEqual(result["written_count"], 0)

    def test_suspended_stock_with_failed_stale_pull_is_tolerated(self):
        # 停牌股的历史拉取在整段停牌窗口返回空 -> FAIL + STALE；
        # 悬挂容忍不再要求 code==GOOD，仅凭 停牌+STALE 即放行
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock", trade_calendar=[])
        processor.most_recent_trading_day = pandas.Timestamp("2026-08-21")
        processor.perform_date_check = lambda: None
        processor.check_data_freshness = lambda stock, **kwargs: "UPD"
        processor.perform_stock_name_check = Mock()
        processor.get_hist_quote_data = Mock(
            return_value={
                "code": "FAIL",
                "written_count": 0,
                "validated_count": 0,
                "freshness_status": "STALE",
            }
        )
        stock = self._stock("sh600000")
        remote = pandas.DataFrame(
            [{"code": stock.code, "name": stock.name, "close": 0}]
        )

        with (
            patch(
                "app.lib.datahub.processors.china_a_stock.progress_bar",
                return_value=lambda *args: None,
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
                return_value={},
            ),
        ):
            result = processor.check_data_integrity(
                obj_type="stock",
                local_data_list=DummyQuerySet([stock]),
                remote_data_df=remote,
                hist_handler="get_hist_stock_quote_data",
                allow_update=True,
            )

        # 走历史路径、被容忍、phase 不失败
        self.assertEqual(processor.get_hist_quote_data.call_count, 1)
        self.assertEqual(result["written_count"], 0)

    def test_new_suspended_stock_with_stale_history_does_not_fail_bootstrap(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock")
        processor.perform_date_check = lambda: None
        processor.handle_new_stock = lambda **kwargs: {
            "code": "GOOD",
            "written_count": 3,
            "validated_count": 3,
            "freshness_status": "STALE",
        }
        remote = pandas.DataFrame(
            [{"code": "sh600000", "name": "Suspended", "close": 0}]
        )

        with patch(
            "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
            return_value={},
        ):
            result = processor.check_data_integrity(
                obj_type="stock",
                local_data_list=EmptyQuerySet(),
                remote_data_df=remote,
                hist_handler="get_hist_stock_quote_data",
                allow_update=True,
            )

        self.assertEqual(result["written_count"], 3)

    def test_new_suspended_stock_without_history_fails_bootstrap(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock")
        processor.perform_date_check = lambda: None
        processor.handle_new_stock = lambda **kwargs: {
            "code": "FAIL",
            "written_count": 0,
            "validated_count": 0,
            "freshness_status": "NO_DATA",
        }
        remote = pandas.DataFrame(
            [{"code": "sh600000", "name": "Suspended", "close": 0}]
        )

        with (
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
                return_value={},
            ),
            self.assertRaisesRegex(RuntimeError, "sh600000"),
        ):
            processor.check_data_integrity(
                obj_type="stock",
                local_data_list=EmptyQuerySet(),
                remote_data_df=remote,
                hist_handler="get_hist_stock_quote_data",
                allow_update=True,
            )

    def test_zero_close_index_does_not_receive_stock_suspension_exemption(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock", trade_calendar=[])
        processor.most_recent_trading_day = pandas.Timestamp("2026-08-21")
        processor.perform_date_check = lambda: None
        processor.check_data_freshness = lambda stock, **kwargs: "FULL"
        processor.perform_stock_name_check = Mock()
        processor.get_hist_quote_data = Mock(
            return_value={
                "code": "FAIL",
                "written_count": 0,
                "validated_count": 0,
                "freshness_status": "NO_DATA",
            }
        )
        index = self._stock("sh000001")
        remote = pandas.DataFrame(
            [{"code": index.code, "name": index.name, "close": 0}]
        )

        with (
            patch(
                "app.lib.datahub.processors.china_a_stock.progress_bar",
                return_value=lambda *args: None,
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
                return_value={},
            ),
            self.assertRaisesRegex(RuntimeError, index.code),
        ):
            processor.check_data_integrity(
                obj_type="index",
                local_data_list=DummyQuerySet([index]),
                remote_data_df=remote,
                hist_handler="get_hist_index_quote_data",
                allow_update=True,
            )

    def test_symbol_loop_exception_preserves_prior_writes_in_job_summary(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market_name = "ChinaAStock"
        processor.market = SimpleNamespace(name="ChinaAStock", trade_calendar=[])
        processor.most_recent_trading_day = pandas.Timestamp("2026-08-21")
        processor.last_job_summary = None
        processor._partial_phase_result = None
        processor.perform_date_check = lambda: None
        processor.check_data_freshness = lambda stock, **kwargs: "FULL"
        processor.perform_stock_name_check = Mock()
        processor.get_hist_quote_data = Mock(
            side_effect=[
                {
                    "code": "GOOD",
                    "written_count": 3,
                    "validated_count": 1,
                    "freshness_status": "OK",
                },
                RuntimeError("source failed"),
            ]
        )
        stocks = DummyQuerySet([self._stock("sh600000"), self._stock("sh600004")])
        remote = pandas.DataFrame(
            [{"code": stock.code, "name": stock.name, "close": 1} for stock in stocks]
        )

        def run_phase(allow_update=False):
            return processor.check_data_integrity(
                obj_type="stock",
                local_data_list=stocks,
                remote_data_df=remote,
                hist_handler="get_hist_stock_quote_data",
                allow_update=True,
            )

        with (
            patch(
                "app.lib.datahub.processors.china_a_stock.progress_bar",
                return_value=lambda *args: None,
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
                return_value={},
            ),
            self.assertRaisesRegex(RuntimeError, "source failed"),
        ):
            processor._run_job("stock_quote_sync", [("quotes", run_phase)])

        self.assertEqual(processor.last_job_summary["written_total"], 3)
        self.assertEqual(processor.last_job_summary["validated_total"], 1)

    def test_consecutive_history_failures_trip_circuit_breaker(self):
        from app.lib.datahub.processors.china_a_stock import (
            HISTORY_FAILURE_CIRCUIT_LIMIT,
            ChinaAStock,
        )

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock", trade_calendar=[])
        processor.most_recent_trading_day = pandas.Timestamp("2026-08-21")
        processor.perform_date_check = lambda: None
        processor.check_data_freshness = lambda stock, **kwargs: "INC"
        processor.perform_stock_name_check = Mock()
        processor.update_active_status = Mock()
        processor.get_hist_quote_data = Mock(
            return_value={
                "code": "FAIL",
                "written_count": 0,
                "validated_count": 0,
                "freshness_status": "NO_DATA",
            }
        )
        stocks = DummyQuerySet(
            [
                self._stock(f"sh6{index:05d}")
                for index in range(HISTORY_FAILURE_CIRCUIT_LIMIT + 5)
            ]
        )
        remote = pandas.DataFrame(
            [{"code": stock.code, "name": stock.name, "close": 1} for stock in stocks]
        )

        with (
            patch(
                "app.lib.datahub.processors.china_a_stock.progress_bar",
                return_value=lambda *args: None,
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.data_asset_status_helper.read_quote_status_map",
                return_value={},
            ),
        ):
            with self.assertRaisesRegex(
                RuntimeError, "history source appears unavailable"
            ):
                processor.check_data_integrity(
                    obj_type="stock",
                    local_data_list=stocks,
                    remote_data_df=remote,
                    hist_handler="get_hist_stock_quote_data",
                    allow_update=True,
                )

        # 熔断提前中止：不再遍历剩余股票
        self.assertLess(
            processor.get_hist_quote_data.call_count,
            len(stocks),
        )

    def test_failed_phase_preserves_partial_quote_stats(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market_name = "ChinaAStock"
        processor.most_recent_trading_day = pandas.Timestamp("2026-08-21")
        processor.last_job_summary = None
        processor._partial_phase_result = None

        def fail_after_writes(allow_update=False):
            processor._partial_phase_result = {
                "pulled_count": 2,
                "written_count": 7,
                "validated_count": 2,
            }
            raise RuntimeError("validation failed")

        with self.assertRaisesRegex(RuntimeError, "validation failed"):
            processor._run_job("stock_quote_sync", [("quotes", fail_after_writes)])

        self.assertEqual(processor.last_job_summary["written_total"], 7)
        self.assertEqual(
            processor.last_job_summary["phase_stats"]["quotes"]["validated_count"],
            2,
        )

    def test_duplicate_only_bulk_write_error_is_detected(self):
        from app.lib.utilities.mongo_error_helper import (
            is_duplicate_only_bulk_write_error,
        )

        error = DummyBulkWriteError(
            {
                "writeErrors": [
                    {"code": 11000, "errmsg": "dup"},
                    {"code": 11000, "errmsg": "dup"},
                ]
            }
        )

        self.assertTrue(is_duplicate_only_bulk_write_error(error))

    def test_mixed_bulk_write_error_is_not_treated_as_duplicate_only(self):
        from app.lib.utilities.mongo_error_helper import (
            is_duplicate_only_bulk_write_error,
        )

        error = DummyBulkWriteError(
            {
                "writeErrors": [
                    {"code": 11000, "errmsg": "dup"},
                    {"code": 50, "errmsg": "other"},
                ]
            }
        )

        self.assertFalse(is_duplicate_only_bulk_write_error(error))

    def test_mongoengine_style_bulk_write_error_is_detected(self):
        from app.lib.utilities.mongo_error_helper import (
            is_duplicate_only_bulk_write_error,
        )

        error = DummyMongoEngineBulkWriteError(
            {
                "writeErrors": [
                    {"code": 11000, "errmsg": "dup"},
                    {"code": 11000, "errmsg": "dup"},
                ]
            }
        )

        self.assertTrue(is_duplicate_only_bulk_write_error(error))

    def test_handle_new_stock_reuses_existing_master_on_duplicate_save(self):
        """Concurrent quote runs must not crash with NotUniqueError when a
        second run tries to create master data that already exists."""
        from app.lib.datahub.processors.china_a_stock import ChinaAStock
        from mongoengine.errors import NotUniqueError

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(name="ChinaAStock")
        processor.most_recent_trading_day = datetime.datetime(2026, 8, 24)
        processor.get_hist_stock_quote_data = Mock(
            return_value={
                "code": "GOOD",
                "written_count": 3,
                "validated_count": 3,
                "freshness_status": "OK",
            }
        )

        reuse_info = {"objects_calls": 0}

        class FakeIndividualStock:
            instances = []

            def __init__(self):
                self.code = None
                self.name = None
                self.object_type = None
                self.market = None
                self.data_capabilities = None
                FakeIndividualStock.instances.append(self)

            def save(self):
                if len(FakeIndividualStock.instances) > 1:
                    raise NotUniqueError("Tried to save duplicate unique keys (E11000)")

            @classmethod
            def objects(cls, code=None):
                reuse_info["objects_calls"] += 1
                return SimpleNamespace(first=lambda: FakeIndividualStock.instances[0])

        with patch(
            "app.lib.datahub.processors.china_a_stock.IndividualStock",
            FakeIndividualStock,
        ):
            first = processor.handle_new_stock("stock", "sh600000", "浦发银行")
            second = processor.handle_new_stock("stock", "sh600000", "浦发银行")

        self.assertEqual(first["written_count"], 3)
        self.assertEqual(second["written_count"], 3)
        # The duplicate-save branch must have reused the existing master
        # record (objects() is only called from the NotUniqueError path).
        self.assertGreaterEqual(reuse_info["objects_calls"], 1)

    def test_stock_universe_snapshot_uses_resolved_trading_day(self):
        from app.lib.datahub.processors.china_a_stock import ChinaAStock

        processor = object.__new__(ChinaAStock)
        processor.market = SimpleNamespace(
            name="ChinaAStock",
            trade_calendar=[
                datetime.datetime(2026, 8, 24),
                datetime.datetime(2026, 8, 25),
            ],
        )
        processor.today = datetime.date(2026, 8, 25)
        processor.run_started_at = datetime.datetime(
            2026, 8, 25, 18, 0, tzinfo=ZoneInfo("Asia/Shanghai")
        )
        processor.explicit_as_of_date = False
        processor.most_recent_trading_day = None
        processor.check_data_integrity = Mock()

        with (
            patch(
                "app.lib.datahub.processors.china_a_stock.IndividualStock",
                Mock(),
            ),
            patch(
                "app.lib.datahub.processors.china_a_stock.zh_a_daily.get_zh_a_stock_universe",
                return_value=pandas.DataFrame(),
            ) as universe,
            patch(
                "app.lib.datahub.processors.china_a_stock.zh_a_daily.stock_history_uses_baostock",
                return_value=False,
            ),
        ):
            processor.check_stock_data_integrity()

        # perform_date_check must resolve the frozen trading day BEFORE the
        # universe snapshot so default runs snapshot the right date.
        self.assertEqual(universe.call_args.kwargs["as_of_date"], "2026-08-25")

    def test_stringified_mongoengine_bulk_write_error_is_detected(self):
        from app.lib.utilities.mongo_error_helper import (
            is_duplicate_only_bulk_write_error,
        )

        error = DummyMongoEngineBulkWriteError(
            "Bulk write error: ({'writeErrors': [{'index': 0, 'code': 11000, "
            "'errmsg': 'dup'}], 'writeConcernErrors': []})"
        )

        self.assertTrue(is_duplicate_only_bulk_write_error(error))

    def test_stringified_bulk_write_error_with_non_duplicate_code_is_rejected(self):
        from app.lib.utilities.mongo_error_helper import (
            is_duplicate_only_bulk_write_error,
        )

        error = DummyMongoEngineBulkWriteError(
            "Bulk write error: ({'writeErrors': [{'index': 0, 'code': 11000}, "
            "{'index': 1, 'code': 50}], 'writeConcernErrors': []})"
        )

        self.assertFalse(is_duplicate_only_bulk_write_error(error))
