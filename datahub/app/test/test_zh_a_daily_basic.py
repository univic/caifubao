from __future__ import annotations

import datetime
from unittest import TestCase
from unittest.mock import patch

import pandas

from app.lib.datahub.data_source.interface import tushare_interface
from app.lib.datahub.data_source.handler import zh_a_daily_basic


class TestDailyBasicInterface(TestCase):
    def test_daily_basic_by_trade_date_fetches_one_market_snapshot(self):
        class FakePro:
            def __init__(self):
                self.calls = []

            def daily_basic(self, **kwargs):
                self.calls.append(kwargs)
                return pandas.DataFrame(
                    [
                        {
                            "ts_code": "600000.SH",
                            "trade_date": "20260827",
                            "pe_ttm": 8.5,
                            "pb": 1.2,
                        },
                        {
                            "ts_code": "000001.SZ",
                            "trade_date": "20260827",
                            "pe_ttm": 6.0,
                            "pb": 0.9,
                        },
                    ]
                )

        pro = FakePro()
        with (
            patch.object(tushare_interface, "_get_pro", return_value=pro),
            patch.object(tushare_interface.time, "sleep") as sleep,
        ):
            result = tushare_interface.daily_basic_by_trade_date("20260827")

        self.assertEqual(pro.calls, [{"trade_date": "20260827"}])
        sleep.assert_called_once_with(0.25)
        self.assertEqual(len(result), 2)

    def test_daily_basic_by_trade_date_raises_on_empty_snapshot(self):
        class FakePro:
            def daily_basic(self, **kwargs):
                return pandas.DataFrame()

        pro = FakePro()
        with (
            patch.object(tushare_interface, "_get_pro", return_value=pro),
            patch.object(tushare_interface.time, "sleep"),
        ):
            with self.assertRaisesRegex(RuntimeError, "20260827"):
                tushare_interface.daily_basic_by_trade_date("20260827")


class TestDailyBasicNormalize(TestCase):
    def test_normalize_maps_codes_and_keeps_nan_valuations_as_none(self):
        raw = pandas.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_date": "20260827",
                    "pe_ttm": 8.5,
                    "pb": 1.2,
                    "ps_ttm": 0.7,
                    "dv_ttm": 3.1,
                    "total_mv": 2450000.0,
                    "circ_mv": 1980000.0,
                    "turnover_rate": 0.85,
                },
                # loss-making stock: tushare leaves pe_ttm blank
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20260827",
                    "pe_ttm": None,
                    "pb": 0.9,
                    "ps_ttm": 1.5,
                    "dv_ttm": None,
                    "total_mv": 120000.0,
                    "circ_mv": 120000.0,
                    "turnover_rate": 2.4,
                },
            ]
        )
        rows = zh_a_daily_basic.normalize_daily_basic(raw, "20260827")

        self.assertEqual(len(rows), 2)
        first, second = rows
        self.assertEqual(first["code"], "sh600000")
        self.assertEqual(second["code"], "sz000001")
        expected_date = datetime.datetime(2026, 8, 27)
        self.assertEqual(first["date"], expected_date)
        self.assertEqual(first["pe_ttm"], 8.5)
        self.assertEqual(first["total_mv"], 2450000.0)
        self.assertIsNone(second["pe_ttm"])
        self.assertIsNone(second["dv_ttm"])
        self.assertEqual(second["pb"], 0.9)
        self.assertEqual(second["turnover_rate"], 2.4)

    def test_normalize_missing_columns_become_none(self):
        raw = pandas.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_date": "20260827",
                    "pe_ttm": 8.5,
                }
            ]
        )
        rows = zh_a_daily_basic.normalize_daily_basic(raw, "20260827")

        self.assertEqual(len(rows), 1)
        row = rows[0]
        for column in [
            "pb",
            "ps_ttm",
            "dv_ttm",
            "total_mv",
            "circ_mv",
            "turnover_rate",
        ]:
            self.assertIsNone(row[column], f"{column} should be None when absent")

    def test_normalize_empty_frame_returns_empty_list(self):
        self.assertEqual(
            zh_a_daily_basic.normalize_daily_basic(pandas.DataFrame(), "20260827"), []
        )
        self.assertEqual(zh_a_daily_basic.normalize_daily_basic(None, "20260827"), [])

    def test_normalize_canonical_columns_only(self):
        raw = pandas.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_date": "20260827",
                    "pe_ttm": 8.5,
                    "pb": 1.2,
                    "ps_ttm": 0.7,
                    "dv_ttm": 3.1,
                    "total_mv": 2450000.0,
                    "circ_mv": 1980000.0,
                    "turnover_rate": 0.85,
                    # non-canonical columns must be dropped
                    "close": 10.0,
                    "volume_ratio": 1.1,
                }
            ]
        )
        rows = zh_a_daily_basic.normalize_daily_basic(raw, "20260827")

        self.assertEqual(
            sorted(rows[0].keys()),
            sorted(
                [
                    "code",
                    "date",
                    "pe_ttm",
                    "pb",
                    "ps_ttm",
                    "dv_ttm",
                    "total_mv",
                    "circ_mv",
                    "turnover_rate",
                ]
            ),
        )


class TestDailyBasicModel(TestCase):
    def test_model_registered(self):
        from app.model.daily_basic import StockDailyBasic

        self.assertEqual(StockDailyBasic._meta["collection"], "stock_daily_basic")
        self.assertTrue(StockDailyBasic._meta["indexes"])
