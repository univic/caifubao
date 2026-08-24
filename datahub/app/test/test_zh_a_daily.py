from __future__ import annotations

import datetime
import os
from unittest import TestCase
from unittest.mock import patch

import pandas

from app.lib.datahub.data_source.handler import zh_a_daily


class TestStockHistorySource(TestCase):
    def test_akshare_history_is_normalized_to_quote_schema(self):
        raw = pandas.DataFrame(
            [
                {
                    "日期": datetime.date(2026, 8, 21),
                    "股票代码": "600519",
                    "开盘": 1291.5,
                    "收盘": 1272.83,
                    "最高": 1291.5,
                    "最低": 1272.01,
                    "成交量": 33472,
                    "成交额": 4278311022.0,
                    "涨跌幅": -1.45,
                    "涨跌额": -18.67,
                    "换手率": 0.27,
                }
            ]
        )

        with patch.dict(os.environ, {zh_a_daily.STOCK_HISTORY_SOURCE_ENV: "akshare"}):
            with patch.object(
                zh_a_daily.interface.akshare_interface,
                "stock_zh_a_hist",
                return_value=raw,
            ) as fetch:
                result = zh_a_daily.get_zh_a_stock_hist_daily_quote(
                    "sh600519", start_date="2026-08-20"
                )

        fetch.assert_called_once_with(
            "600519",
            start_date="20260820",
            end_date=datetime.date.today().strftime("%Y%m%d"),
        )
        self.assertEqual(result.iloc[0]["code"], "sh600519")
        self.assertEqual(result.iloc[0]["previous_close"], 1291.5)
        self.assertEqual(result.iloc[0]["trade_status"], 1)
        self.assertEqual(result.iloc[0]["peTTM"], 0.0)
        self.assertTrue(pandas.api.types.is_datetime64_any_dtype(result["date"]))

    def test_baostock_remains_available_as_explicit_source(self):
        raw = pandas.DataFrame(
            [
                {
                    "date": "2026-08-21",
                    "code": "sh.600519",
                    "open": "1291.5",
                    "high": "1291.5",
                    "low": "1272.01",
                    "close": "1272.83",
                    "preclose": "1291.5",
                    "volume": "33472",
                    "amount": "4278311022",
                    "adjustflag": "3",
                    "turn": "0.27",
                    "tradestatus": "1",
                    "pctChg": "-1.45",
                    "peTTM": "20",
                    "pbMRQ": "8",
                    "psTTM": "10",
                    "pcfNcfTTM": "15",
                    "isST": "0",
                }
            ]
        )

        with patch.dict(os.environ, {zh_a_daily.STOCK_HISTORY_SOURCE_ENV: "baostock"}):
            with patch.object(
                zh_a_daily.BaostockInterfaceManager,
                "get_zh_a_stock_hist_k_data",
                return_value=raw,
            ):
                result = zh_a_daily.get_zh_a_stock_hist_daily_quote("sh600519")

        self.assertEqual(result.iloc[0]["code"], "sh600519")
        self.assertEqual(result.iloc[0]["previous_close"], 1291.5)

    def test_invalid_stock_history_source_is_rejected(self):
        with patch.dict(os.environ, {zh_a_daily.STOCK_HISTORY_SOURCE_ENV: "invalid"}):
            with self.assertRaisesRegex(ValueError, "DATAHUB_STOCK_HISTORY_SOURCE"):
                zh_a_daily.get_stock_history_source()
