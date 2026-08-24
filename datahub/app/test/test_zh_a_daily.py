from __future__ import annotations

import datetime
import os
from unittest import TestCase
from unittest.mock import patch

import pandas

from app.lib.datahub.data_source.handler import zh_a_daily


class TestRetryPolicy(TestCase):
    def test_json_decode_error_is_retryable(self):
        class FakeJSONDecodeError(Exception):
            pass

        error = FakeJSONDecodeError("Can not decode value starting with character '<'")
        self.assertTrue(zh_a_daily._is_retryable_market_data_error(error))

    def test_stdlib_style_json_decode_error_class_is_retryable(self):
        # akshare 1.18.94 surfaces anti-bot HTML as stdlib json.JSONDecodeError
        # ("Expecting value: ...") whose class name is what the predicate keys on.
        class JSONDecodeError(Exception):
            pass

        error = JSONDecodeError("Expecting value: line 1 column 1 (char 0)")
        self.assertTrue(zh_a_daily._is_retryable_market_data_error(error))

    def test_network_error_is_retryable_and_value_error_is_not(self):
        from requests.exceptions import ConnectionError as RequestsConnectionError

        self.assertTrue(
            zh_a_daily._is_retryable_market_data_error(
                RequestsConnectionError("Connection aborted.")
            )
        )
        self.assertFalse(
            zh_a_daily._is_retryable_market_data_error(ValueError("bad data"))
        )

    def test_call_with_retry_retries_decode_error_then_succeeds(self):
        class FakeJSONDecodeError(Exception):
            pass

        error = FakeJSONDecodeError("Can not decode value starting with character '<'")
        calls = {"n": 0}

        def fetcher():
            calls["n"] += 1
            if calls["n"] < 3:
                raise error
            return "ok"

        result = zh_a_daily._call_with_retry(fetcher, label="test", base_delay=0)

        self.assertEqual(result, "ok")
        self.assertEqual(calls["n"], 3)


class TestStockHistorySource(TestCase):
    def test_index_increment_start_date_is_inclusive(self):
        raw = pandas.DataFrame(
            {
                "date": [
                    datetime.date(2026, 8, 21),
                    datetime.date(2026, 8, 24),
                ],
                "close": [1, 2],
            }
        )

        with patch.object(
            zh_a_daily.interface.akshare_interface,
            "stock_zh_index_daily",
            return_value=raw,
        ):
            result = zh_a_daily.get_zh_a_index_hist_daily_quote(
                "sh000001",
                start_date="2026-08-24",
                end_date="2026-08-24",
            )

        self.assertEqual(result["date"].tolist(), [datetime.date(2026, 8, 24)])

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

    def test_tushare_history_is_normalized_to_quote_schema(self):
        raw = pandas.DataFrame(
            [
                {
                    "ts_code": "600519.SH",
                    "trade_date": "20260824",
                    "open": 1271.01,
                    "high": 1313.8,
                    "low": 1270.33,
                    "close": 1304.66,
                    "pre_close": 1272.83,
                    "change": 31.83,
                    "pct_chg": 2.5,
                    "vol": 4844000,
                    "amount": 6299794.0,
                },
                {
                    "ts_code": "600519.SH",
                    "trade_date": "20260821",
                    "open": 1291.5,
                    "high": 1291.5,
                    "low": 1272.01,
                    "close": 1272.83,
                    "pre_close": 1291.5,
                    "change": -18.67,
                    "pct_chg": -1.45,
                    "vol": 3347200,
                    "amount": 4278311.0,
                },
            ]
        )

        with patch.dict(os.environ, {zh_a_daily.STOCK_HISTORY_SOURCE_ENV: "tushare"}):
            with patch.object(
                zh_a_daily.interface.tushare_interface,
                "tushare_daily",
                return_value=raw,
            ) as fetch:
                result = zh_a_daily.get_zh_a_stock_hist_daily_quote(
                    "sh600519", start_date="2026-08-20", end_date="2026-08-24"
                )

        fetch.assert_called_once_with(
            "600519.SH",
            start_date="20260820",
            end_date="20260824",
        )
        # tushare 返回降序 -> 归一化后升序；end_date 含 2026-08-24，两行都保留
        self.assertEqual(
            [str(d)[:10] for d in result["date"].tolist()],
            ["2026-08-21", "2026-08-24"],
        )
        self.assertEqual(result.iloc[0]["code"], "sh600519")
        self.assertEqual(result.iloc[0]["previous_close"], 1291.5)
        self.assertEqual(result.iloc[0]["change_amount"], -18.67)
        self.assertEqual(result.iloc[0]["change_rate"], -1.45)
        self.assertEqual(result.iloc[0]["volume"], 3347200)
        # amount 千元 -> 元
        self.assertEqual(result.iloc[0]["trade_amount"], 4278311000.0)
        self.assertEqual(result.iloc[0]["trade_status"], 1)
        self.assertTrue(pandas.api.types.is_datetime64_any_dtype(result["date"]))

    def test_tushare_empty_history_returns_none(self):
        with patch.dict(os.environ, {zh_a_daily.STOCK_HISTORY_SOURCE_ENV: "tushare"}):
            with patch.object(
                zh_a_daily.interface.tushare_interface,
                "tushare_daily",
                return_value=pandas.DataFrame(),
            ):
                result = zh_a_daily.get_zh_a_stock_hist_daily_quote(
                    "sh600519", start_date="2026-08-20", end_date="2026-08-24"
                )
        self.assertIsNone(result)

    def test_tushare_missing_required_column_raises(self):
        raw = pandas.DataFrame(
            [
                {
                    "ts_code": "600519.SH",
                    "trade_date": "20260821",
                    "open": 1291.5,
                    "high": 1291.5,
                    "low": 1272.01,
                    "close": 1272.83,
                    "pre_close": 1291.5,
                    "change": -18.67,
                    "pct_chg": -1.45,
                    "vol": 3347200,
                    "amount": 4278311.0,
                }
            ]
        )
        del raw["high"]
        with patch.dict(os.environ, {zh_a_daily.STOCK_HISTORY_SOURCE_ENV: "tushare"}):
            with patch.object(
                zh_a_daily.interface.tushare_interface,
                "tushare_daily",
                return_value=raw,
            ):
                with self.assertRaisesRegex(ValueError, "required columns"):
                    zh_a_daily.get_zh_a_stock_hist_daily_quote(
                        "sh600519", start_date="2026-08-20", end_date="2026-08-24"
                    )

    def test_tushare_branch_retries_transient_error(self):
        from requests.exceptions import ConnectionError as RequestsConnectionError

        raw = pandas.DataFrame(
            [
                {
                    "ts_code": "600519.SH",
                    "trade_date": "20260821",
                    "open": 1291.5,
                    "high": 1291.5,
                    "low": 1272.01,
                    "close": 1272.83,
                    "pre_close": 1291.5,
                    "change": -18.67,
                    "pct_chg": -1.45,
                    "vol": 3347200,
                    "amount": 4278311.0,
                }
            ]
        )
        calls = {"n": 0}

        def flaky_fetch(ts_code, start_date=None, end_date=None):
            calls["n"] += 1
            if calls["n"] < 2:
                raise RequestsConnectionError("Connection aborted.")
            return raw

        with patch.dict(os.environ, {zh_a_daily.STOCK_HISTORY_SOURCE_ENV: "tushare"}):
            with patch.object(
                zh_a_daily.interface.tushare_interface,
                "tushare_daily",
                side_effect=flaky_fetch,
            ):
                result = zh_a_daily.get_zh_a_stock_hist_daily_quote(
                    "sh600519", start_date="2026-08-20", end_date="2026-08-24"
                )
        self.assertEqual(calls["n"], 2)
        self.assertIsNotNone(result)

    def test_tushare_daily_paginates_by_year_windows(self):

        from app.lib.datahub.data_source.interface import tushare_interface

        windows = []

        def fake_pro():
            class FakePro:
                def daily(self, ts_code, start_date, end_date):
                    windows.append((start_date, end_date))
                    year = int(start_date[:4])
                    return pandas.DataFrame(
                        [
                            {
                                "ts_code": ts_code,
                                "trade_date": f"{year}0102",
                                "open": 1.0,
                                "high": 1.0,
                                "low": 1.0,
                                "close": 1.0,
                                "pre_close": 1.0,
                                "change": 0.0,
                                "pct_chg": 0.0,
                                "vol": 100,
                                "amount": 1000.0,
                            }
                        ]
                    )

            return FakePro()

        with patch.object(tushare_interface, "_get_pro", side_effect=fake_pro):
            df = tushare_interface.tushare_daily(
                "000001.SZ", start_date="19900101", end_date="20260824"
            )

        # 18 年窗口：1990-2007, 2008-2025, 2026
        self.assertEqual(
            windows,
            [
                ("19900101", "20071231"),
                ("20080101", "20251231"),
                ("20260101", "20260824"),
            ],
        )
        self.assertEqual(len(df), 3)

    def test_tushare_source_is_accepted(self):
        with patch.dict(os.environ, {zh_a_daily.STOCK_HISTORY_SOURCE_ENV: "tushare"}):
            self.assertEqual(zh_a_daily.get_stock_history_source(), "tushare")
        self.assertIn("tushare", zh_a_daily.SUPPORTED_STOCK_HISTORY_SOURCES)

    def test_to_tushare_ts_code_mapping(self):
        convert = zh_a_daily.interface.tushare_interface.to_tushare_ts_code
        self.assertEqual(convert("sh600519"), "600519.SH")
        self.assertEqual(convert("sz000977"), "000977.SZ")
        self.assertEqual(convert("bj920000"), "920000.BJ")

    def test_to_tushare_ts_code_unknown_prefix_raises(self):
        convert = zh_a_daily.interface.tushare_interface.to_tushare_ts_code
        with self.assertRaises(ValueError):
            convert("000977")

    def test_tushare_missing_token_fails_clearly(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("TUSHARE_TOKEN", None)
            with self.assertRaisesRegex(RuntimeError, "TUSHARE_TOKEN"):
                zh_a_daily.interface.tushare_interface.tushare_daily("600519.SH")

    def test_explicit_end_date_is_shared_with_baostock(self):
        raw = pandas.DataFrame(
            [
                {
                    "date": "2026-08-21",
                    "code": "sh.600519",
                    "open": "1",
                    "high": "1",
                    "low": "1",
                    "close": "1",
                    "preclose": "1",
                    "volume": "1",
                    "amount": "1",
                    "adjustflag": "3",
                    "turn": "1",
                    "tradestatus": "1",
                    "pctChg": "0",
                    "peTTM": "1",
                    "pbMRQ": "1",
                    "psTTM": "1",
                    "pcfNcfTTM": "1",
                    "isST": "0",
                }
            ]
        )

        with patch.dict(os.environ, {zh_a_daily.STOCK_HISTORY_SOURCE_ENV: "baostock"}):
            with patch.object(
                zh_a_daily.BaostockInterfaceManager,
                "get_zh_a_stock_hist_k_data",
                return_value=raw,
            ) as fetch:
                result = zh_a_daily.get_zh_a_stock_hist_daily_quote(
                    "sh600519", start_date="2026-08-20", end_date="2026-08-21"
                )

        fetch.assert_called_once_with("sh600519", "2026-08-20", "2026-08-21")
        self.assertEqual(result["date"].max(), pandas.Timestamp("2026-08-21"))

    def test_explicit_end_date_filters_akshare_response(self):
        raw = pandas.DataFrame(
            [
                {
                    "日期": datetime.date(2026, 8, day),
                    "股票代码": "600519",
                    "开盘": 1,
                    "收盘": 1,
                    "最高": 1,
                    "最低": 1,
                    "成交量": 1,
                    "成交额": 1,
                    "涨跌幅": 0,
                    "涨跌额": 0,
                    "换手率": 1,
                }
                for day in (21, 24)
            ]
        )

        with patch.dict(os.environ, {zh_a_daily.STOCK_HISTORY_SOURCE_ENV: "akshare"}):
            with patch.object(
                zh_a_daily.interface.akshare_interface,
                "stock_zh_a_hist",
                return_value=raw,
            ) as fetch:
                result = zh_a_daily.get_zh_a_stock_hist_daily_quote(
                    "sh600519", end_date="2026-08-21"
                )

        fetch.assert_called_once_with("600519", start_date=None, end_date="20260821")
        self.assertEqual(result["date"].tolist(), [pandas.Timestamp("2026-08-21")])

    def test_invalid_stock_history_source_is_rejected(self):
        with patch.dict(os.environ, {zh_a_daily.STOCK_HISTORY_SOURCE_ENV: "invalid"}):
            with self.assertRaisesRegex(ValueError, "DATAHUB_STOCK_HISTORY_SOURCE"):
                zh_a_daily.get_stock_history_source()
