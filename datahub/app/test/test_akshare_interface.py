"""Tests for akshare data source interface."""

from __future__ import annotations

from unittest import TestCase
from unittest.mock import patch
import pandas as pd


class TestAkshareInterfaceFunctions(TestCase):
    """Test cases for akshare interface functions via direct mocking."""

    def test_required_provider_functions_are_available(self):
        from app.lib.datahub.data_source.interface import akshare_interface

        required_functions = (
            "stock_sse_deal_daily",
            "stock_sse_summary",
            "stock_sh_a_spot_em",
            "stock_zh_a_hist",
            "stock_zh_a_spot",
            "stock_zh_a_spot_em",
            "stock_zh_a_stop_em",
            "stock_zh_index_daily",
            "stock_zh_index_spot_sina",
            "tool_trade_date_hist_sina",
        )
        for function_name in required_functions:
            self.assertTrue(
                callable(getattr(akshare_interface.akshare, function_name, None)),
                function_name,
            )

    def test_get_trade_date_hist_returns_dataframe(self):
        """Test that get_trade_date_hist returns DataFrame from akshare."""
        mock_df = pd.DataFrame(
            {
                "trade_date": ["2024-01-08", "2024-01-09", "2024-01-10"],
                "date": ["2024-01-08", "2024-01-09", "2024-01-10"],
            }
        )

        with patch(
            "app.lib.datahub.data_source.interface.akshare_interface.akshare"
        ) as mock_akshare:
            mock_akshare.tool_trade_date_hist_sina.return_value = mock_df

            from app.lib.datahub.data_source.interface.akshare_interface import (
                get_trade_date_hist,
            )

            result = get_trade_date_hist()

            mock_akshare.tool_trade_date_hist_sina.assert_called_once()
            self.assertIsInstance(result, pd.DataFrame)

    def test_stock_sse_summary_returns_dataframe(self):
        """Test that stock_sse_summary returns DataFrame from akshare."""
        mock_df = pd.DataFrame({"name": ["test"], "code": ["123456"]})

        with patch(
            "app.lib.datahub.data_source.interface.akshare_interface.akshare"
        ) as mock_akshare:
            mock_akshare.stock_sse_summary.return_value = mock_df

            from app.lib.datahub.data_source.interface.akshare_interface import (
                stock_sse_summary,
            )

            result = stock_sse_summary()

            mock_akshare.stock_sse_summary.assert_called_once()
            self.assertIsInstance(result, pd.DataFrame)

    def test_zh_stock_index_spot_returns_dataframe(self):
        """Test that zh_stock_index_spot returns DataFrame from akshare."""
        mock_df = pd.DataFrame(
            {
                "指数名称": ["test_index"],
                "指数代码": ["000001"],
                "今开": [100.0],
                "最新价": [105.0],
                "最高": [108.0],
                "最低": [99.0],
                "成交量": [1000000],
            }
        )

        with patch(
            "app.lib.datahub.data_source.interface.akshare_interface.akshare"
        ) as mock_akshare:
            mock_akshare.stock_zh_index_spot_sina.return_value = mock_df

            from app.lib.datahub.data_source.interface.akshare_interface import (
                zh_stock_index_spot,
            )

            result = zh_stock_index_spot()

            mock_akshare.stock_zh_index_spot_sina.assert_called_once()
            self.assertIsInstance(result, pd.DataFrame)

    def test_stock_zh_a_spot_em_returns_dataframe(self):
        """Test that stock_zh_a_spot_em returns DataFrame from akshare."""
        mock_df = pd.DataFrame(
            {
                "代码": ["600000"],
                "名称": ["浦发银行"],
                "今开": [10.0],
                "昨收": [9.9],
                "最新价": [10.1],
            }
        )

        with patch(
            "app.lib.datahub.data_source.interface.akshare_interface.akshare"
        ) as mock_akshare:
            mock_akshare.stock_zh_a_spot_em.return_value = mock_df

            from app.lib.datahub.data_source.interface.akshare_interface import (
                stock_zh_a_spot_em,
            )

            result = stock_zh_a_spot_em()

            mock_akshare.stock_zh_a_spot_em.assert_called_once()
            self.assertIsInstance(result, pd.DataFrame)

    def test_stock_zh_a_hist_with_date_range(self):
        """Test that stock_zh_a_hist returns DataFrame with date range."""
        mock_df = pd.DataFrame(
            {
                "日期": ["2024-01-08", "2024-01-09"],
                "开盘": [10.0, 10.1],
                "收盘": [10.2, 10.3],
                "最高": [10.5, 10.6],
                "最低": [9.8, 9.9],
                "成交量": [1000000, 1100000],
            }
        )

        with patch(
            "app.lib.datahub.data_source.interface.akshare_interface.akshare"
        ) as mock_akshare:
            mock_akshare.stock_zh_a_hist.return_value = mock_df

            from app.lib.datahub.data_source.interface.akshare_interface import (
                stock_zh_a_hist,
            )

            result = stock_zh_a_hist(
                "600000", start_date="2024-01-08", end_date="2024-01-10"
            )

            mock_akshare.stock_zh_a_hist.assert_called_once()
            self.assertIsInstance(result, pd.DataFrame)

    def test_stock_zh_a_hist_without_date_range(self):
        """Test that stock_zh_a_hist returns DataFrame without date range."""
        mock_df = pd.DataFrame(
            {
                "日期": ["2024-01-08", "2024-01-09"],
                "开盘": [10.0, 10.1],
                "收盘": [10.2, 10.3],
            }
        )

        with patch(
            "app.lib.datahub.data_source.interface.akshare_interface.akshare"
        ) as mock_akshare:
            mock_akshare.stock_zh_a_hist.return_value = mock_df

            from app.lib.datahub.data_source.interface.akshare_interface import (
                stock_zh_a_hist,
            )

            result = stock_zh_a_hist("600000")

            mock_akshare.stock_zh_a_hist.assert_called_once()
            self.assertIsInstance(result, pd.DataFrame)

    def test_stock_zh_a_stop_em_returns_dataframe(self):
        """Test that stock_zh_a_stop_em returns DataFrame for suspended stocks."""
        mock_df = pd.DataFrame(
            {
                "代码": ["600001"],
                "名称": ["退市股"],
            }
        )

        with patch(
            "app.lib.datahub.data_source.interface.akshare_interface.akshare"
        ) as mock_akshare:
            mock_akshare.stock_zh_a_stop_em.return_value = mock_df

            from app.lib.datahub.data_source.interface.akshare_interface import (
                stock_zh_a_stop_em,
            )

            result = stock_zh_a_stop_em()

            mock_akshare.stock_zh_a_stop_em.assert_called_once()
            self.assertIsInstance(result, pd.DataFrame)

    def test_stock_zh_index_daily_returns_dataframe(self):
        """Test that stock_zh_index_daily returns DataFrame."""
        mock_df = pd.DataFrame(
            {
                "date": ["2024-01-08", "2024-01-09"],
                "open": [3000.0, 3010.0],
                "close": [3020.0, 3030.0],
                "high": [3040.0, 3050.0],
                "low": [2990.0, 3000.0],
                "volume": [100000000, 110000000],
            }
        )

        with patch(
            "app.lib.datahub.data_source.interface.akshare_interface.akshare"
        ) as mock_akshare:
            mock_akshare.stock_zh_index_daily.return_value = mock_df

            from app.lib.datahub.data_source.interface.akshare_interface import (
                stock_zh_index_daily,
            )

            result = stock_zh_index_daily("sh000001")

            mock_akshare.stock_zh_index_daily.assert_called_once()
            self.assertIsInstance(result, pd.DataFrame)
