"""Tests for baostock data source interface."""

from __future__ import annotations

from unittest import TestCase
from unittest.mock import patch, MagicMock
import pandas as pd


class TestBaostockInterfaceManager(TestCase):
    """Test cases for BaostockInterfaceManager class."""

    def test_establish_baostock_conn_success(self):
        """Test successful baostock connection establishment."""
        with patch(
            "app.lib.datahub.data_source.interface.baostock_interface.bs"
        ) as mock_bs:
            mock_conn = MagicMock()
            mock_conn.error_code = "0"
            mock_conn.error_msg = ""
            mock_bs.login.return_value = mock_conn

            from app.lib.datahub.data_source.interface.baostock_interface import (
                BaostockInterfaceManager,
            )

            result = BaostockInterfaceManager.establish_baostock_conn()

            mock_bs.login.assert_called_once()
            self.assertEqual(result.error_code, "0")

    def test_establish_baostock_conn_failure(self):
        """Test failed baostock connection establishment."""
        with patch(
            "app.lib.datahub.data_source.interface.baostock_interface.bs"
        ) as mock_bs:
            mock_conn = MagicMock()
            mock_conn.error_code = "-1"
            mock_conn.error_msg = "Connection failed"
            mock_bs.login.return_value = mock_conn

            from app.lib.datahub.data_source.interface.baostock_interface import (
                BaostockInterfaceManager,
            )

            result = BaostockInterfaceManager.establish_baostock_conn()

            mock_bs.login.assert_called_once()
            self.assertEqual(result.error_code, "-1")

    def test_terminate_baostock_conn(self):
        """Test baostock connection termination."""
        with patch(
            "app.lib.datahub.data_source.interface.baostock_interface.bs"
        ) as mock_bs:
            from app.lib.datahub.data_source.interface.baostock_interface import (
                BaostockInterfaceManager,
            )

            BaostockInterfaceManager.terminate_baostock_conn()

            mock_bs.logout.assert_called_once()

    def test_get_zh_a_stock_hist_k_data_default_dates(self):
        """Test get_zh_a_stock_hist_k_data with default dates."""
        with patch(
            "app.lib.datahub.data_source.interface.baostock_interface.bs"
        ) as mock_bs:
            mock_result = MagicMock()
            mock_result.get_data.return_value = pd.DataFrame(
                {
                    "date": ["2024-01-08", "2024-01-09"],
                    "code": ["sh.600000", "sh.600000"],
                    "open": [10.0, 10.1],
                    "close": [10.2, 10.3],
                }
            )
            mock_bs.query_history_k_data_plus.return_value = mock_result

            from app.lib.datahub.data_source.interface.baostock_interface import (
                BaostockInterfaceManager,
            )

            BaostockInterfaceManager.get_zh_a_stock_hist_k_data("sh600000")

            mock_bs.query_history_k_data_plus.assert_called_once()
            call_args = mock_bs.query_history_k_data_plus.call_args
            self.assertEqual(call_args.kwargs["start_date"], "1990-01-01")
            self.assertIn("sh.600000", call_args.args[0])

    def test_get_zh_a_stock_hist_k_data_custom_dates(self):
        """Test get_zh_a_stock_hist_k_data with custom dates."""
        with patch(
            "app.lib.datahub.data_source.interface.baostock_interface.bs"
        ) as mock_bs:
            mock_result = MagicMock()
            mock_result.get_data.return_value = pd.DataFrame(
                {
                    "date": ["2024-01-08"],
                    "code": ["sh.600000"],
                    "open": [10.0],
                    "close": [10.2],
                }
            )
            mock_bs.query_history_k_data_plus.return_value = mock_result

            from app.lib.datahub.data_source.interface.baostock_interface import (
                BaostockInterfaceManager,
            )

            BaostockInterfaceManager.get_zh_a_stock_hist_k_data(
                "sh600000", start_date="2024-01-08", end_date="2024-01-10"
            )

            call_args = mock_bs.query_history_k_data_plus.call_args
            self.assertEqual(call_args.kwargs["start_date"], "2024-01-08")
            self.assertEqual(call_args.kwargs["end_date"], "2024-01-10")

    def test_get_zh_a_stock_hist_k_data_with_adjustflag(self):
        """Test get_zh_a_stock_hist_k_data with adjustflag parameter."""
        with patch(
            "app.lib.datahub.data_source.interface.baostock_interface.bs"
        ) as mock_bs:
            mock_result = MagicMock()
            mock_result.get_data.return_value = pd.DataFrame()
            mock_bs.query_history_k_data_plus.return_value = mock_result

            from app.lib.datahub.data_source.interface.baostock_interface import (
                BaostockInterfaceManager,
            )

            BaostockInterfaceManager.get_zh_a_stock_hist_k_data(
                "sh600000", adjustflag="2"
            )

            call_args = mock_bs.query_history_k_data_plus.call_args
            self.assertEqual(call_args.kwargs["adjustflag"], "2")

    def test_get_zh_a_stock_hist_k_data_result_fields(self):
        """Test that correct result fields are requested."""
        with patch(
            "app.lib.datahub.data_source.interface.baostock_interface.bs"
        ) as mock_bs:
            mock_result = MagicMock()
            mock_result.get_data.return_value = pd.DataFrame()
            mock_bs.query_history_k_data_plus.return_value = mock_result

            from app.lib.datahub.data_source.interface.baostock_interface import (
                BaostockInterfaceManager,
            )

            BaostockInterfaceManager.get_zh_a_stock_hist_k_data("sh600000")

            call_args = mock_bs.query_history_k_data_plus.call_args
            # res_fields is passed as positional argument (second arg)
            res_fields = call_args.args[1]
            # Check that key fields are included
            self.assertIn("date", res_fields)
            self.assertIn("code", res_fields)
            self.assertIn("open", res_fields)
            self.assertIn("close", res_fields)
            self.assertIn("high", res_fields)
            self.assertIn("low", res_fields)

    def test_get_zh_a_stock_hist_k_data_empty_result(self):
        """Test get_zh_a_stock_hist_k_data with empty result."""
        with patch(
            "app.lib.datahub.data_source.interface.baostock_interface.bs"
        ) as mock_bs:
            mock_result = MagicMock()
            mock_result.get_data.return_value = pd.DataFrame()
            mock_bs.query_history_k_data_plus.return_value = mock_result

            from app.lib.datahub.data_source.interface.baostock_interface import (
                BaostockInterfaceManager,
            )

            result = BaostockInterfaceManager.get_zh_a_stock_hist_k_data("sh600000")

            self.assertIsInstance(result, pd.DataFrame)
            self.assertTrue(result.empty)

    def test_get_zh_a_stock_hist_k_data_falls_back_when_get_data_uses_append(self):
        """Test manual row conversion fallback for pandas 3 compatibility."""
        with patch(
            "app.lib.datahub.data_source.interface.baostock_interface.bs"
        ) as mock_bs:
            mock_result = MagicMock()
            mock_result.get_data.side_effect = AttributeError(
                "'DataFrame' object has no attribute 'append'"
            )
            mock_result.error_code = "0"
            mock_result.fields = ["date", "code", "open", "close"]
            rows = [
                ["2024-01-08", "sh.600000", "10.0", "10.2"],
                ["2024-01-09", "sh.600000", "10.1", "10.3"],
            ]
            state = {"idx": -1}

            def next_side_effect():
                state["idx"] += 1
                return state["idx"] < len(rows)

            def row_side_effect():
                return rows[state["idx"]]

            mock_result.next.side_effect = next_side_effect
            mock_result.get_row_data.side_effect = row_side_effect
            mock_bs.query_history_k_data_plus.return_value = mock_result

            from app.lib.datahub.data_source.interface.baostock_interface import (
                BaostockInterfaceManager,
            )

            result = BaostockInterfaceManager.get_zh_a_stock_hist_k_data("sh600000")

            self.assertEqual(list(result.columns), mock_result.fields)
            self.assertEqual(len(result), 2)
            self.assertEqual(result.iloc[0]["code"], "sh.600000")
