"""Tests for data integrity keeper module."""

from __future__ import annotations

import datetime
from unittest import TestCase
from unittest.mock import MagicMock, patch


class TestFreshnessMetaHelperFunctions(TestCase):
    """Test cases for freshness_meta_helper functions."""

    def test_read_freshness_meta_returns_none_when_no_entry(self):
        """Test read_freshness_meta returns None when no entry exists."""
        with patch(
            "app.lib.utilities.freshness_meta_helper.read_meta_obj"
        ) as mock_read_meta_obj:
            mock_read_meta_obj.return_value = None

            from app.lib.utilities.freshness_meta_helper import read_freshness_meta

            result = read_freshness_meta(
                code="sh600000",
                object_type="individual_stock",
                meta_type="quote",
                meta_name="daily_quote",
            )
            self.assertIsNone(result)

    def test_read_freshness_meta_returns_datetime_when_entry_exists(self):
        """Test read_freshness_meta returns datetime when entry exists."""
        mock_entry = MagicMock()
        mock_entry.freshness_datetime = datetime.datetime(2024, 1, 15)

        with patch(
            "app.lib.utilities.freshness_meta_helper.read_meta_obj"
        ) as mock_read_meta_obj:
            mock_read_meta_obj.return_value = mock_entry

            from app.lib.utilities.freshness_meta_helper import read_freshness_meta

            result = read_freshness_meta(
                code="sh600000",
                object_type="individual_stock",
                meta_type="quote",
                meta_name="daily_quote",
            )
            self.assertEqual(result, datetime.datetime(2024, 1, 15))

    def test_upsert_freshness_meta_calls_upsert_one(self):
        """Test upsert_freshness_meta calls upsert_one correctly."""
        with patch(
            "app.lib.utilities.freshness_meta_helper.DataFreshnessMeta"
        ) as mock_dfm:
            mock_query = MagicMock()
            mock_dfm.objects.return_value = mock_query

            from app.lib.utilities.freshness_meta_helper import upsert_freshness_meta

            upsert_freshness_meta(
                code="sh600000",
                object_type="individual_stock",
                meta_type="quote",
                meta_name="daily_quote",
                dt=datetime.datetime(2024, 1, 15),
            )

            mock_dfm.objects.assert_called_once()
            mock_query.upsert_one.assert_called_once()

    def test_check_single_factor_freshness_returns_upd_when_no_meta(self):
        """Test check_single_factor_freshness returns UPD when no meta exists."""
        mock_stock = MagicMock()
        mock_stock.code = "sh600000"
        mock_scenario = MagicMock()
        mock_scenario.current_datetime_prev_complete_trading_day = datetime.datetime(
            2024, 1, 15
        )

        with patch(
            "app.lib.utilities.freshness_meta_helper.DataFreshnessMeta"
        ) as mock_dfm:
            mock_dfm.objects.return_value.first.return_value = None

            from app.lib.utilities.freshness_meta_helper import (
                check_single_factor_freshness,
            )

            result = check_single_factor_freshness(mock_stock, "pe", mock_scenario)
            self.assertEqual(result, "UPD")

    def test_check_single_factor_freshness_returns_ok_when_fresh(self):
        """Test check_single_factor_freshness returns OK when meta is fresh."""
        mock_stock = MagicMock()
        mock_stock.code = "sh600000"

        mock_scenario = MagicMock()
        mock_scenario.current_datetime_prev_complete_trading_day = datetime.datetime(
            2024, 1, 15
        )

        mock_meta = MagicMock()
        mock_meta.freshness_datetime = datetime.datetime(2024, 1, 15)

        with patch(
            "app.lib.utilities.freshness_meta_helper.DataFreshnessMeta"
        ) as mock_dfm:
            mock_dfm.objects.return_value.first.return_value = mock_meta

            from app.lib.utilities.freshness_meta_helper import (
                check_single_factor_freshness,
            )

            result = check_single_factor_freshness(mock_stock, "pe", mock_scenario)
            self.assertEqual(result, "OK")

    def test_check_single_factor_freshness_returns_upd_when_stale(self):
        """Test check_single_factor_freshness returns UPD when meta is stale."""
        mock_stock = MagicMock()
        mock_stock.code = "sh600000"

        mock_scenario = MagicMock()
        mock_scenario.current_datetime_prev_complete_trading_day = datetime.datetime(
            2024, 1, 15
        )

        mock_meta = MagicMock()
        mock_meta.freshness_datetime = datetime.datetime(2024, 1, 10)  # older

        with patch(
            "app.lib.utilities.freshness_meta_helper.DataFreshnessMeta"
        ) as mock_dfm:
            mock_dfm.objects.return_value.first.return_value = mock_meta

            from app.lib.utilities.freshness_meta_helper import (
                check_single_factor_freshness,
            )

            result = check_single_factor_freshness(mock_stock, "pe", mock_scenario)
            self.assertEqual(result, "UPD")


if __name__ == "__main__":
    import unittest

    unittest.main()
