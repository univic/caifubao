"""Tests for trading day helper utility."""

from __future__ import annotations

import unittest
import datetime
from unittest import TestCase
from zoneinfo import ZoneInfo


class TestDetermineClosestTradingDate(TestCase):
    """Test cases for determine_closest_trading_date function."""

    def test_determine_closest_trading_date_before_first(self):
        """Test when given time is before first trading day."""
        from app.lib.utilities.trading_day_helper import determine_closest_trading_date

        trade_calendar = [
            datetime.datetime(2024, 1, 2),
            datetime.datetime(2024, 1, 3),
            datetime.datetime(2024, 1, 4),
        ]
        given_time = datetime.datetime(2024, 1, 1, 12, 0)
        result = determine_closest_trading_date(trade_calendar, given_time)
        self.assertEqual(result, datetime.datetime(2024, 1, 2))

    def test_determine_closest_trading_date_after_last(self):
        """Test when given time is after last trading day."""
        from app.lib.utilities.trading_day_helper import determine_closest_trading_date

        trade_calendar = [
            datetime.datetime(2024, 1, 2),
            datetime.datetime(2024, 1, 3),
            datetime.datetime(2024, 1, 4),
        ]
        given_time = datetime.datetime(2024, 1, 10, 12, 0)
        result = determine_closest_trading_date(trade_calendar, given_time)
        self.assertEqual(result, datetime.datetime(2024, 1, 4))

    def test_determine_closest_trading_date_in_between(self):
        """Test when given time is between trading days."""
        from app.lib.utilities.trading_day_helper import determine_closest_trading_date

        trade_calendar = [
            datetime.datetime(2024, 1, 2),
            datetime.datetime(2024, 1, 3),
            datetime.datetime(2024, 1, 4),
        ]
        given_time = datetime.datetime(2024, 1, 3, 12, 0)
        result = determine_closest_trading_date(trade_calendar, given_time)
        self.assertEqual(result, datetime.datetime(2024, 1, 3))

    def test_determine_closest_trading_date_early_morning(self):
        """Test when given time is early morning (before 3 PM)."""
        from app.lib.utilities.trading_day_helper import determine_closest_trading_date

        trade_calendar = [
            datetime.datetime(2024, 1, 2),
            datetime.datetime(2024, 1, 3),
        ]
        # Given time is 2024-01-02 08:00 (before divide_hour=3), so subtracts 1 day
        given_time = datetime.datetime(2024, 1, 2, 8, 0)
        result = determine_closest_trading_date(trade_calendar, given_time)
        self.assertEqual(result, datetime.datetime(2024, 1, 2))

    def test_determine_closest_trading_date_uses_runtime_now(self):
        """Test that default time is evaluated at call time, not import time."""
        from unittest.mock import patch
        from app.lib.utilities import trading_day_helper

        trade_calendar = [
            datetime.datetime(2024, 4, 3),
            datetime.datetime(2024, 4, 7),
        ]

        class FrozenDatetime(datetime.datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2024, 4, 7, 18, 0, tzinfo=tz)

        with patch.object(trading_day_helper.datetime, "datetime", FrozenDatetime):
            result = trading_day_helper.determine_closest_trading_date(trade_calendar)

        self.assertEqual(result, datetime.datetime(2024, 4, 7))


class TestDetermineLatestCompleteTradingDate(TestCase):
    def test_uses_beijing_market_close_boundary(self):
        from app.lib.utilities.trading_day_helper import (
            determine_latest_complete_trading_date,
        )

        calendar = [
            datetime.datetime(2026, 8, 21),
            datetime.datetime(2026, 8, 24),
            datetime.datetime(2026, 8, 25),
        ]

        before_close = datetime.datetime(
            2026, 8, 24, 15, 59, tzinfo=ZoneInfo("Asia/Shanghai")
        )
        after_close = datetime.datetime(
            2026, 8, 24, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")
        )

        self.assertEqual(
            determine_latest_complete_trading_date(calendar, before_close),
            datetime.datetime(2026, 8, 21),
        )
        self.assertEqual(
            determine_latest_complete_trading_date(calendar, after_close),
            datetime.datetime(2026, 8, 24),
        )

    def test_converts_utc_input_to_beijing_time(self):
        from app.lib.utilities.trading_day_helper import (
            determine_latest_complete_trading_date,
        )

        calendar = [datetime.datetime(2026, 8, 21), datetime.datetime(2026, 8, 24)]
        utc_time = datetime.datetime(2026, 8, 24, 8, 1, tzinfo=datetime.UTC)

        self.assertEqual(
            determine_latest_complete_trading_date(calendar, utc_time),
            datetime.datetime(2026, 8, 24),
        )


class TestDetermineMostRecentPreviousCompleteTradingDay(TestCase):
    """Test cases for determine_most_recent_previous_complete_trading_day function."""

    def test_valid_trading_day(self):
        """Test when given_time is in trading hours."""
        from app.lib.utilities.trading_day_helper import (
            determine_most_recent_previous_complete_trading_day,
        )

        trade_calendar = [
            datetime.datetime(2024, 1, 2),
            datetime.datetime(2024, 1, 3),
            datetime.datetime(2024, 1, 4),
            datetime.datetime(2024, 1, 5),
        ]
        # 12:00 < 16, so algorithm subtracts 1 day
        given_time = datetime.datetime(2024, 1, 4, 12, 0)
        result = determine_most_recent_previous_complete_trading_day(
            trade_calendar, given_time
        )
        self.assertEqual(result, datetime.datetime(2024, 1, 3))

    def test_late_given_time(self):
        """Test when given_time is after trading hours (16:00)."""
        from app.lib.utilities.trading_day_helper import (
            determine_most_recent_previous_complete_trading_day,
        )

        trade_calendar = [
            datetime.datetime(2024, 1, 2),
            datetime.datetime(2024, 1, 3),
            datetime.datetime(2024, 1, 4),
        ]
        # 17:00 > 16, no subtraction
        given_time = datetime.datetime(2024, 1, 4, 17, 0)
        result = determine_most_recent_previous_complete_trading_day(
            trade_calendar, given_time
        )
        self.assertEqual(result, datetime.datetime(2024, 1, 4))

    def test_empty_trade_calendar(self):
        """Test when trade calendar is empty."""
        from app.lib.utilities.trading_day_helper import (
            determine_most_recent_previous_complete_trading_day,
        )

        trade_calendar = []
        given_time = datetime.datetime(2024, 1, 2, 12, 0)
        result = determine_most_recent_previous_complete_trading_day(
            trade_calendar, given_time
        )
        self.assertIsNone(result)

    def test_given_time_before_first_trading_day(self):
        """Test when given_time is before first trading day."""
        from app.lib.utilities.trading_day_helper import (
            determine_most_recent_previous_complete_trading_day,
        )

        trade_calendar = [
            datetime.datetime(2024, 1, 2),
            datetime.datetime(2024, 1, 3),
        ]
        given_time = datetime.datetime(2024, 1, 1, 12, 0)
        result = determine_most_recent_previous_complete_trading_day(
            trade_calendar, given_time
        )
        self.assertEqual(result, datetime.datetime(2024, 1, 2))


class TestDetermineMostRecentNextTradingDt(TestCase):
    """Test cases for determine_most_recent_next_trading_dt function."""

    def test_next_trading_day_exists(self):
        """Test finding next trading day."""
        from app.lib.utilities.trading_day_helper import (
            determine_most_recent_next_trading_dt,
        )

        trade_calendar = [
            datetime.datetime(2024, 1, 2),
            datetime.datetime(2024, 1, 3),
            datetime.datetime(2024, 1, 4),
        ]
        given_time = datetime.datetime(2024, 1, 2, 17, 0)
        result = determine_most_recent_next_trading_dt(trade_calendar, given_time)
        self.assertEqual(result, datetime.datetime(2024, 1, 3))

    def test_empty_trade_calendar(self):
        """Test when trade calendar is empty."""
        from app.lib.utilities.trading_day_helper import (
            determine_most_recent_next_trading_dt,
        )

        trade_calendar = []
        given_time = datetime.datetime(2024, 1, 2, 12, 0)
        result = determine_most_recent_next_trading_dt(trade_calendar, given_time)
        self.assertIsNone(result)


class TestDetermineTradingDateDiff(TestCase):
    """Test cases for determine_trading_date_diff function."""

    def test_same_day(self):
        """Test when both dates are the same."""
        from app.lib.utilities.trading_day_helper import determine_trading_date_diff

        trade_calendar = [
            datetime.datetime(2024, 1, 2),
            datetime.datetime(2024, 1, 3),
            datetime.datetime(2024, 1, 4),
        ]
        result = determine_trading_date_diff(
            trade_calendar,
            datetime.datetime(2024, 1, 3),
            datetime.datetime(2024, 1, 3),
        )
        self.assertEqual(result, 0)

    def test_one_day_diff(self):
        """Test when dates are one day apart."""
        from app.lib.utilities.trading_day_helper import determine_trading_date_diff

        trade_calendar = [
            datetime.datetime(2024, 1, 2),
            datetime.datetime(2024, 1, 3),
            datetime.datetime(2024, 1, 4),
        ]
        result = determine_trading_date_diff(
            trade_calendar,
            datetime.datetime(2024, 1, 3),
            datetime.datetime(2024, 1, 4),
        )
        self.assertEqual(result, 1)

    def test_multiple_day_diff(self):
        """Test when dates are multiple days apart."""
        from app.lib.utilities.trading_day_helper import determine_trading_date_diff

        trade_calendar = [
            datetime.datetime(2024, 1, 2),
            datetime.datetime(2024, 1, 3),
            datetime.datetime(2024, 1, 4),
            datetime.datetime(2024, 1, 5),
            datetime.datetime(2024, 1, 8),
        ]
        result = determine_trading_date_diff(
            trade_calendar,
            datetime.datetime(2024, 1, 2),
            datetime.datetime(2024, 1, 8),
        )
        self.assertEqual(result, 4)


class TestIsTradingDay(TestCase):
    """Test cases for is_trading_day function."""

    def test_is_trading_day_true(self):
        """Test when given time is a trading day."""
        from app.lib.utilities.trading_day_helper import is_trading_day

        trade_calendar = [
            datetime.datetime(2024, 1, 2),
            datetime.datetime(2024, 1, 3),
            datetime.datetime(2024, 1, 4),
        ]
        given_time = datetime.datetime(2024, 1, 3, 12, 0)
        result = is_trading_day(trade_calendar, given_time)
        self.assertTrue(result)

    def test_is_trading_day_false(self):
        """Test when given time is not a trading day."""
        from app.lib.utilities.trading_day_helper import is_trading_day

        trade_calendar = [
            datetime.datetime(2024, 1, 2),
            datetime.datetime(2024, 1, 3),
            datetime.datetime(2024, 1, 4),
        ]
        given_time = datetime.datetime(2024, 1, 5, 12, 0)
        result = is_trading_day(trade_calendar, given_time)
        self.assertFalse(result)

    def test_is_trading_day_with_time_component(self):
        """Test that time component is stripped before comparison."""
        from app.lib.utilities.trading_day_helper import is_trading_day

        trade_calendar = [
            datetime.datetime(2024, 1, 2),
            datetime.datetime(2024, 1, 3),
        ]
        # Given time has different time components but same date
        given_time = datetime.datetime(2024, 1, 3, 23, 59, 59)
        result = is_trading_day(trade_calendar, given_time)
        self.assertTrue(result)


class TestNextTradingDay(TestCase):
    """Test cases for next_trading_day function."""

    def test_next_trading_day_found(self):
        """Test finding next trading day using algorithm that finds closest <= given_time."""
        from app.lib.utilities.trading_day_helper import next_trading_day

        trade_calendar = [
            datetime.datetime(2024, 1, 2),
            datetime.datetime(2024, 1, 3),
            datetime.datetime(2024, 1, 4),
        ]
        # The algorithm finds min with key: (x <= given_time, abs(x - given_time))
        # Given 2024-01-02 17:00, all trading days are > given_time, so it finds closest
        # For ties, (False, 0) < (False, abs) is False, so first element wins = 2024-01-02
        given_time = datetime.datetime(2024, 1, 5, 12, 0)
        result = next_trading_day(trade_calendar, given_time)
        self.assertEqual(result, datetime.datetime(2024, 1, 4))


class TestConvertDateToDatetime(TestCase):
    """Test cases for convert_date_to_datetime function."""

    def test_convert_date_to_datetime(self):
        """Test converting date object to datetime."""
        from app.lib.utilities.trading_day_helper import convert_date_to_datetime

        date_obj = datetime.date(2024, 1, 3)
        result = convert_date_to_datetime(date_obj)
        self.assertEqual(result, datetime.datetime(2024, 1, 3, 0, 0))


class TestMeasureTimeDifference(TestCase):
    """Test cases for measure_time_difference function."""

    def test_measure_time_difference_same_day(self):
        """Test measuring time difference within same day."""
        from app.lib.utilities.trading_day_helper import measure_time_difference

        dt1 = datetime.datetime(2024, 1, 1, 10, 0)
        dt2 = datetime.datetime(2024, 1, 1, 10, 30)
        result = measure_time_difference(dt1, dt2)
        self.assertEqual(result, 1800.0)  # 30 minutes in seconds

    def test_measure_time_difference_next_day(self):
        """Test measuring time difference across days."""
        from app.lib.utilities.trading_day_helper import measure_time_difference

        dt1 = datetime.datetime(2024, 1, 1, 23, 0)
        dt2 = datetime.datetime(2024, 1, 2, 1, 0)
        result = measure_time_difference(dt1, dt2)
        self.assertEqual(result, 7200.0)  # 2 hours in seconds


class TestGetCurrentDateStr(TestCase):
    """Test cases for get_current_date_str function."""

    def test_get_current_date_str_format(self):
        """Test that date string has correct format."""
        from app.lib.utilities.trading_day_helper import get_current_date_str

        result = get_current_date_str()
        # Should be YYYYMMDD format, 8 characters
        self.assertEqual(len(result), 8)
        self.assertTrue(result.isdigit())


class TestUpdateTitleDateStr(TestCase):
    """Test cases for update_title_date_str function."""

    def test_update_existing_date_in_title(self):
        """Test updating existing date in title string."""
        from app.lib.utilities.trading_day_helper import update_title_date_str

        title = "Task 20240101 completed"
        date = datetime.datetime(2024, 1, 15)
        result = update_title_date_str(title, date)
        self.assertEqual(result, "Task 20240115 completed")

    def test_append_date_to_title(self):
        """Test appending date to title without date pattern."""
        from app.lib.utilities.trading_day_helper import update_title_date_str

        title = "Task completed"
        date = datetime.datetime(2024, 1, 15)
        result = update_title_date_str(title, date)
        self.assertEqual(result, "Task completed 20240115")


if __name__ == "__main__":
    unittest.main()
