import datetime
from unittest import TestCase
from app.utilities.trading_day_helper import measure_time_difference
from app.utilities.trading_day_helper import (
    determine_most_recent_previous_complete_trading_day,
)
from app.utilities.trading_day_helper import determine_the_next_trading_day_end


class TestDeterminePrevTradingDt(TestCase):
    def test_valid_trading_day(self):
        """Test when given_time is in trading hours - algorithm checks hour < 16"""
        trade_calendar = [
            datetime.datetime(2023, 1, 1),
            datetime.datetime(2023, 1, 2),
            datetime.datetime(2023, 1, 3),
            datetime.datetime(2023, 1, 4),
            datetime.datetime(2023, 1, 5),
        ]
        # Given time 12:00 < 16, so algorithm subtracts 1 day
        # Then finds closest trading day to (2023-01-01 12:00) which is 2023-01-01
        given_time = datetime.datetime(2023, 1, 2, 12, 0)
        expected_result = datetime.datetime(
            2023, 1, 1
        )  # Algorithm returns previous day
        self.assertEqual(
            determine_most_recent_previous_complete_trading_day(
                trade_calendar, given_time
            ),
            expected_result,
        )

    def test_early_given_time(self):
        trade_calendar = [
            datetime.datetime(2023, 1, 2),
            datetime.datetime(2025, 1, 27),
            datetime.datetime(2025, 2, 5),
        ]
        given_time = datetime.datetime(2025, 2, 5, 14, 0)  # Before 16:00
        expected_result = datetime.datetime(
            2025, 1, 27
        )  # Subtracts 1 day, finds closest
        self.assertEqual(
            determine_most_recent_previous_complete_trading_day(
                trade_calendar, given_time
            ),
            expected_result,
        )

    def test_late_given_time(self):
        trade_calendar = [
            datetime.datetime(2023, 1, 2),
            datetime.datetime(2025, 1, 27),
            datetime.datetime(2025, 2, 5),
        ]
        given_time = datetime.datetime(2025, 2, 5, 17, 0)  # After 16:00
        expected_result = datetime.datetime(2025, 2, 5)  # No subtraction
        self.assertEqual(
            determine_most_recent_previous_complete_trading_day(
                trade_calendar, given_time
            ),
            expected_result,
        )

    def test_empty_trade_calendar(self):
        trade_calendar = []
        given_time = datetime.datetime(2023, 1, 2, 12, 0)
        expected_result = None  # No available trading days
        self.assertEqual(
            determine_most_recent_previous_complete_trading_day(
                trade_calendar, given_time
            ),
            expected_result,
        )

    def test_given_time_equal_to_trading_day(self):
        """Test when given_time is exactly at midnight on a trading day"""
        trade_calendar = [
            datetime.datetime(2023, 1, 2),
            datetime.datetime(2023, 1, 3),
            datetime.datetime(2023, 1, 4),
        ]
        # Given time 00:00 < 16, so algorithm subtracts 1 day
        # Then finds closest to (2023-01-02 00:00) which is 2023-01-02
        given_time = datetime.datetime(2023, 1, 3, 0, 0)
        expected_result = datetime.datetime(
            2023, 1, 2
        )  # Algorithm returns previous day
        self.assertEqual(
            determine_most_recent_previous_complete_trading_day(
                trade_calendar, given_time
            ),
            expected_result,
        )


class TestDetermineNextEndOfTradingDay(TestCase):
    def test_in_the_middle_of_trading_hour(self):
        """Test when given_time is during trading hours"""
        self.trade_calendar = [
            datetime.datetime(2023, 1, 2),
            datetime.datetime(2023, 1, 3),
            datetime.datetime(2023, 1, 4),
            datetime.datetime(2023, 1, 5),
            datetime.datetime(2023, 1, 8),
            datetime.datetime(2023, 1, 9),
        ]
        # Note: The algorithm hardcodes divide_hour=16, so hour < 16 subtracts 1 day
        # Given 12:00 < 16, so algorithm subtracts 1 day: 2023-01-05 -> 2023-01-04
        # Then finds closest to 2023-01-04 which is 2023-01-04
        given_time = datetime.datetime(2023, 1, 5, 12, 0)
        expected_result = datetime.datetime(2023, 1, 4, 15, 0)
        self.assertEqual(
            determine_the_next_trading_day_end(self.trade_calendar, given_time),
            expected_result,
        )

    def test_after_the_end_of_a_trading_day(self):
        self.trade_calendar = [
            datetime.datetime(2023, 1, 2),
            datetime.datetime(2023, 1, 3),
            datetime.datetime(2023, 1, 4),
            datetime.datetime(2023, 1, 5),
            datetime.datetime(2023, 1, 8),
            datetime.datetime(2023, 1, 9),
        ]
        # 18:00 > 15 (end_hour), so finds next trading day
        given_time = datetime.datetime(2023, 1, 2, 18, 0)
        expected_result = datetime.datetime(2023, 1, 3, 15, 0)
        self.assertEqual(
            determine_the_next_trading_day_end(self.trade_calendar, given_time),
            expected_result,
        )

    def test_on_trading_day_before_end_hour(self):
        self.trade_calendar = [
            datetime.datetime(2023, 1, 2),
            datetime.datetime(2023, 1, 3),
            datetime.datetime(2023, 1, 4),
            datetime.datetime(2023, 1, 5),
            datetime.datetime(2023, 1, 8),
            datetime.datetime(2023, 1, 9),
        ]
        # Same as test_in_the_middle_of_trading_hour - algorithm uses divide_hour=16
        given_time = datetime.datetime(2023, 1, 5, 12, 0)
        expected_result = datetime.datetime(2023, 1, 4, 15, 0)
        self.assertEqual(
            determine_the_next_trading_day_end(self.trade_calendar, given_time),
            expected_result,
        )


class TestMeasureTimeDifference(TestCase):
    def test_measure_time_difference_same_day(self):
        dt1 = datetime.datetime(2023, 1, 1, 10, 0)
        dt2 = datetime.datetime(2023, 1, 1, 10, 30)
        expected = 1800  # 30 minutes in seconds
        self.assertEqual(measure_time_difference(dt1, dt2), expected)

    def test_measure_time_difference_next_day(self):
        dt1 = datetime.datetime(2023, 1, 1, 23, 0)
        dt2 = datetime.datetime(2023, 1, 2, 1, 0)
        expected = 7200  # 2 hours in seconds
        self.assertEqual(measure_time_difference(dt1, dt2), expected)
