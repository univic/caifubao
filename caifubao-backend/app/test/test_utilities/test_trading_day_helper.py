import datetime
from unittest import TestCase
from app.utilities.trading_day_helper import determine_most_recent_previous_trading_dt


class TestDeterminePrevTradingDt(TestCase):
    def test_valid_trading_day(self):
        trade_calendar = [
            datetime.datetime(2023, 1, 1),
            datetime.datetime(2023, 1, 2),
            datetime.datetime(2023, 1, 3),
            datetime.datetime(2023, 1, 4),
            datetime.datetime(2023, 1, 5)
        ]
        given_time = datetime.datetime(2023, 1, 2, 12, 0)  # Closest to 2023-01-02
        expected_result = datetime.datetime(2023, 1, 2)
        self.assertEqual(determine_most_recent_previous_trading_dt(trade_calendar, given_time), expected_result)

    def test_early_given_time(self):
        trade_calendar = [
            datetime.datetime(2023, 1, 2),
            datetime.datetime(2023, 1, 3),
            datetime.datetime(2023, 1, 4)
        ]
        given_time = datetime.datetime(2023, 1, 1, 12, 0)  # Before any trading day
        expected_result = datetime.datetime(2023, 1, 2)  # Closest to 2023-01-02
        self.assertEqual(determine_most_recent_previous_trading_dt(trade_calendar, given_time), expected_result)

    def test_late_given_time(self):
        trade_calendar = [
            datetime.datetime(2023, 1, 2),
            datetime.datetime(2023, 1, 3),
            datetime.datetime(2023, 1, 4)
        ]
        given_time = datetime.datetime(2023, 1, 5, 12, 0)  # After all trading days
        expected_result = datetime.datetime(2023, 1, 4)  # Closest to 2023-01-04
        self.assertEqual(determine_most_recent_previous_trading_dt(trade_calendar, given_time), expected_result)

    def test_empty_trade_calendar(self):
        trade_calendar = []
        given_time = datetime.datetime(2023, 1, 2, 12, 0)
        expected_result = None  # No available trading days
        self.assertEqual(determine_most_recent_previous_trading_dt(trade_calendar, given_time), expected_result)

    def test_given_time_equal_to_trading_day(self):
        trade_calendar = [
            datetime.datetime(2023, 1, 2),
            datetime.datetime(2023, 1, 3),
            datetime.datetime(2023, 1, 4)
        ]
        given_time = datetime.datetime(2023, 1, 3, 0, 0)  # Equal to a trading day
        expected_result = datetime.datetime(2023, 1, 3)
        self.assertEqual(determine_most_recent_previous_trading_dt(trade_calendar, given_time), expected_result)
