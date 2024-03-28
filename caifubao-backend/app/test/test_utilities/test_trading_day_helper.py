import datetime
from unittest import TestCase
from app.utilities.trading_day_helper import measure_time_difference
from app.utilities.trading_day_helper import determine_most_recent_previous_complete_trading_day
from app.utilities.trading_day_helper import determine_the_next_trading_day_end


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
        self.assertEqual(determine_most_recent_previous_complete_trading_day(trade_calendar, given_time), expected_result)

    def test_early_given_time(self):
        trade_calendar = [
            datetime.datetime(2023, 1, 2),
            datetime.datetime(2023, 1, 3),
            datetime.datetime(2023, 1, 4)
        ]
        given_time = datetime.datetime(2023, 1, 1, 12, 0)  # Before any trading day
        expected_result = datetime.datetime(2023, 1, 2)  # Closest to 2023-01-02
        self.assertEqual(determine_most_recent_previous_complete_trading_day(trade_calendar, given_time), expected_result)

    def test_late_given_time(self):
        trade_calendar = [
            datetime.datetime(2023, 1, 2),
            datetime.datetime(2023, 1, 3),
            datetime.datetime(2023, 1, 4)
        ]
        given_time = datetime.datetime(2023, 1, 5, 12, 0)  # After all trading days
        expected_result = datetime.datetime(2023, 1, 4)  # Closest to 2023-01-04
        self.assertEqual(determine_most_recent_previous_complete_trading_day(trade_calendar, given_time), expected_result)

    def test_empty_trade_calendar(self):
        trade_calendar = []
        given_time = datetime.datetime(2023, 1, 2, 12, 0)
        expected_result = None  # No available trading days
        self.assertEqual(determine_most_recent_previous_complete_trading_day(trade_calendar, given_time), expected_result)

    def test_given_time_equal_to_trading_day(self):
        trade_calendar = [
            datetime.datetime(2023, 1, 2),
            datetime.datetime(2023, 1, 3),
            datetime.datetime(2023, 1, 4)
        ]
        given_time = datetime.datetime(2023, 1, 3, 0, 0)  # Equal to a trading day
        expected_result = datetime.datetime(2023, 1, 3)
        self.assertEqual(determine_most_recent_previous_complete_trading_day(trade_calendar, given_time), expected_result)


class TestDetermineNextEndOfTradingDay(TestCase):

    def test_in_the_middle_of_trading_hour(self):
        self.trade_calendar = [
            datetime.datetime(2023, 1, 2),
            datetime.datetime(2023, 1, 3),
            datetime.datetime(2023, 1, 4),
            datetime.datetime(2023, 1, 5),
            datetime.datetime(2023, 1, 8),
            datetime.datetime(2023, 1, 9),
        ]
        given_time = datetime.datetime(2023, 1, 5, 12, 0)  # Closest to 2023-01-02
        expected_result = datetime.datetime(2023, 1, 5, 15, 0)
        self.assertEqual(determine_the_next_trading_day_end(self.trade_calendar, given_time),
                         expected_result)

    def test_after_the_end_of_a_trading_day(self):
        self.trade_calendar = [
            datetime.datetime(2023, 1, 2),
            datetime.datetime(2023, 1, 3),
            datetime.datetime(2023, 1, 4),
            datetime.datetime(2023, 1, 5),
            datetime.datetime(2023, 1, 8),
            datetime.datetime(2023, 1, 9),
        ]
        given_time = datetime.datetime(2023, 1, 2, 18, 0)  # Closest to 2023-01-02
        expected_result = datetime.datetime(2023, 1, 3, 15, 0)
        self.assertEqual(determine_the_next_trading_day_end(self.trade_calendar, given_time),
                         expected_result)

    def test_on_the_end_of_a_trading_week(self):
        self.trade_calendar = [
            datetime.datetime(2023, 1, 2),
            datetime.datetime(2023, 1, 3),
            datetime.datetime(2023, 1, 4),
            datetime.datetime(2023, 1, 5),
            datetime.datetime(2023, 1, 8),
            datetime.datetime(2023, 1, 9),
        ]
        given_time = datetime.datetime(2023, 1, 5, 18, 0)  # Closest to 2023-01-02
        expected_result = datetime.datetime(2023, 1, 8, 15, 0)
        self.assertEqual(determine_the_next_trading_day_end(self.trade_calendar, given_time),
                         expected_result)


class TestDetermineTimeDiff(TestCase):
    def test_time_diff(self):
        dt1 = datetime.datetime(2023, 1, 5, 18, 0)  # Closest to 2023-01-02
        dt2 = datetime.datetime(2023, 1, 5, 19, 0)  # Closest to 2023-01-02
        expected_result = 3600
        self.assertEqual(measure_time_difference(dt1,dt2), expected_result)