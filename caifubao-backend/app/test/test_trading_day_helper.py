import unittest
import datetime
from app.utilities import trading_day_helper


class TestTradingDayHelper(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    def test_determine_pervious_trading_day(self):
        trade_calendar_list = [
            datetime.datetime(2022, 4, 28, 0, 0, 0),
            datetime.datetime(2022, 4, 29, 0, 0, 0),
            datetime.datetime(2022, 5, 5, 0, 0, 0),
            datetime.datetime(2022, 5, 6, 0, 0, 0),
        ]
        given_date_1 = datetime.datetime(2022, 5, 4, 5, 0, 0)
        exp_result_1 = datetime.datetime(2022, 4, 29, 0, 0, 0)
        act_result_1 = trading_day_helper.determine_pervious_trading_day(trade_calendar_list, given_date_1)
        self.assertEqual(act_result_1, exp_result_1)  # add assertion here


if __name__ == '__main__':
    unittest.main()
