import unittest
import datetime
from app.utilities import trading_day_helper


class TestTradingDayHelper(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    def test_determine_closest_trading_date(self):
        trade_calendar_list = [
            datetime.datetime(2022, 4, 28, 0, 0, 0),
            datetime.datetime(2022, 4, 29, 0, 0, 0),
            datetime.datetime(2022, 5, 5, 0, 0, 0),
            datetime.datetime(2022, 5, 6, 0, 0, 0),
        ]
        given_input_1 = datetime.datetime(2022, 5, 4, 5, 0, 0)
        exp_result_1 = datetime.datetime(2022, 4, 29, 0, 0, 0)
        act_result_1 = trading_day_helper.determine_closest_trading_date(trade_calendar=trade_calendar_list,
                                                                         given_time=given_input_1)
        self.assertEqual(act_result_1, exp_result_1)

    def test_convert_date_to_datetime(self):
        date = datetime.date(2022, 5, 18)
        exp_result_1 = datetime.datetime(2022, 5, 18, 0, 0, 0)
        act_result_1 = trading_day_helper.convert_date_to_datetime(date)
        self.assertEqual(act_result_1, exp_result_1)

    def test_update_title_date_str(self):
        dt_1 = datetime.datetime(2022, 5, 23, 0, 0, 0)
        title_str_1 = 'UPDATE INDEX QUOTE WITH SPOT DATA 20220522'
        exp_result_1 = 'UPDATE INDEX QUOTE WITH SPOT DATA 20220523'
        act_result_1 = trading_day_helper.update_title_date_str(title_str_1, dt_1)
        self.assertEqual(exp_result_1, act_result_1)
        title_str_2 = 'UPDATE INDEX QUOTE WITH SPOT DATA'
        exp_result_2 = 'UPDATE INDEX QUOTE WITH SPOT DATA 20220523'
        act_result_2 = trading_day_helper.update_title_date_str(title_str_2, dt_1)
        self.assertEqual(exp_result_2, act_result_2)

    def test_next_trading_day(self):
        trade_calendar_list = [
            datetime.datetime(2022, 4, 28, 0, 0, 0),
            datetime.datetime(2022, 4, 29, 0, 0, 0),
            datetime.datetime(2022, 5, 5, 0, 0, 0),
            datetime.datetime(2022, 5, 6, 0, 0, 0),
        ]
        given_input_1 = datetime.datetime(2022, 4, 29, 0, 0, 0)
        exp_result_1 = datetime.datetime(2022, 5, 5, 0, 0, 0)
        act_result_1 = trading_day_helper.next_trading_day(trade_calendar=trade_calendar_list,
                                                           given_time=given_input_1)
        self.assertEqual(act_result_1, exp_result_1)


if __name__ == '__main__':
    unittest.main()
