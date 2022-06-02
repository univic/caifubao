
# TODO: FORCE UPDATE
# TODO: INTEGRITY SELF CHECK

import pandas as pd
from app.lib.db_tool import mongoengine_tool
from app.utilities.progress_bar import progress_bar
from app.model.stock import StockIndex, IndividualStock, StockDailyQuote
from app.utilities import trading_day_helper


class DataIntegrityWatcher(object):

    def __init__(self):
        self.target_name = None
        self.target_obj = None
        self.list_len = 0
        self.item_list = []
        self.abnormal_list = []
        self.trade_calendar = None
        self.most_recent_quote_date = None
        self.check_func_list = [
            self.check_data_freshness_meta,
            self.check_quote_data_continuity
        ]
        self.counter_dict = {
            "GOOD": 0,
            "NO_DATA": 0,
            "BAD": 0,
            "OUT_OF_DATE": 0,
        }

    def check_index_data_integrity(self, allow_update=False):
        self.target_name = 'index'
        self.dispatch_check(StockIndex)
        return None

    def check_stock_data_integrity(self, allow_update=False):
        self.target_name = 'stock'
        self.dispatch_check(IndividualStock)
        return None

    def before_check(self):
        mongoengine_tool.connect_to_db()
        self.trade_calendar = trading_day_helper.get_a_stock_market_trade_calendar()
        self.most_recent_quote_date = trading_day_helper.determine_closest_trading_date(self.trade_calendar)

    def after_check(self):
        mongoengine_tool.disconnect_from_db()

    def get_item_list(self):
        self.item_list = self.target_obj.objects()
        self.list_len = self.item_list.count()

    def perform_check(self, item):
        res = "GOOD"
        flag_set = set()
        for func in self.check_func_list:
            flag = func(item)
            flag_set.add(flag)
        if "NO_DATA" in flag_set:
            res = "NO_DATA"
        elif "BAD" in flag_set:
            res = "BAD"
        elif "OUT_OF_DATE" in flag_set:
            res = "OUT_OF_DATE"
        return res

    def dispatch_check(self, target_obj):
        self.target_obj = target_obj
        self.before_check()
        self.get_item_list()
        prog_bar = progress_bar()
        for i, item in enumerate(self.item_list):
            flag = self.perform_check(item)
            self.counter_dict[flag] += 1
            prog_bar(i, self.list_len)
        self.after_check()
        self.output()

    def check_data_freshness_meta(self, item):
        check_flag = "GOOD"
        daily_quote_freshness_meta = trading_day_helper.read_freshness_meta(item, 'daily_quote')
        latest_quote_date = trading_day_helper.determine_latest_quote_date(item)
        if daily_quote_freshness_meta != latest_quote_date:
            check_flag = "BAD"
            self.abnormal_list.append(f'{item.code} - {item.name}: Data freshness meta does not match with quote data')
        return check_flag

    def check_quote_data_continuity(self, item):
        check_flag = "GOOD"
        quote_list = StockDailyQuote.objects(code=item.code).only('date').order_by('+date')
        if quote_list:
            most_early_quote = min(quote_list, key=lambda x: x.date)
            most_recent_quote = max(quote_list, key=lambda x: x.date)
            most_early_quote_date = most_early_quote.date
            most_recent_quote_date = most_recent_quote.date
            if most_recent_quote_date == self.most_recent_quote_date:
                start_date_index = self.trade_calendar.index(most_early_quote_date)
                end_date_index = self.trade_calendar.index(most_recent_quote_date)
                sliced_trade_calendar_set = set(self.trade_calendar[start_date_index:end_date_index + 1])
                quote_date_set = set([x.date for x in quote_list])
                quote_set_diff_1 = sliced_trade_calendar_set - quote_date_set
                quote_set_diff_2 = quote_date_set - sliced_trade_calendar_set
                quote_set_diff = quote_set_diff_1 | quote_set_diff_2
                if quote_set_diff:
                    check_flag = "BAD"
                    day_str = ''
                    for day in quote_set_diff:
                        day_str += day.strftime('%Y-%m-%d') + ', '
                    self.abnormal_list.append(
                        f'{item.code} - {item.name}: quote data discontinuity: {day_str}')
            else:
                check_flag = "OUT_OF_DATE"
                # self.abnormal_list.append(
                #     f'{item.code} - {item.name}: Quote data out of date')
        else:
            check_flag = "NO_DATA"

        return check_flag

    def output(self):
        ratio_1 = (self.counter_dict['GOOD'] / (self.list_len - self.counter_dict['NO_DATA'] -
                                                self.counter_dict['OUT_OF_DATE']))*100
        print(f"Of all {self.list_len} {self.target_name} data, "
              f"{self.counter_dict['GOOD']} are good, "
              f"{self.counter_dict['NO_DATA']} no data, "
              f"{self.counter_dict['OUT_OF_DATE']} out of date, "
              f"{self.counter_dict['BAD']} faild to pass integrity check, "
              f"pass ratio of objects with recent data is {ratio_1:2.2f}%, "
              f"total pass ratio is {(self.counter_dict['GOOD'] / self.list_len)*100:2.2f}%")
        for item in self.abnormal_list:
            print(item)


if __name__ == '__main__':
    runner = DataIntegrityWatcher()
    runner.check_index_data_integrity()
    # runner.check_stock_data_integrity()
    # runner.before_check()
    # t_obj = StockIndex.objects(code='sz399986').first()
    #
    # runner.check_quote_data_continuity(t_obj)

