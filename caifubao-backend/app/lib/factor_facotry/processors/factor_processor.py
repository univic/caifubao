# import datetime
# import pandas as pd
# from app.lib.db_tool import mongoengine_tool
from app.utilities import trading_day_helper
# from app.model.stock import StockDailyQuote, IndividualStock
# from app.model.factor import FactorDataEntry
from app.utilities import freshness_meta_helper


class FactorProcessor(object):
    """
    Base class for all the factor scenario_processors
    """

    def __init__(self, stock, quote_df, current_processing_date=None):
        self.stock = stock
        self.quote_df = quote_df
        self.factor_name = None
        self.most_recent_factor_date = None
        self.current_processing_date = current_processing_date
        self.exec_flag = True

    def before_exec(self):
        self.determine_exec_flag()

    def after_exec(self):
        pass

    def determine_exec_flag(self):
        self.most_recent_factor_date = freshness_meta_helper.read_freshness_meta(self.stock, self.factor_name)
        if self.most_recent_factor_date:
            if self.current_processing_date and self.most_recent_factor_date >= self.current_processing_date:
                self.exec_flag = False

    def perform_factor_calc(self):
        pass

    def generate_factor(self):
        self.before_exec()
        if self.exec_flag:
            self.perform_factor_calc()
            self.perform_db_upsert()
            self.update_freshness_meta()
        self.after_exec()

    def update_freshness_meta(self):
        last_factor_date = self.quote_df.index[-1]
        freshness_meta_helper.upsert_freshness_meta(self.stock, self.factor_name, last_factor_date)

    def read_existing_factors(self):
        pass

    def perform_db_upsert(self):
        pass


#
# class FactorProcessor(object):
#     def __init__(self, stock, quote_df):
#         mongoengine_tool.connect_to_db()
#         self.today = datetime.date.today()
#         self.most_recent_trading_day = None
#         self.trade_calendar = trading_day_helper.get_a_stock_market_trade_calendar()
#         self.quote_df = quote_df
#         self.stock_obj = stock
#         self.update_field_list = []
#         self.update_freshness_meta_list = []

#
#     def perform_date_check(self):
#         # determine the closest trading day
#         today = datetime.date.today()
#         if self.today != today or self.most_recent_trading_day is None:
#             self.today = today
#             self.most_recent_trading_day = trading_day_helper.determine_closest_trading_date(self.trade_calendar)

    # def update_quote_data(self):
    #     for i, quote_item in self.quote_df.iterrows():
    #         quote_obj = self.quote_qs(_id=quote_item["_id"])
    #         for field_name in self.update_field_list:
    #             quote_obj[field_name] = quote_item[field_name]
    #     for field_name in self.update_freshness_meta_list:
    #         trading_day_helper.update_freshness_meta(self.stock_obj, field_name, self.most_recent_trading_day)
