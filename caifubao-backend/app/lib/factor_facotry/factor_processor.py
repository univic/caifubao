import datetime
import pandas as pd
import talib
from app.lib.db_tool import mongoengine_tool
from app.utilities import trading_day_helper
from app.model.stock import StockDailyQuote, IndividualStock
from app.model.factor import FactorDataEntry


class FactorProcessor(object):
    def __init__(self):
        mongoengine_tool.connect_to_db()
        self.today = datetime.date.today()
        self.most_recent_trading_day = None
        self.trade_calendar = trading_day_helper.get_a_stock_market_trade_calendar()
        self.quote_qs = None
        self.quote_df = None
        self.stock_obj = None
        self.update_field_list = []
        self.update_freshness_meta_list = []

    def dispatch(self, item, allow_update=False):
        self.stock_obj = item
        self.perform_date_check()
        self.query_quote_data()
        if allow_update:
            self.update_quote_data()

    def perform_date_check(self):
        # determine the closest trading day
        today = datetime.date.today()
        if self.today != today or self.most_recent_trading_day is None:
            self.today = today
            self.most_recent_trading_day = trading_day_helper.determine_closest_trading_date(self.trade_calendar)

    def query_quote_data(self):
        field_exclude_list = ['volume', 'trade_amount']
        most_recent_factor_date = trading_day_helper.read_freshness_meta(self.stock_obj, 'fq_factor')
        if most_recent_factor_date:
            self.quote_qs = StockDailyQuote.objects(code=self.stock_obj.code,
                                                    date__gt=most_recent_factor_date) \
                .exclude(*field_exclude_list) \
                .order_by('+date')

        else:
            self.quote_qs = StockDailyQuote.objects(code=self.stock_obj.code) \
                .exclude(*field_exclude_list) \
                .order_by('+date')
        # convert to df
        quote_json = self.quote_qs.as_pymongo()
        self.quote_df = pd.DataFrame(quote_json)
        self.quote_df.set_index("date", inplace=True)

    def calc_ma_factor(self):
        field_list = ["ma_10"]
        self.quote_df["ma_10"] = talib.MA(self.quote_df["close_hfq"], timeperiod=10)
        self.update_field_list += field_list
        self.update_freshness_meta_list += field_list
    
    def update_quote_data(self):
        for i, quote_item in self.quote_df.iterrows():
            quote_obj = self.quote_qs(_id=quote_item["_id"])
            for field_name in self.update_field_list:
                quote_obj[field_name] = quote_item[field_name]
        for field_name in self.update_freshness_meta_list:
            trading_day_helper.update_freshness_meta(self.stock_obj, field_name, self.most_recent_trading_day)


if __name__ == '__main__':
    obj = FactorProcessor()
    stock_obj = IndividualStock.objects(code='sh601166').first()
    obj.dispatch(stock_obj)
