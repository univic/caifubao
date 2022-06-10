import datetime
from app.model.stock import StockDailyQuote
from app.lib.db_tool import mongoengine_tool
from app.utilities import trading_day_helper


class FactorProcessor(object):
    def __init__(self):
        mongoengine_tool.connect_to_db()
        self.today = datetime.date.today()
        self.most_recent_trading_day = None
        self.trade_calendar = trading_day_helper.get_a_stock_market_trade_calendar()

    def perform_date_check(self):
        # determine the closest trading day
        today = datetime.date.today()
        if self.today != today or self.most_recent_trading_day is None:
            self.today = today
            self.most_recent_trading_day = trading_day_helper.determine_closest_trading_date(self.trade_calendar)

    def calc_fq_factor(self, item):
        self.perform_date_check()
        most_recent_factor_date = trading_day_helper.read_freshness_meta(item, 'fq_factor')
        if most_recent_factor_date:
            quote_list = StockDailyQuote.objects(code=item.code, date__gt=most_recent_factor_date)
        else:
            quote_list = StockDailyQuote.objects(code=item.code)
        # TODO: convert to df

