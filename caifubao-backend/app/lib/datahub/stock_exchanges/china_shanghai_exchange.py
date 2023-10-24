import datetime
import logging
from app.model.market_info import StockExchange
from app.lib.datahub.remote_data.interface import akshare as akshare_interface

logger = logging.getLogger(__name__)


class ShanghaiExchange(object):

    def __init__(self):
        self.name = "上海证券交易所"
        self.code = None
        self.exchange = StockExchange.objects(name=self.name)
        self.initialize()

    def initialize(self):
        self.check_integrity()

    def check_integrity(self):
        self.check_existence()
        self.check_trade_calendar_integrity()
        # self.check_market_overall_status()
        self.check_china_stock_index_integrity()

    def check_existence(self):
        if not self.exchange:
            logger.info(f'Initializing exchange data')
            self.exchange = StockExchange()
            self.exchange.name = self.name
            self.exchange.save()

    def check_trade_calendar_integrity(self):
        if self.exchange.trade_calendar:
            local_data_tail = self.exchange.trade_calendar[-1]
            today = datetime.datetime.today()
            if local_data_tail < today:
                logger.info(f"Updating trade calendar for {self.exchange.name}")
                trade_calendar = akshare_interface.get_trade_date_hist()
                trade_calendar = list(trade_calendar['trade_date'])
                self.exchange.trade_calendar = trade_calendar
                self.exchange.save()
            else:
                logger.info(f'Using local trade calendar for {self.exchange.name}')
        else:
            trade_calendar = akshare_interface.get_trade_date_hist()
            trade_calendar = list(trade_calendar['trade_date'])
            self.exchange.trade_calendar = trade_calendar
            self.exchange.save()

    def check_market_overall_status(self):
        if self.exchange.summary:
            pass
        else:
            pass
