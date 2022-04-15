import datetime
import logging
from app.lib.datahub.remote_data.akshare import handler as akshare_handler
from app.model.stock import FinanceMarket, StockIndex

logger = logging.getLogger()


class ChinaAStock(object):

    def __init__(self):
        self.market = None
        self.market_name = "A股"
        self.initialization()

    def initialization(self):
        self.check_local_data_existence()

    def check_local_data_existence(self):
        self.market = FinanceMarket.objects(name="A股").first()
        # check the existence of basic market data
        if not self.market:
            logger.info(f'Local market data for {self.market_name} not found, initializing')
            new_market = FinanceMarket()
            new_market.name = "A股"
            trade_calendar = akshare_handler.get_a_stock_trade_date_hist()
            trade_calendar = list(trade_calendar['trade_date'])
            new_market.trade_calendar = trade_calendar
            new_market.save()
        else:
            logger.info(f'Checking Market Data Integrity for {self.market.name}')
            self.check_trade_calendar_integrity()

    def check_trade_calendar_integrity(self):
        if self.market.trade_calendar:
            local_data_tail = self.market.trade_calendar[-1]
            today = datetime.datetime.today()
            if local_data_tail < today:
                logger.info(f"Stock Market {self.market.name} - Updating trade calendar")
                trade_calendar = akshare_handler.get_a_stock_trade_date_hist()
                trade_calendar = list(trade_calendar['trade_date'])
                self.market.trade_calendar = trade_calendar
                self.market.save()
            else:
                logger.info(f'Trade calendar check OK, using local trade calendar for {self.market.name}')
        else:
            trade_calendar = akshare_handler.get_a_stock_trade_date_hist()
            trade_calendar = list(trade_calendar['trade_date'])
            self.market.trade_calendar = trade_calendar
            self.market.save()

    def check_stock_index_integrity(self):
        local_index_list = StockIndex.objects(market=self.market)
        remote_stock_list = akshare_handler.get_zh_stock_index_list()
        if local_index_list:
            logger.info(f'Stock Market {self.market.name} - '
                        f'Checking local index data integrity')
            for remote_index_item in remote_stock_list.iterrows():
                code = remote_index_item['代码']
                name = remote_index_item['名称']
                query = local_index_list.objects(code=code).first()
                if query:
                    pass
                else:
                    logger.info(f'Stock Market {self.market.name} - '
                                f'Local data for {code}-{name} not found, initializing...')
        else:
            logger.info(f'Stock Market {self.market.name} - Local index data not found, initializing...')

    def create_new_stock_index(self, code, name):
        new_stock_index = StockIndex()
        new_stock_index.code = code
        new_stock_index.name = name
        new_stock_index.market = self.market
