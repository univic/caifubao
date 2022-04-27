import datetime
import logging
from app.lib.datahub.remote_data.akshare import handler as akshare_handler
from app.model.stock import FinanceMarket, StockIndex, IndividualStock
from app.lib.datahub.data_retriever import data_retriever


logger = logging.getLogger()


class ChinaAStock(object):

    def __init__(self):
        self.market = None
        self.market_name = "A股"
        self.initialization()

    def initialization(self):
        self.check_local_data_existence()

    def check_local_data_existence(self):
        self.check_market_data_existence()
        self.check_stock_index_integrity()

    def check_market_data_existence(self):
        self.market = FinanceMarket.objects(name="A股").first()
        # check the existence of basic market data
        if not self.market:
            logger.info(f'Local market data for {self.market_name} not found, initializing')
            new_market = FinanceMarket()
            new_market.name = "A股"
            new_market.save()
            self.market = new_market
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
        remote_index_list = akshare_handler.get_zh_stock_index_list()
        if local_index_list:
            logger.info(f'Stock Market {self.market.name} - '
                        f'Checking local index data integrity')
            for i, remote_index_item in remote_index_list.iterrows():
                code = remote_index_item['代码']
                name = remote_index_item['名称']
                query = local_index_list(code=code).first()
                if query:
                    pass
                else:
                    # create absent stock index and create data retrieve task.
                    self.handle_new_stock_index(code=code, name=name)
        else:
            # create all stock index and create data retrieve task.
            logger.info(f'Stock Market {self.market.name} - Local index data not found, initializing...')
            for i, remote_index_item in remote_index_list.iterrows():
                print(remote_index_item)
                code = remote_index_item['代码']
                name = remote_index_item['名称']
                self.handle_new_stock_index(code=code, name=name)
            logger.info(f'Stock Market {self.market.name} - Local index data created')
            logger.info(f'Stock Market {self.market.name} - index quote data retrieve task created')

    def handle_new_stock_index(self, code, name):
        logger.info(f'Stock Market {self.market.name} - Initializing local index data for {code}-{name}')
        new_stock_index = StockIndex()
        new_stock_index.code = code
        new_stock_index.name = name
        new_stock_index.market = self.market
        new_stock_index.save()
        data_retrieve_kwarg = {
            'code': code
        }
        data_retriever.create_data_retrieve_task(name=f'GET STOCK INDEX FULL QUOTE FOR '
                                                      f'{new_stock_index.code}-{new_stock_index.name}',
                                                 module='akshare',
                                                 handler='get_full_stock_index_quote',
                                                 kwarg_dict=data_retrieve_kwarg)

    def check_stock_index_data_freshness(self, stock_index_obj):

        # determine the closest trading day
        now = datetime.datetime.now()
        current_date = now.date()
        print(current_date)
        trading_date_list = self.market.trade_calendar
        cloest_trading_day = min(trading_date_list, key=lambda x: (x > now, abs(x - now)))
        if now.hour < 15:
            closest_avail_trading_day = cloest_trading_day - datetime.timedelta(days=1)
        else:
            closest_avail_trading_day = cloest_trading_day
        print(closest_avail_trading_day)

        # determine latest quote data
        quote_list = stock_index_obj.daily_quote

        # create data update task

    def check_individual_stock_integrity(self):
        local_stock_list = IndividualStock.objects(market=self.market)
        remote_stock_list = None
        if local_stock_list:
            pass
        else:
            logger.info(f'Stock Market {self.market.name} - Local indivdual stock data not found, initializing...')

    @staticmethod
    def update_metadata(data, df, column):
        max_date = max(df[column])
        data.meta_data.last_update = datetime.datetime.now()
        # data.meta_data.date_of_most_recent_daily_quote = max_data