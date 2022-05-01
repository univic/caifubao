import datetime
import logging
from app.lib.datahub.remote_data.akshare import handler as akshare_handler
from app.model.stock import FinanceMarket, StockIndex, IndividualStock
from app.lib.datahub.data_retriever import data_retriever
from app.utilities.progress_bar import progress_bar

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
        # prepare the progress bar
        local_index_num = len(local_index_list)
        remote_index_num = len(remote_index_list)
        unmatched_index_counter = 0
        matched_index_counter = 0
        prog_bar = progress_bar()
        update_counter_dict = {
            'NO': 0,
            "UPD": 0,
            "INC": 0,
            "NEW": 0
        }
        # check the existence of the index list
        if local_index_list:
            logger.info(f'Stock Market {self.market.name} - '
                        f'Checking local index data integrity')

            # check the existence of each index
            for i, remote_index_item in remote_index_list.iterrows():
                code = remote_index_item['代码']
                name = remote_index_item['名称']
                query = local_index_list(code=code).first()
                if query:
                    # check the quote data freshness of each index
                    update_flag = self.check_stock_index_data_freshness(query)
                    update_counter_dict[update_flag] += 1
                    matched_index_counter += 1
                else:
                    # create absent stock index and create data retrieve task.
                    self.handle_new_stock_index(code=code, name=name)
                    unmatched_index_counter += 1
                prog_bar(i, remote_index_num)
            logger.info(f'Stock Market {self.market.name} - '
                        f'Checked {local_index_num} local indexes with {remote_index_num} remote data，'
                        f'Created {unmatched_index_counter} data and data retrieve tasks for unmatched indexes')
            logger.info(f'Stock Market {self.market.name} - '
                        f'Checked quote data freshness of {matched_index_counter} local indexes. \r '
                        f'- Fresh:          {update_counter_dict["NO"]} \r'
                        f'- Need update:    {update_counter_dict["UPD"]} \r'
                        f'- Need overwrite: {update_counter_dict["INC"]} \r'
                        f'- Need new data:  {update_counter_dict["NEW"]} \r')
        else:
            # create all stock index from the scratch and create data retrieve task.
            logger.info(f'Stock Market {self.market.name} - Local index data not found, initializing...')
            for i, remote_index_item in remote_index_list.iterrows():
                code = remote_index_item['代码']
                name = remote_index_item['名称']
                self.handle_new_stock_index(code=code, name=name)
                prog_bar(i, remote_index_num)
            logger.info(f'Stock Market {self.market.name} - '
                        f'Created local data and data retrieve tasks for {remote_index_num} stock indexes')

    def handle_new_stock_index(self, code, name):
        logger.debug(f'Stock Market {self.market.name} - Initializing local index data for {code}-{name}')
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
                                                 handler='get_zh_a_stock_index_quote_daily',
                                                 kwarg_dict=data_retrieve_kwarg)

    def check_stock_index_data_freshness(self, stock_index_obj):
        # check the existence of the quote data, if not, get the full quote
        if stock_index_obj.daily_quote:
            closest_avail_trading_day = self.determine_closest_trading_date()

            # determine latest quote data
            quote_list = stock_index_obj.daily_quote
            latest_quote_date = self.determine_latest_quote_date(quote_list, 'date')

            # determine time difference
            time_diff = closest_avail_trading_day - latest_quote_date
            # create data update task
            if time_diff.days == 0:
                update_flag = "NO"
            elif time_diff.days == 1:
                update_flag = "UPD"  # Just update it with the most recent daily quote (difference of only 1 day)
            else:
                # Need the whole history quote data to do the incremental update (difference of more than 1 day)
                update_flag = "INC"
                data_retrieve_kwarg = {
                    'code': stock_index_obj.code,
                    'incremental': True
                }
                data_retriever.create_data_retrieve_task(name=f'GET STOCK INDEX FULL QUOTE FOR '
                                                              f'{stock_index_obj.code}-{stock_index_obj.name}',
                                                         module='akshare',
                                                         handler='get_zh_a_stock_index_quote_daily',
                                                         kwarg_dict=data_retrieve_kwarg)

        else:
            # no quote data at all
            update_flag = "NEW"
            data_retrieve_kwarg = {
                'code': stock_index_obj.code
            }
            data_retriever.create_data_retrieve_task(name=f'GET STOCK INDEX FULL QUOTE FOR '
                                                          f'{stock_index_obj.code}-{stock_index_obj.name}',
                                                     module='akshare',
                                                     handler='get_zh_a_stock_index_quote_daily',
                                                     kwarg_dict=data_retrieve_kwarg)
        return update_flag

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

    def determine_closest_trading_date(self):
        now = datetime.datetime.now()
        trading_date_list = self.market.trade_calendar
        cloest_trading_day = min(trading_date_list, key=lambda x: (x > now, abs(x - now)))
        if now.hour < 15:
            closest_avail_trading_day = cloest_trading_day - datetime.timedelta(days=1)
        else:
            closest_avail_trading_day = cloest_trading_day
        return closest_avail_trading_day

    @staticmethod
    def determine_latest_quote_date(quote_list, date_attribute):
        latest_quote_date = max(quote_list, key=lambda x: x[date_attribute])
        return latest_quote_date
