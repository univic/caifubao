import datetime
import logging
from app.lib.datahub.remote_data.akshare import handler as akshare_handler
from app.model.stock import FinanceMarket, StockIndex, IndividualStock
from app.lib.datahub.data_retriever import akshare_datahub_task, baostock_datahub_task
from app.utilities.progress_bar import progress_bar
from app.utilities import trading_day_helper

logger = logging.getLogger()


class ChinaAStock(object):

    def __init__(self):
        self.market = None
        self.market_name = "A股"
        self.today = datetime.date.today()
        self.most_recent_trading_day = None

    def initialize(self):
        self.check_local_data_existence()

    def check_local_data_existence(self):
        self.check_market_data_existence()
        self.check_stock_index_integrity()
        self.check_stock_data_integrity()

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
        logger.info(f'Stock Market {self.market.name} - '
                    f'Checking local index data integrity')
        local_index_list = StockIndex.objects(market=self.market)
        remote_index_list = akshare_handler.get_zh_stock_index_list()

        # determine the closest trading day
        today = datetime.date.today()
        if self.today != today or self.most_recent_trading_day is None:
            self.today = today
            self.most_recent_trading_day = trading_day_helper.determine_closest_trading_date(self.market.trade_calendar)

        # prepare the progress bar
        local_index_num = local_index_list.count()
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
        if local_index_num > 0:

            # check the existence of each index
            for i, remote_index_item in remote_index_list.iterrows():
                code = remote_index_item['代码']
                name = remote_index_item['名称']
                query = local_index_list(code=code).first()
                if query:
                    # check the quote data freshness of each index
                    update_flag = self.check_stock_data_freshness(query)
                    update_counter_dict[update_flag] += 1
                    matched_index_counter += 1
                else:
                    # create absent stock index and create data retrieve task.
                    self.handle_new_stock(code=code, name=name, obj_type='stock_index')
                    unmatched_index_counter += 1
                prog_bar(i, remote_index_num)
            logger.info(f'Stock Market {self.market.name} - '
                        f'Checked {local_index_num} local indexes with {remote_index_num} remote data，')
            logger.info(f'Stock Market {self.market.name} - '
                        f'Checked quote data freshness of {matched_index_counter} local indexes. '
                        f'- Up to date:          {update_counter_dict["NO"]} '
                        f'- One day behind:    {update_counter_dict["UPD"]} '
                        f'- Need incremental update: {update_counter_dict["INC"]} \n'
                        f'- No local data:  {update_counter_dict["NEW"]} ')
        else:
            # create all stock index from the scratch and create data retrieve task.
            logger.info(f'Stock Market {self.market.name} - Local index data not found, initializing...')
            for i, remote_index_item in remote_index_list.iterrows():
                code = remote_index_item['代码']
                name = remote_index_item['名称']
                self.handle_new_stock(code=code, name=name, obj_type='stock_index')
                prog_bar(i, remote_index_num)
            logger.info(f'Stock Market {self.market.name} - '
                        f'Created local data and data retrieve tasks for {remote_index_num} stock indexes')

    def handle_new_stock(self, code, name, obj_type):
        if obj_type == 'stock_index':
            logger.debug(f'Stock Market {self.market.name} - Initializing local index data for {code}-{name}')
            new_stock_obj = StockIndex()
            datahub_task_handler = akshare_datahub_task
            task_name = f'GET FULL QUOTE FOR STOCK INDEX {code}-{name}'
            module = 'akshare'
            handler = 'get_zh_a_stock_index_quote_daily'
        else:
            logger.debug(f'Stock Market {self.market.name} - Initializing local stock data for {code}-{name}')
            new_stock_obj = IndividualStock()
            datahub_task_handler = baostock_datahub_task
            task_name = f'GET FULL QUOTE FOR STOCK {code}-{name}'
            module = 'baostock'
            handler = 'get_zh_a_stock_k_data_daily'

        new_stock_obj.code = code
        new_stock_obj.name = name
        new_stock_obj.market = self.market
        new_stock_obj.save()
        data_retrieve_kwarg = {
            'code': code
        }
        datahub_task_handler.create_task(name=task_name,
                                         module=module,
                                         handler=handler,
                                         task_kwarg_dict=data_retrieve_kwarg)

    def check_stock_data_freshness(self, stock_obj):
        """
        check the existence of the quote data, if not, get the full quote
        """
        module = None
        handler = None
        datahub_task_handler = akshare_datahub_task
        if type(stock_obj) == StockIndex:
            full_quote_task_name = f'GET FULL QUOTE FOR STOCK INDEX {stock_obj.code}-{stock_obj.name}'
            inc_quote_task_name = f'GET FULL QUOTE FOR INCREMENTAL UPD STOCK INDEX {stock_obj.code}-{stock_obj.name}'
            datahub_task_handler = akshare_datahub_task
            module = 'akshare'
            handler = 'get_zh_a_stock_index_quote_daily'
        elif type(stock_obj) == IndividualStock:
            full_quote_task_name = f'GET FULL QUOTE FOR STOCK {stock_obj.code}-{stock_obj.name}'
            inc_quote_task_name = f'GET FULL QUOTE FOR INCREMENTAL UPD STOCK {stock_obj.code}-{stock_obj.name}'
            datahub_task_handler = baostock_datahub_task
            module = 'baostock'
            handler = 'get_zh_a_stock_k_data_daily'
        else:
            full_quote_task_name = None
            inc_quote_task_name = None
            logger.warning(f'Stock Market {self.market.name} - OBJECT COMPARISON ERROR  {stock_obj.code}')
        if stock_obj.daily_quote:

            # determine time difference
            most_recent_quote_date = trading_day_helper.determine_latest_quote_date(stock_obj.daily_quote, 'date')
            time_diff = trading_day_helper.determine_trading_date_diff(self.market.trade_calendar,
                                                                       most_recent_quote_date,
                                                                       self.most_recent_trading_day)
            # create data update task
            if time_diff == 0:
                update_flag = "NO"
            elif time_diff == 1:
                update_flag = "UPD"  # Just update it with the most recent daily quote (difference of only 1 day)
            elif time_diff > 1:
                # Need the whole history quote data to do the incremental update (difference of more than 1 day)
                update_flag = "INC"
                start_date = trading_day_helper.next_trading_day(self.market.trade_calendar, most_recent_quote_date)
                data_retrieve_kwarg = {
                    'code': stock_obj.code,
                    'incremental': "true",
                    "start_date": start_date.strftime('%Y-%m-%d')
                }
                datahub_task_handler.create_task(name=inc_quote_task_name,
                                                 module=module,
                                                 handler=handler,
                                                 kwarg_dict=data_retrieve_kwarg)
            else:
                logger.warning(f'Stock Market {self.market.name} - {stock_obj.code} Quote date ahead of time!')
                update_flag = "NO"

        else:
            # no quote data at all
            update_flag = "NEW"
            data_retrieve_kwarg = {
                'code': stock_obj.code
            }
            datahub_task_handler.create_task(name=full_quote_task_name,
                                             module=module,
                                             handler=handler,
                                             task_kwarg_dict=data_retrieve_kwarg)
        return update_flag

    def check_stock_data_integrity(self):
        logger.info(f'Stock Market {self.market.name} - '
                    f'Checking local stock data integrity')
        local_stock_list = IndividualStock.objects(market=self.market)
        remote_stock_list = akshare_handler.get_zh_individual_stock_list()

        # prepare the progress bar
        local_stock_num = local_stock_list.count()
        remote_stock_num = len(remote_stock_list)
        prog_bar = progress_bar()
        counter_dict = {
            'NO': 0,
            "UPD": 0,
            "INC": 0,
            "NEW": 0
        }
        # check the existence of the stock list
        if local_stock_num > 0:
            # check the existence of each stock
            for i, remote_stock_item in remote_stock_list.iterrows():
                code = remote_stock_item['代码']
                name = remote_stock_item['名称']
                local_stock_obj = local_stock_list(code=code).first()
                if local_stock_obj:
                    # check the quote data freshness of each index
                    update_flag = self.check_stock_data_freshness(local_stock_obj)
                    counter_dict[update_flag] += 1
                else:
                    # create absent stock index and create data retrieve task.
                    self.handle_new_stock(code=code, name=name, obj_type='stock')
                prog_bar(i, remote_stock_num)
            logger.info(f'Stock Market {self.market.name} - '
                        f'Checked {local_stock_num} local stock data with {remote_stock_num} remote data，' 
                        f'- Up to date:          {counter_dict["NO"]} '
                        f'- One day behind:    {counter_dict["UPD"]} '
                        f'- Need incremental update: {counter_dict["INC"]} \n'
                        f'- No local data:  {counter_dict["NEW"]} ')
        else:
            logger.info(f'Stock Market {self.market.name} - Local stock data not found, initializing...')
            for i, remote_stock_item in remote_stock_list.iterrows():
                code = remote_stock_item['代码']
                name = remote_stock_item['名称']
                self.handle_new_stock(code=code, name=name, obj_type='stock')
                prog_bar(i, remote_stock_num)
            logger.info(f'Stock Market {self.market.name} - '
                        f'Created local data and data retrieve tasks for {remote_stock_num} stocks')
