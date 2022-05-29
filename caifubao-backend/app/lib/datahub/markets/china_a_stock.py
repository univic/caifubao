import time
import logging
import datetime
import traceback
from app.lib.datahub.remote_data.handler import zh_a_data
from app.model.stock import FinanceMarket, StockIndex, IndividualStock, StockDailyQuote
from app.lib.datahub.data_retriever import akshare_datahub_task, baostock_datahub_task
from app.utilities.progress_bar import progress_bar
from app.utilities import trading_day_helper, performance_helper

logger = logging.getLogger()


class ChinaAStock(object):

    def __init__(self):
        self.market_name = "A股"
        self.market_code = "ZH-A"
        self.today = datetime.date.today()
        self.most_recent_trading_day = None
        self.market = FinanceMarket.objects(name="A股").first()

    def initialize(self):
        self.check_market_data_existence()
        self.check_index_data_integrity()
        self.check_stock_data_integrity()

    def check_market_data_existence(self):
        self.market = FinanceMarket.objects(name="A股").first()
        # check the existence of basic market data
        if not self.market:
            logger.info(f'Stock Market {self.market.name} - Local market data not found, initializing')
            new_market = FinanceMarket()
            new_market.name = self.market_name
            new_market.code = self.market_code
            new_market.save()
            self.market = new_market
        else:
            logger.info(f'Stock Market {self.market.name} - Local market data check OK')
        self.check_trade_calendar_integrity()

    def check_trade_calendar_integrity(self):
        if self.market.trade_calendar:
            local_data_tail = self.market.trade_calendar[-1]
            today = datetime.datetime.today()
            if local_data_tail < today:
                logger.info(f"Stock Market {self.market.name} - Updating trade calendar")
                trade_calendar = zh_a_data.get_a_stock_trade_date_hist()
                self.market.trade_calendar = trade_calendar
                self.market.save()
            else:
                logger.info(f'Stock Market {self.market.name} - Trade calendar check OK')
        else:
            trade_calendar = zh_a_data.get_a_stock_trade_date_hist()
            self.market.trade_calendar = trade_calendar
            self.market.save()

    def check_index_data_integrity(self, allow_update=False):

        local_index_list = StockIndex.objects(market=self.market)
        remote_index_list = zh_a_data.get_zh_a_stock_index_spot()
        status = self.check_data_integrity(obj_name='index',
                                           local_data_list=local_index_list,
                                           remote_data_list=remote_index_list,
                                           new_quote_handler=self.handle_new_index_quote,
                                           hist_handler=self.get_hist_index_quote_data,
                                           new_obj_handler=self.handle_new_index,
                                           allow_update=allow_update)
        return status

    def check_stock_data_integrity(self, allow_update=False):
        local_stock_list = IndividualStock.objects(market=self.market)
        remote_stock_list = zh_a_data.get_zh_a_stock_spot()
        status = self.check_data_integrity(obj_name='index',
                                           local_data_list=local_stock_list,
                                           remote_data_list=remote_stock_list,
                                           new_quote_handler=self.handle_new_stock_quote,
                                           hist_handler=self.get_hist_stock_quote_data,
                                           new_obj_handler=self.handle_new_stock,
                                           allow_update=allow_update)
        return status

    def check_data_integrity(self, obj_name, local_data_list, remote_data_list, new_quote_handler,
                             hist_handler, new_obj_handler, allow_update=False, bulk_insert=False):
        logger.info(f'Stock Market {self.market.name} - '
                    f'Checking local {obj_name} data integrity, data update: {allow_update}')
        status_code = "GOOD"
        status_msg = ""
        check_counter_dict = {
            'GOOD': 0,
            "UPD": 0,
            "INC": 0,
            "FULL": 0,
            "WARN": 0,
            "NEW": 0
        }
        upd_counter_dict = {
            "UPD": 0,
            "INC": 0,
            "FULL": 0,
            "WARN": 0,
            "NEW": 0
        }
        self.perform_date_check()
        local_data_num = local_data_list.count()
        remote_data_num = len(remote_data_list)
        # prepare quote list for bulk insert
        new_quote_instance_list = []
        # prepare the progress bar
        prog_bar = progress_bar()

        # check the existence of the stock list
        if local_data_num > 0:
            # check the existence of each stock
            for i, remote_stock_item in remote_data_list.iterrows():
                code = remote_stock_item['code']
                name = remote_stock_item['name']
                stock_obj = local_data_list(code=code).only('code', 'data_freshness_meta').first()
                if stock_obj:
                    # check the quote data freshness of each index
                    flag = self.check_data_freshness(stock_obj)
                    check_counter_dict[flag] += 1
                    if allow_update:
                        if flag == "UPD":
                            most_recent_quote_date = trading_day_helper.read_freshness_meta(stock_obj, 'daily_quote')
                            save_quote = not bulk_insert
                            new_quote = new_quote_handler(stock_obj, remote_stock_item,
                                                          most_recent_quote_date, save_quote=save_quote)
                            new_quote_instance_list.append(new_quote)
                        elif flag == "INC":
                            most_recent_quote_date = trading_day_helper.read_freshness_meta(stock_obj, 'daily_quote')
                            start_date = trading_day_helper.next_trading_day(most_recent_quote_date)
                            hist_handler(code=code, start_date=start_date)
                        elif flag == "FULL":
                            hist_handler(code=code)
                        upd_counter_dict[flag] += 1
                else:
                    check_counter_dict["NEW"] += 1
                    if allow_update:
                        # create absent stock index and create data retrieve task.
                        new_obj_handler(code=code, name=name)
                        upd_counter_dict["NEW"] +=1
                prog_bar(i, remote_data_num)
            if bulk_insert:
                # do bulk insert
                StockDailyQuote.objects.insert(new_quote_instance_list, load_bulk=False)
            logger.info(f'Stock Market {self.market.name} - '
                        f'Checked {local_data_num} local {obj_name} data with {remote_data_num} remote data，'
                        f'- Up to date:          {check_counter_dict["GOOD"]} '
                        f'- One day behind:    {check_counter_dict["UPD"]} '
                        f'- Need incremental update: {check_counter_dict["INC"]}'
                        f'- No local data:  {check_counter_dict["FULL"]} '
                        f'- With warning: {check_counter_dict["WARN"]}')
            if allow_update:
                logger.info(f'Stock Market {self.market.name} - update attempt for {obj_name} data are as follows: '
                            f'- Update with spot data:    {upd_counter_dict["UPD"]} '
                            f'- Incremental update: {upd_counter_dict["INC"]}'
                            f'- Get full quote data:  {upd_counter_dict["FULL"]} '
                            f'- New stock:  {upd_counter_dict["NEW"]} '
                            f'- With warning: {upd_counter_dict["WARN"]}')
            else:
                logger.info(f'Stock Market {self.market.name} - no update attempt was made for {obj_name} data.')
        else:
            for i, remote_stock_item in remote_data_list.iterrows():
                code = remote_stock_item['代码']
                name = remote_stock_item['名称']
                new_obj_handler(code=code, name=name, market=self.market)
                prog_bar(i, remote_data_num)
        status = {"code": status_code,
                  "msg": status_msg}
        return status

    def check_data_freshness(self, stock_obj):
        most_recent_quote_date = trading_day_helper.read_freshness_meta(stock_obj, 'daily_quote')
        if most_recent_quote_date:
            # determine time difference
            time_diff = trading_day_helper.determine_trading_date_diff(self.market.trade_calendar,
                                                                       most_recent_quote_date,
                                                                       self.most_recent_trading_day)
            # create data update task
            if time_diff == 0:
                update_flag = "GOOD"
            elif time_diff == 1:
                update_flag = "UPD"  # Just update it with the most recent daily quote (difference of only 1 day)
            elif time_diff > 1:
                # Need the whole history quote data to do the incremental update (difference of more than 1 day)
                update_flag = "INC"
            else:
                logger.warning(f'Stock Market {self.market.name} - {stock_obj.code} Quote date ahead of time!')
                update_flag = "WARN"
        else:
            # no quote data at all
            update_flag = "FULL"
        return update_flag

    @staticmethod
    def handle_new_stock(code, name, market):
        logger.debug(f'Stock Market {market.name} - Initializing local stock data for {code}-{name}')
        new_stock_obj = IndividualStock()
        datahub_task_handler = baostock_datahub_task
        task_name = f'GET FULL QUOTE FOR STOCK {code}-{name}'
        module = 'baostock'
        handler = 'get_zh_a_stock_k_data_daily'
        new_stock_obj.code = code
        new_stock_obj.name = name
        new_stock_obj.market = market
        new_stock_obj.save()
        data_retrieve_kwarg = {
            'code': code
        }
        datahub_task_handler.create_task(name=task_name,
                                         package='remote_data',
                                         module=module,
                                         handler=handler,
                                         task_kwarg_dict=data_retrieve_kwarg)

    def handle_new_index(self, code, name):
        logger.debug(f'Stock Market {self.market.name} - Initializing local index data for {code}-{name}')
        new_stock_obj = StockIndex()
        datahub_task_handler = akshare_datahub_task
        task_name = f'GET FULL QUOTE FOR STOCK INDEX {code}-{name}'
        module = 'akshare'
        handler = 'get_zh_a_stock_index_quote_daily'
        new_stock_obj.code = code
        new_stock_obj.name = name
        new_stock_obj.market = self.market
        new_stock_obj.save()
        data_retrieve_kwarg = {
            'code': code
        }
        datahub_task_handler.create_task(name=task_name,
                                         package='remote_data',
                                         module=module,
                                         handler=handler,
                                         task_kwarg_dict=data_retrieve_kwarg)

    @staticmethod
    def handle_new_index_quote(index_obj, quote_obj, quote_date, save_quote=False):
        new_quote = StockDailyQuote()
        new_quote.code = index_obj.code
        new_quote.stock = index_obj
        new_quote.date = quote_date
        new_quote.open = quote_obj['open']
        new_quote.close = quote_obj['close']
        new_quote.high = quote_obj['high']
        new_quote.low = quote_obj['low']
        new_quote.volume = quote_obj['volume']
        trading_day_helper.update_freshness_meta(index_obj, 'daily_quote', quote_date)
        if save_quote:
            new_quote.save()
        index_obj.save()
        return new_quote

    @staticmethod
    def handle_new_stock_quote(stock_obj, quote_obj, quote_date, save_quote=False):
        new_quote = StockDailyQuote()
        new_quote.code = stock_obj.code
        new_quote.stock = stock_obj
        new_quote.date = quote_date
        new_quote.open = quote_obj['open']
        new_quote.close = quote_obj['close']
        new_quote.high = quote_obj['high']
        new_quote.low = quote_obj['low']
        new_quote.volume = quote_obj['volume']
        new_quote.change_rate = quote_obj['change_rate']
        new_quote.change_amount = quote_obj['change_amount']
        new_quote.volume = quote_obj['volume']
        new_quote.trade_amount = quote_obj['trade_amount']
        new_quote.previous_close = quote_obj['previous_close']
        new_quote.amplitude = quote_obj['amplitude']
        new_quote.turnover_rate = quote_obj['turnover_rate']
        new_quote.peTTM = quote_obj['peTTM']
        new_quote.psTTM = quote_obj['pbMRQ']
        trading_day_helper.update_freshness_meta(stock_obj, 'daily_quote', quote_date)
        if save_quote:
            new_quote.save()
        stock_obj.save()
        return new_quote

    # @performance_helper.func_performance_timer
    def get_hist_stock_quote_data(self, code, start_date=None, incremental=True, bulk_insert=True):
        status_code = "GOOD"
        status_msg = None
        try:
            stock_obj = IndividualStock.objects(code=code).only('code', 'name', 'data_freshness_meta').first()
            if stock_obj:
                quote_df = zh_a_data.get_zh_a_stock_hist_daily_quote(code, start_date=start_date)
                if not quote_df.empty:
                    # get column names of the df
                    bulk_insert_list = []
                    col_name_list = quote_df.columns.tolist()
                    for i, row in quote_df.iterrows():
                        daily_quote = StockDailyQuote()
                        daily_quote.code = stock_obj.code
                        daily_quote.stock = stock_obj
                        for col in col_name_list:
                            setattr(daily_quote, col, row[col])
                        # TODO: limit data precision
                        daily_quote.amplitude = daily_quote.high - daily_quote.low
                        daily_quote.change_amount = daily_quote.close - daily_quote.previous_close
                        if bulk_insert:
                            bulk_insert_list.append(daily_quote)
                        else:
                            daily_quote.save()
                    if bulk_insert:
                        # do bulk insert
                        StockDailyQuote.objects.insert(bulk_insert_list, load_bulk=False)
                    # update data freshness meta data
                    date_of_quote = quote_df['date'].max()
                    trading_day_helper.update_freshness_meta(stock_obj, 'daily_quote', date_of_quote)
                    stock_obj.save()
                else:
                    status_code = 'FAIL'
                    status_msg = 'No available data for update'
                    time.sleep(0.5)  # reduce the query frequency
            else:
                status_code = 'FAIL'
                status_msg = 'STOCK CODE CAN NOT BE FOUND IN LOCAL DB'
        except KeyError:
            status_code = 'FAIL'
            status_msg = 'the interface did not return valid dataframe, possibly due to no quote data'
        except Exception as e:
            status_code = 'FAIL'
            status_msg = ';'.join(traceback.format_exception(e))
        status = {
            'code': status_code,
            'message': status_msg,
        }
        return status

    def get_hist_index_quote_data(self, code, start_date=None, end_date=None, incremental=True):
        status_code = "GOOD"
        status_msg = None
        try:
            index_obj = StockIndex.objects(code=code).only('code', 'data_freshness_meta').first()
            most_recent_quote_date = trading_day_helper.read_freshness_meta(index_obj, 'daily_quote').date()
            # start_date = trading_day_helper.next_trading_day(most_recent_quote_date)
            quote_df = zh_a_data.get_zh_a_index_hist_daily_quote(code, start_date=start_date)
            if index_obj:
                if not quote_df.empty:
                    for i, row in quote_df.iterrows():
                        daily_quote = StockDailyQuote()
                        daily_quote.code = index_obj.code
                        daily_quote.stock = index_obj
                        daily_quote.date = row['date']
                        daily_quote.open = row['open']
                        daily_quote.close = row['close']
                        daily_quote.high = row['high']
                        daily_quote.low = row['low']
                        daily_quote.volume = row['volume']
                        daily_quote.save()
                    # update data freshness meta data
                    date_of_quote = quote_df['date'].max()
                    trading_day_helper.update_freshness_meta(index_obj, 'daily_quote', date_of_quote)
                    index_obj.save()
                else:
                    status_code = 'FAIL'
                    status_msg = 'No available data for update'
            else:
                status_code = 'FAIL'
                status_msg = 'INDEX CODE CAN NOT BE FOUND IN LOCAL DB'
            # time.sleep(0.5)    # reduce the query frequency
        except KeyError:
            status_code = 'FAIL'
            status_msg = 'the interface did not return valid dataframe, possibly due to no quote data'
        except Exception as e:
            status_code = 'FAIL'
            status_msg = ';'.join(traceback.format_exception(e))
        status = {
            'code': status_code,
            'message': status_msg,
        }
        return status

    def perform_date_check(self):
        # determine the closest trading day
        today = datetime.date.today()
        if self.today != today or self.most_recent_trading_day is None:
            self.today = today
            self.most_recent_trading_day = trading_day_helper.determine_closest_trading_date(self.market.trade_calendar)

    def perform_stock_name_check(self):
        pass

    def check_scheduled_task(self):
        pass

    def register_scheduled_task(self):
        pass

    def data_integrity_self_check(self):
        pass


if __name__ == '__main__':
    from app.lib.db_tool import mongoengine_tool
    from app.lib.datahub.remote_data import interface
    interface.baostock.establish_baostock_conn()
    mongoengine_tool.connect_to_db()
    obj = ChinaAStock()
    o = obj.get_hist_stock_quote_data(code="sh600815")
    print(o)
