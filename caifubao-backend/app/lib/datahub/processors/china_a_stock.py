import time
import logging
import datetime
import traceback
from app.lib.datahub.data_source.handler import zh_a_data
from app.model.stock import FinanceMarket, StockIndex, IndividualStock, StockDailyQuote
# from app.lib.task_controller import task_controller
from app.lib.datahub.data_source.interface.baostock_interface import BaostockInterfaceManager
from app.utilities.progress_bar import progress_bar
from app.utilities import trading_day_helper, freshness_meta_helper

logger = logging.getLogger(__name__)


class ChinaAStock(object):

    def __init__(self):
        self.market_name = "A股"
        self.market_code = "ZH-A"
        self.today = datetime.date.today()
        self.most_recent_trading_day = None
        self.market = FinanceMarket.objects(name="A股").first()
        self.trade_calendar = None
        self.result: dict = {
            'code': "GOOD",
            'message': "",
        }

    def run(self):

        self.check_market_data_existence()
        self.check_trade_calendar_integrity()
        # self.check_scheduled_task()
        self.check_index_data_integrity(allow_update=True)
        baostock_conn_mgr = BaostockInterfaceManager()
        baostock_conn_mgr.establish_baostock_conn()
        self.check_stock_data_integrity(allow_update=True)
        return self.result

    def check_market_data_existence(self):
        # check the existence of basic market data
        if not self.market:
            logger.info(f'Stock Market {self.market_name} - Local market data not found, initializing')
            new_market = FinanceMarket()
            new_market.name = self.market_name
            new_market.code = self.market_code
            new_market.save()
            self.market = new_market
        else:
            logger.info(f'Stock Market {self.market.name} - Local market data check OK')

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
        status = self.check_data_integrity(obj_type='index',
                                           local_data_list=local_index_list,
                                           remote_data_df=remote_index_list,
                                           hist_handler='get_hist_index_quote_data',
                                           allow_update=allow_update)
        return status

    def check_stock_data_integrity(self, allow_update=False):
        local_stock_list = IndividualStock.objects(market=self.market)
        remote_stock_list = zh_a_data.get_zh_a_stock_spot()
        status = self.check_data_integrity(obj_type='stock',
                                           local_data_list=local_stock_list,
                                           remote_data_df=remote_stock_list,
                                           hist_handler='get_hist_stock_quote_data',
                                           allow_update=allow_update)
        return status

    def check_data_integrity(self, obj_type, local_data_list, remote_data_df,
                             hist_handler, allow_update=False, bulk_insert=False):
        logger.info(f'Stock Market {self.market.name} - '
                    f'Checking local {obj_type} data integrity, data update: {allow_update}')
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
            'GOOD': 0,
            "UPD": 0,
            "INC": 0,
            "FULL": 0,
            "WARN": 0,
            "NEW": 0
        }
        self.perform_date_check()
        local_data_num = local_data_list.count()
        remote_data_num = len(remote_data_df)
        remote_data_col_list = remote_data_df.columns.tolist()
        # prepare quote list for bulk insert
        new_quote_instance_list = []
        # prepare the progress bar
        prog_bar = progress_bar()

        # check the existence of the stock list
        if local_data_num > 0:
            # check the existence of each stock
            for i, remote_stock_item in remote_data_df.iterrows():
                code = remote_stock_item['code']
                name = remote_stock_item['name']
                stock_obj = local_data_list(code=code).first()
                prog_bar_msg: str = ""
                if stock_obj:
                    # check the quote data freshness of each index
                    flag = self.check_data_freshness(stock_obj)
                    check_counter_dict[flag] += 1
                    if allow_update:
                        self.perform_stock_name_check(stock_obj, name)
                        if flag == "UPD":
                            prog_bar_msg = f"Updating quote info for {code} - {name} with spot data"
                            quote_date = self.most_recent_trading_day
                            save_quote = not bulk_insert
                            new_quote = self.handle_new_quote(stock_obj, remote_data_col_list, remote_stock_item,
                                                              quote_date, save_quote=save_quote)
                            new_quote_instance_list.append(new_quote)
                        elif flag in ["INC", "FULL"]:
                            prog_bar_msg = f"Doing {flag} update for {code} - {name}"
                            self.get_hist_quote_data(stock_obj=stock_obj, hist_quote_handler=hist_handler)

                        upd_counter_dict[flag] += 1
                else:
                    prog_bar_msg = f"Get quote info for new stock {code} - {name}"
                    check_counter_dict["NEW"] += 1
                    if allow_update:
                        # create absent stock index and create data retrieve task.
                        self.handle_new_stock(obj_type=obj_type, code=code, name=name)
                        upd_counter_dict["NEW"] += 1
                prog_bar(i, remote_data_num, prog_bar_msg)
            if bulk_insert:
                # do bulk insert
                StockDailyQuote.objects.insert(new_quote_instance_list, load_bulk=False)
            msg_str = (f'Stock Market {self.market.name} - '
                       f'Checked {local_data_num} local {obj_type} data with {remote_data_num} remote data，'
                       f'- Up to date:          {check_counter_dict["GOOD"]} '
                       f'- One day behind:    {check_counter_dict["UPD"]} '
                       f'- Need incremental update: {check_counter_dict["INC"]}'
                       f'- No local data:  {check_counter_dict["FULL"]} '
                       f'- With warning: {check_counter_dict["WARN"]}')
            logger.info(msg_str)
            status_msg += msg_str
            if allow_update:
                msg_str = (f'Stock Market {self.market.name} - update attempt for {obj_type} data are as follows: '
                           f'- Update with spot data:  {upd_counter_dict["UPD"]} '
                           f'- Incremental update: {upd_counter_dict["INC"]}'
                           f'- Get full quote data:  {upd_counter_dict["FULL"]} '
                           f'- New stock:  {upd_counter_dict["NEW"]} '
                           f'- With warning: {upd_counter_dict["WARN"]}')
                logger.info(msg_str)
                status_msg += msg_str
            else:
                msg_str = f'Stock Market {self.market.name} - no update attempt was made for {obj_type} data.'
                logger.info(msg_str)
                status_msg += msg_str
        else:
            if allow_update:
                for i, remote_stock_item in remote_data_df.iterrows():
                    code = remote_stock_item['code']
                    name = remote_stock_item['name']
                    self.handle_new_stock(obj_type=obj_type, code=code, name=name)
                    prog_bar(i, remote_data_num)
        status = {"code": status_code,
                  "msg": status_msg}
        return status

    def check_data_freshness(self, stock_obj):
        most_recent_quote_date = freshness_meta_helper.read_freshness_meta(code=stock_obj.code,
                                                                           object_type=stock_obj.object_type,
                                                                           meta_type='quote',
                                                                           meta_name='daily_quote')
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

    def handle_new_stock(self, obj_type, code, name):
        """
        handle new stock or index, will create a master data and a task, which get its quote data
        :param obj_type: stock or index
        :param code:
        :param name:
        :return:
        """
        logger.info(f'Stock Market {self.market.name} - Initializing local data for {code}-{name}')
        new_stock_obj = None
        task_name = ""
        handler = ""
        object_type = ""
        if obj_type == 'stock':
            new_stock_obj = IndividualStock()
            object_type = "individual_stock"
            task_name = f'GET FULL QUOTE FOR STOCK {code}-{name}'
            handler = 'get_hist_stock_quote_data'
        elif obj_type == 'index':
            new_stock_obj = StockIndex()
            object_type = "stock_index"
            task_name = f'GET FULL QUOTE FOR STOCK INDEX {code}-{name}'
            handler = 'get_hist_index_quote_data'
        else:
            logger.error(f'Stock Market {self.market.name} - Invalid category {obj_type}')
        new_stock_obj.code = code
        new_stock_obj.name = name
        new_stock_obj.object_type = object_type
        new_stock_obj.market = self.market
        new_stock_obj.save()
        # task_kwarg = {
        #     'code': code
        # }
        self.get_hist_index_quote_data(code=code)
        # task_controller.create_task(name=task_name,
        #                             callback_package='datahub',
        #                             callback_module='processors',
        #                             callback_object='ChinaAStock',
        #                             callback_handler=handler,
        #                             kwargs=task_kwarg)

    @staticmethod
    def handle_new_quote(stock_obj, col_name_list, quote_row, quote_date=None, save_quote=False):
        new_quote = StockDailyQuote()
        for col in col_name_list:
            setattr(new_quote, col, quote_row[col])
        new_quote.code = stock_obj.code
        new_quote.stock = stock_obj
        if quote_date:
            new_quote.date = quote_date
        freshness_meta_helper.upsert_freshness_meta(code=stock_obj.code,
                                                    object_type=stock_obj.object_type,
                                                    meta_type='quote',
                                                    meta_name='daily_quote',
                                                    dt=quote_date)
        if save_quote:
            new_quote.save()
        stock_obj.save()
        return new_quote

    def get_hist_quote_data(self, stock_obj, hist_quote_handler, force_upd=False):
        start_date = None
        start_date_str = None
        most_recent_quote_date = freshness_meta_helper.read_freshness_meta(code=stock_obj.code,
                                                                           object_type=stock_obj.object_type,
                                                                           meta_type='quote',
                                                                           meta_name='daily_quote')
        if most_recent_quote_date:
            start_date = trading_day_helper.next_trading_day(self.market.trade_calendar, most_recent_quote_date)
            start_date_str = start_date.strftime('%Y-%m-%d')
            task_name = f'Get quote data from {start_date_str}for {stock_obj.code}-{stock_obj.name}'
        else:
            task_name = f'Get full quote data for {stock_obj.code}-{stock_obj.name}'
        kwarg_dict = {
            'code': stock_obj.code,
        }
        # logger.info(task_name)
        if start_date:
            kwarg_dict['start_date'] = start_date.strftime('%Y-%m-%d')
        func = getattr(self, hist_quote_handler)
        result = func(code=stock_obj.code, start_date=start_date_str)
        if result["code"] != "GOOD":
            logger.warning(f"Something goes wrong when "
                           f"trying to get historic quote data for {stock_obj.code} - {stock_obj.name}\n"
                           f"{result['message']}")

        # task_controller.create_task(name=task_name,
        #                             callback_package='datahub',
        #                             callback_module='markets',
        #                             callback_object='zh_a_stock_market',
        #                             callback_handler=hist_quote_handler,
        #                             kwargs=kwarg_dict)

    # @performance_helper.func_performance_timer
    def get_hist_stock_quote_data(self, code, start_date=None, force_insert=False, bulk_insert=True):
        status_code = "GOOD"
        status_msg = None
        try:
            stock_obj = IndividualStock.objects(code=code).only('code', 'name').first()
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
                        daily_quote.amplitude = round(daily_quote.high - daily_quote.low, 2)
                        daily_quote.change_amount = round(daily_quote.close - daily_quote.previous_close, 2)
                        if bulk_insert:
                            bulk_insert_list.append(daily_quote)
                        else:
                            daily_quote.save(force_insert=force_insert)
                    if bulk_insert:
                        # do bulk insert
                        StockDailyQuote.objects.insert(bulk_insert_list, load_bulk=False)
                    # update data freshness meta data
                    date_of_quote = quote_df['date'].max()
                    # trading_day_helper.update_freshness_meta(stock_obj, 'daily_quote', date_of_quote)
                    freshness_meta_helper.upsert_freshness_meta(code=stock_obj.code,
                                                                object_type=stock_obj.object_type,
                                                                meta_type='quote',
                                                                meta_name='daily_quote',
                                                                dt=date_of_quote)
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
        # except Exception as e:
        #     status_code = 'ERR'
        #     status_msg = ';'.join(traceback.format_exception(e))
        status = {
            'code': status_code,
            'message': status_msg,
        }
        return status

    @staticmethod
    def get_hist_index_quote_data(code, start_date=None, end_date=None, force_insert=False, bulk_insert=True):
        """

        :param code:
        :param start_date:
        :param end_date:
        :param force_insert: only works when bulk insert is false!
        :param bulk_insert:
        :return:
        """
        status_code = "GOOD"
        status_msg = None
        try:
            index_obj = StockIndex.objects(code=code).only('code', 'name').first()
            quote_df = zh_a_data.get_zh_a_index_hist_daily_quote(code, start_date=start_date)
            if index_obj:
                if not quote_df.empty:
                    bulk_insert_list = []
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
                        if bulk_insert:
                            bulk_insert_list.append(daily_quote)
                        else:
                            daily_quote.save()
                    if bulk_insert:
                        # do bulk insert
                        StockDailyQuote.objects.insert(bulk_insert_list, load_bulk=False)
                    # update data freshness meta data
                    date_of_quote = quote_df['date'].max()
                    freshness_meta_helper.upsert_freshness_meta(code=index_obj.code,
                                                                object_type=index_obj.object_type,
                                                                meta_type='quote',
                                                                meta_name='daily_quote',
                                                                dt=date_of_quote)
                    index_obj.save(force_insert=force_insert)
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
        # except Exception as e:
        #     status_code = 'ERR'
        #     status_msg = ';'.join(traceback.format_exception(e))
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

    @staticmethod
    def perform_stock_name_check(stock_obj, curr_name):
        if stock_obj.name != curr_name and curr_name not in stock_obj.pre_name:
            stock_obj.pre_name.append(stock_obj.name)
            stock_obj.name = curr_name
            stock_obj.save()

    # def check_scheduled_task(self):
    #     # check the existence of index task
    #     index_task_ok_flag = False
    #     stock_task_ok_flag = False
    #     run_hour = 18
    #     task_num = DatahubTaskDoc.objects(status="CRTD",
    #                                       name__startswith="Check index data integrity").count()
    #     data_retrieve_kwarg = {
    #         'allow_update': 'True'
    #     }
    #     if task_num == 0:
    #         logger.info(f'Stock Market {self.market.name} - Initializing scheduled index data integrity task')
    #         if trading_day_helper.is_trading_day(self.trade_calendar):
    #             next_run_time = datetime.datetime.now()
    #         else:
    #             next_run_time = trading_day_helper.next_trading_day(self.trade_calendar)
    #         next_run_time = next_run_time.replace(hour=run_hour, minute=0, second=0)
    #         scheduled_datahub_task.create_task(
    #             name=trading_day_helper.update_title_date_str('Check index data integrity', next_run_time),
    #             package='datahub',
    #             module='markets',
    #             obj='zh_a_stock_market',
    #             interface='akshare',
    #             handler='check_index_data_integrity',
    #             repeat='T-DAY',
    #             scheduled_time=next_run_time,
    #             task_kwarg_dict=data_retrieve_kwarg)
    #     else:
    #         index_task_ok_flag = True
    #     # check the existence of stock task
    #     task_num = DatahubTaskDoc.objects(status="CRTD",
    #                                       name__startswith="Check stock data integrity").count()
    #     if task_num == 0:
    #         logger.info(f'Stock Market {self.market.name} - Initializing scheduled stock data integrity task')
    #         if trading_day_helper.is_trading_day(self.trade_calendar):
    #             next_run_time = datetime.datetime.now()
    #         else:
    #             next_run_time = trading_day_helper.next_trading_day(self.trade_calendar)
    #         next_run_time = next_run_time.replace(hour=run_hour, minute=0, second=0)
    #
    #         scheduled_datahub_task.create_task(
    #             name=trading_day_helper.update_title_date_str('Check stock data integrity', next_run_time),
    #             package='datahub',
    #             module='markets',
    #             obj='zh_a_stock_market',
    #             interface='akshare',
    #             handler='check_stock_data_integrity',
    #             repeat='T-DAY',
    #             scheduled_time=next_run_time,
    #             task_kwarg_dict=data_retrieve_kwarg)
    #     else:
    #         stock_task_ok_flag = True
    #     if stock_task_ok_flag and index_task_ok_flag:
    #         logger.info(f'Stock Market {self.market.name} - Scheduled data update task check OK')


if __name__ == '__main__':
    from app.lib.datahub.data_source import interface
    interface.baostock_interface.establish_baostock_conn()
    obj = ChinaAStock()
    o = obj.get_hist_index_quote_data(code="sh000061", force_insert=True, bulk_insert=False)
    print(o)
