import os
import time
import datetime
import logging
from app.lib.database import connect_to_db, disconnect_from_db
from app.conf import app_config
from app.model.data_retrive import DatahubTaskDoc, ScheduledDatahubTaskDoc
from app.utilities.progress_bar import progress_bar
from app.lib.datahub.remote_data import baostock
from app.lib.datahub.data_retriever.common import convert_dict_to_kwarg, check_task_uniqueness, \
    exec_datahub_task, convert_kwarg_to_dict
from app.utilities import trading_day_helper

logger = logging.getLogger()


class DatahubTask(object):

    def __init__(self, runner_name='General', task_obj=DatahubTaskDoc):
        self.runner_name = runner_name
        self.db_alias = runner_name
        self.task_obj = task_obj
        self.task_list = []
        self.task_list_length = 0
        self.task_scan_interval = app_config.DATAHUB_SETTINGS["TASK_SCAN_INTERVAL"]
        self.continue_scan = True

    def dispatch(self):
        logger.info(f'{self.runner_name} task worker - Running in process {os.getpid()}, '
                    f'task scan interval {self.task_scan_interval} minutes')
        # if nothing goes wrong check new tasks regularly
        while self.continue_scan:
            logger.info(f'{self.runner_name} task worker - Preparing for task scan')

            connect_to_db()
            self.before_dispatch()
            self.get_task_list()
            self.task_list_length = len(self.task_list)
            if self.task_list_length > 0:
                logger.info(f'{self.runner_name} task worker - found {self.task_list_length} task(s), now executing')
                self.exec_task_list()
            else:
                logger.info(f'{self.runner_name} task worker - no available task，waiting for next scan')
            logger.info(f'{self.runner_name} task worker - task scan completed, '
                        f'next scan in {self.task_scan_interval} minutes')
            self.after_dispatch()
            disconnect_from_db()
            time_to_wait = self.task_scan_interval * 60
            time.sleep(time_to_wait)

    def get_task_list(self):
        # get task list and calculate list length
        self.task_list = self.task_obj.objects(status='CRTD')  # Slice here to limit task number
        return self.task_list

    def exec_task_list(self):
        self.before_task_list_exec()
        prog_bar = progress_bar()
        counter = {
            "COMP": 0,
            "FAIL": 0
        }
        for i, item in enumerate(self.task_list):
            result = self.exec_task(item)
            if result['code'] == 'GOOD':
                counter["COMP"] += 1
            else:
                counter["FAIL"] += 1
            prog_bar(i, self.task_list_length)
        logger.info(f'Processed {self.task_list_length} tasks, '
                    f'{counter["COMP"]} success, {counter["FAIL"]} failed.')
        self.after_task_list_exec()

    def exec_task(self, item):
        self.before_task_exec(item)
        result = exec_datahub_task(item)
        self.after_task_exec(item)
        return result

    def before_dispatch(self):
        pass

    def after_dispatch(self):
        pass

    def before_task_list_exec(self):
        pass

    def after_task_list_exec(self):
        pass

    def before_task_exec(self, item):
        pass

    def after_task_exec(self, item):
        pass

    def create_task(self, name, package, module, handler, task_args_list=None, task_kwarg_dict=None, **extra_kw):
        new_task = self.task_obj()
        new_task.name = name
        new_task.callback_package = package
        new_task.callback_module = module
        new_task.callback_handler = handler
        new_task.args = task_args_list
        if task_kwarg_dict:
            new_task.kwargs = convert_dict_to_kwarg(task_kwarg_dict)
        if check_task_uniqueness(new_task, task_kwarg_dict):
            new_task.save()
            logger.debug(f'Data retrieve task {new_task.name} created')
        else:
            logger.debug(f'Found duplicate data retrieve task {new_task.name}')


class AkshareDatahubTask(DatahubTask):
    def __init__(self):
        super().__init__(runner_name='Akshare', task_obj=DatahubTaskDoc)

    def get_task_list(self):
        self.task_list = self.task_obj.objects(status='CRTD', callback_module='akshare')[:]  # Slice here to limit task number
        return self.task_list


class BaostockDatahubTask(DatahubTask):
    def __init__(self):
        super().__init__(runner_name='Baostock', task_obj=DatahubTaskDoc)

    def get_task_list(self):
        self.task_list = self.task_obj.objects(status='CRTD', callback_module='baostock')[:]  # Slice here to limit task number
        return self.task_list

    def before_task_list_exec(self):
        baostock.interface.establish_baostock_conn()

    def after_task_list_exec(self):
        baostock.interface.terminate_baostock_conn()


class ScheduledDatahubTask(DatahubTask):
    """
    REPEAT: DAY, T-DAY(EACH TRADING DAY), WEEK, BI-WEEK, MONTH, YEAR
    """

    def __init__(self):
        super().__init__(runner_name='Scheduled', task_obj=ScheduledDatahubTaskDoc)
        # self.initialize_scheduled_task()
        # TODO: WHERE TO INIT?

    def before_dispatch(self):
        self.initialize_scheduled_task()

    def initialize_scheduled_task(self):
        task_num = self.task_obj.objects().count()

        if task_num == 0:
            logger.info(f'Initializing scheduled datahub tasks')
            trade_calendar = trading_day_helper.get_a_stock_market_trade_calendar()
            run_hour = 18
            date_str = datetime.date.today().strftime('%Y%m%d')
            if trading_day_helper.is_trading_day(trade_calendar):
                next_run_time = datetime.datetime.now()
            else:
                next_run_time = trading_day_helper.next_trading_day(trade_calendar)
            next_run_time = next_run_time.replace(hour=run_hour, minute=0, second=0)

            self.create_task(name=f'UPDATE INDEX QUOTE WITH SPOT DATA {date_str}',
                             package='remote_data',
                             module='akshare',
                             handler='get_zh_a_stock_index_quote_daily',
                             repeat='T-DAY',
                             scheduled_time=next_run_time)
            self.create_task(name=f'UPDATE STOCK QUOTE WITH SPOT DATA {date_str}',
                             package='remote_data',
                             module='akshare',
                             handler='get_zh_a_stock_quote_daily',
                             repeat='T-DAY',
                             scheduled_time=next_run_time)
            logger.info(f'Scheduled datahub tasks Initialized')

    def get_task_list(self):
        self.task_list = self.task_obj.objects(status='CRTD').order_by('-scheduled_time')  # Slice here to limit task number
        return self.task_list

    def exec_task_list(self):
        continue_flag = True
        task_scan_interval = 30  # minutes
        next_scan_second = task_scan_interval * 60
        counter = {
            "COMP": 0,
            "FAIL": 0
        }
        while continue_flag:
            task_list = self.get_task_list()
            logger.info(f'Found {self.task_list_length} scheduled task(s)')
            if task_list:
                for i, item in enumerate(task_list):
                    scheduled_run_time = item.scheduled_process_time
                    time_diff = datetime.datetime.now() - scheduled_run_time
                    time_diff_second = time_diff.seconds
                    if time_diff_second <= 0:
                        next_run = 0
                    if time_diff_second > next_scan_second:
                        next_run = -1
                    else:
                        next_run = time_diff_second
                    if next_run >= 0:
                        logger.info(f'Will run task {item.name} in {next_run} seconds')
                        time.sleep(next_run)
                        logger.info(f'Running task {item.name}')
                        result = self.exec_task(item)
                        if result['code'] == 'GOOD':
                            logger.info(f'Successfully processed task {item.name}')
                        else:
                            counter["FAIL"] += 1
                            logger.info(f'Error when processing task {item.name}')
                logger.info(f'Task scan completed, next scan in {task_scan_interval} minutes')
            else:
                logger.info(f'No scheduled task was found, next scan in {task_scan_interval} minuets')
                time.sleep(next_scan_second)

    def before_task_exec(self, item):
        if item.callback_module == 'baostock':
            baostock.interface.establish_baostock_conn()

    def after_task_exec(self, item):
        if item.callback_module == 'baostock':
            baostock.interface.terminate_baostock_conn()
        self.handle_repeat_task(item)

    def handle_repeat_task(self, item):
        if item.repeat:

            if item.repeat == 'T-DAY':
                trade_calendar = item.market.trade_calendar
                curr_run_time = item.scheduled_time
                next_run_time = trading_day_helper.next_trading_day(trade_calendar)
                next_run_time += datetime.timedelta(hours=curr_run_time.hour,
                                                    minutes=curr_run_time.minute,
                                                    seconds=curr_run_time.second)

            else:
                next_run_time = None
            # create task
            kw_dict = convert_kwarg_to_dict(item.kwargs)
            self.create_task(name=item.name,
                             package=item.callback_package,
                             module=item.callback_module,
                             handler=item.callback_handler,
                             repeat=item.repeat,
                             args=item.args,
                             task_kwarg_dict=kw_dict,
                             scheduled_time=next_run_time)

    def create_task(self, name, package, module, handler, task_args_list=None, task_kwarg_dict=None, **extra_kw):
        new_task = self.task_obj()
        new_task.scheduled_process_time = extra_kw["scheduled_time"]
        new_task.name = name
        new_task.callback_package = package
        new_task.callback_module = module
        new_task.callback_handler = handler
        new_task.repeat = extra_kw["repeat"]
        new_task.args = task_args_list
        if new_task.kwargs:
            new_task.kwargs = convert_dict_to_kwarg(task_kwarg_dict)
        if check_task_uniqueness(new_task, task_kwarg_dict):
            new_task.save()
            logger.debug(f'Scheduled datahub task {new_task.name} created')
        else:
            logger.debug(f'Found duplicate task {new_task.name}')
