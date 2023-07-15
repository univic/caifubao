import os
import time
import datetime
import logging
from app.lib.db_tool.mongoengine_tool import connect_to_db, disconnect_from_db
from app.conf import app_config
from app.model.data_retrive import DatahubTaskDoc, ScheduledDatahubTaskDoc
from app.utilities.progress_bar import progress_bar
from app.lib.datahub.data_source.interface import baostock_interface as baostock_if
from app.lib.task_controller.common import convert_dict_to_kwarg, check_task_uniqueness, \
    exec_task, convert_kwarg_to_dict
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
                logger.info(f'{self.runner_name} task worker - found {self.task_list_length} task(s)')
                self.before_task_list_exec()
                self.exec_task_list()
                self.after_task_list_exec()
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
        prog_bar = progress_bar()
        counter = {
            "COMP": 0,
            "WARN": 0,
            "FAIL": 0,
            "ERR": 0,
        }
        for i, item in enumerate(self.task_list):
            result = self.exec_task(item)
            if result['code'] == 'GOOD':
                counter["COMP"] += 1
            elif result['code'] == 'WARN':
                counter["WARN"] += 1
            elif result['code'] == 'ERR':
                counter["ERR"] += 1
            else:
                counter["FAIL"] += 1
            prog_bar(i, self.task_list_length)
        logger.info(f'{self.runner_name} task worker - Processed {self.task_list_length} tasks, '
                    f'{counter["COMP"]} success, {counter["WARN"]} completed with warning, {counter["FAIL"]} failed, '
                    f'{counter["ERR"]} encountered error. ')

    def exec_task(self, item):
        # result = {"code": "GOOD"}
        self.before_task_exec(item)
        result = exec_task(item)
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

    def create_task(self, name, package, module, obj, handler, interface, task_args_list=None, task_kwarg_dict=None, **extra_kw):
        new_task = self.task_obj()
        new_task.name = name
        new_task.callback_package = package
        new_task.callback_module = module
        new_task.callback_object = obj
        new_task.callback_handler = handler
        new_task.callback_interface = interface
        new_task.args = task_args_list
        if task_kwarg_dict:
            new_task.kwargs = convert_dict_to_kwarg(task_kwarg_dict)
        if check_task_uniqueness(new_task, task_kwarg_dict):
            new_task.save()
            logger.debug(f'{self.runner_name} task worker - Data retrieve task {new_task.name} created')
        else:
            logger.debug(f'{self.runner_name} task worker - Found duplicate data retrieve task {new_task.name}')


class AkshareDatahubTask(DatahubTask):
    def __init__(self):
        super().__init__(runner_name='Akshare', task_obj=DatahubTaskDoc)

    def get_task_list(self):
        # Slice here to limit task number
        self.task_list = self.task_obj.objects(_cls='DatahubTaskDoc', status='CRTD', callback_interface='akshare')[:]
        return self.task_list

    def create_task(self, name, package, module, obj, handler, interface='akshare',
                    task_args_list=None, task_kwarg_dict=None, **extra_kw):
        super().create_task(name, package, module, obj, handler, interface, task_args_list, task_kwarg_dict, **extra_kw)


class BaostockDatahubTask(DatahubTask):
    def __init__(self):
        super().__init__(runner_name='Baostock', task_obj=DatahubTaskDoc)

    def get_task_list(self):
        # Slice here to limit task number
        self.task_list = self.task_obj.objects(_cls='DatahubTaskDoc', status='CRTD', callback_interface='baostock')[:]
        return self.task_list

    def create_task(self, name, package, module, obj, handler, interface='baostock',
                    task_args_list=None, task_kwarg_dict=None, **extra_kw):
        super().create_task(name, package, module, obj, handler, interface, task_args_list, task_kwarg_dict, **extra_kw)

    def before_task_list_exec(self):
        baostock_if.establish_baostock_conn()

    def after_task_list_exec(self):
        baostock_if.terminate_baostock_conn()


class ScheduledDatahubTask(DatahubTask):
    """
    REPEAT: DAY, T-DAY(EACH TRADING DAY), WEEK, BI-WEEK, MONTH, YEAR
    """

    def __init__(self):
        super().__init__(runner_name='Scheduled', task_obj=ScheduledDatahubTaskDoc)

    def before_dispatch(self):
        pass

    def get_task_list(self):
        self.task_list = self.task_obj.objects(status='CRTD').order_by('-scheduled_time')  # Slice here to limit task number
        return self.task_list

    def exec_task_list(self):
        if self.task_list:
            next_scan_second = self.task_scan_interval * 60
            counter = {
                "COMP": 0,
                "WARN": 0,
                "FAIL": 0,
                "SKIP": 0,
                "ERR": 0
            }
            for i, item in enumerate(self.task_list):
                scheduled_run_timestamp = datetime.datetime.timestamp(item.scheduled_process_time)
                time_diff = scheduled_run_timestamp - datetime.datetime.timestamp(datetime.datetime.now())
                # determine whether to execute
                if time_diff <= 0:
                    next_run = 0
                    exec_flag = True
                elif time_diff > next_scan_second:
                    next_run = -1
                    counter["SKIP"] += 1
                    exec_flag = False
                else:
                    exec_flag = True
                    next_run = time_diff

                if exec_flag:
                    logger.info(f'{self.runner_name} task worker - Will run task {item.name} in {int(next_run)} seconds')
                    time.sleep(next_run)
                    logger.info(f'{self.runner_name} task worker - Running task {item.name}')
                    result = self.exec_task(item)
                    if result['code'] == 'GOOD':
                        counter["COMP"] += 1
                        logger.info(f'{self.runner_name} task worker - Successfully processed task {item.name}')
                    elif result['code'] == 'WARN':
                        counter["WARN"] += 1
                        logger.info(f'{self.runner_name} task worker - Task {item.name} completed with warning message')
                    elif result['code'] == 'ERR':
                        counter["ERR"] += 1
                        logger.info(f'{self.runner_name} task worker - Error when processing task {item.name}')
                    else:
                        counter["FAIL"] += 1
                        logger.info(f'{self.runner_name} task worker - task {item.name} has failed')
            logger.info(f'{self.runner_name} task worker - Task execution completed, '
                        f'of all {self.task_list_length} task(s) '
                        f'{counter["WARN"]} completed with warning, '
                        f'{counter["COMP"]} success, {counter["FAIL"]} failed, {counter["SKIP"]} skipped, '
                        f'{counter["ERR"]} encountered error. '
                        f'next scan in {self.task_scan_interval} minutes')
        else:
            logger.info(f'{self.runner_name} task worker - No scheduled task was found, '
                        f'next scan in {self.task_scan_interval} minuets')

    def before_task_exec(self, item):
        if item.callback_interface == 'baostock':
            baostock_if.establish_baostock_conn()

    def after_task_exec(self, item):
        if item.callback_interface == 'baostock':
            baostock_if.terminate_baostock_conn()
        self.handle_repeat_task(item)

    def handle_repeat_task(self, item):
        if item.repeat:
            if item.repeat == 'T-DAY':
                trade_calendar = trading_day_helper.get_a_stock_market_trade_calendar()
                curr_run_time = item.scheduled_process_time
                next_run_time = trading_day_helper.next_trading_day(trade_calendar)
                next_run_time += datetime.timedelta(hours=curr_run_time.hour,
                                                    minutes=curr_run_time.minute,
                                                    seconds=curr_run_time.second)
            else:
                next_run_time = None
            # create task
            kw_dict = convert_kwarg_to_dict(item.kwargs)
            self.create_task(name=trading_day_helper.update_title_date_str(item.name, next_run_time),
                             package=item.callback_package,
                             module=item.callback_module,
                             obj=item.callback_object,
                             handler=item.callback_handler,
                             interface=item.callback_interface,
                             repeat=item.repeat,
                             args=item.args,
                             task_kwarg_dict=kw_dict,
                             scheduled_time=next_run_time)

    def create_task(self, name, package, module, obj, handler, interface,
                    task_args_list=None, task_kwarg_dict=None, **extra_kw):
        new_task = self.task_obj()
        new_task.scheduled_process_time = extra_kw["scheduled_time"]
        new_task.name = name
        new_task.callback_package = package
        new_task.callback_module = module
        new_task.callback_object = obj
        new_task.callback_handler = handler
        new_task.callback_interface = interface
        new_task.repeat = extra_kw["repeat"]
        new_task.args = task_args_list
        if task_kwarg_dict:
            new_task.kwargs = convert_dict_to_kwarg(task_kwarg_dict)
        if check_task_uniqueness(new_task, task_kwarg_dict):
            new_task.save()
            logger.debug(f'Scheduled datahub task {new_task.name} created')
        else:
            logger.debug(f'Found duplicate task {new_task.name}')
