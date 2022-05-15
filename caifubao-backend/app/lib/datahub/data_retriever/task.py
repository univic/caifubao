import time
import datetime
import logging
from app.model.data_retrive import DatahubTaskDoc, ScheduledDatahubTaskDoc
from app.utilities.progress_bar import progress_bar
from app.lib.datahub.remote_data import baostock
from app.lib.datahub.data_retriever.common import convert_dict_to_kwarg, check_task_uniqueness, exec_data_retrieve_task

logger = logging.getLogger()


class DatahubTask(object):

    def __init__(self, runner_name='General', task_obj=DatahubTaskDoc):
        self.runner_name = runner_name
        self.task_obj = task_obj
        self.task_list = []
        self.task_list_length = 0

    def dispatch(self):
        logger.info(f'Executing {self.runner_name} datahub tasks')
        self.before_task_list_exec()

        # get task list and calculate list length
        self.task_list = self.get_task_list()
        logger.info(f'Found {self.task_list_length} data retrieve task(s), executing')
        self.exec_task_list()

        self.after_task_list_exec()

    def get_task_list(self):
        self.task_list = self.task_obj.objects(status='CRTD')  # Slice here to limit task number
        self.task_list_length = len(self.task_list)
        return self.task_list

    def exec_task_list(self):
        prog_bar = progress_bar()
        counter = {
            "COMP": 0,
            "FAIL": 0
        }
        for i, item in enumerate(self.task_list):
            result = self.exec_task()
            if result['code'] == 'GOOD':
                counter["COMP"] += 1
            else:
                counter["FAIL"] += 1
            prog_bar(i, self.task_list_length)
        logger.info(f'Processed {self.task_list_length} tasks, '
                    f'{counter["COMP"]} success, {counter["FAIL"]} failed.')

    def exec_task(self, item):
        self.before_task_exec()
        result = exec_data_retrieve_task(item)
        self.after_task_exec()
        return result

    def before_task_list_exec(self):
        pass

    def after_task_list_exec(self):
        pass

    def before_task_exec(self, item):
        pass

    def after_task_exec(self, item):
        pass

    def create_task(self, name, module, handler, task_args_list=None, task_kwarg_dict=None, **extra_kw):
        new_task = self.task_obj()
        new_task.name = name
        new_task.callback_module = module
        new_task.callback_handler = handler
        new_task.args = task_args_list
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
        task_list = self.task_obj.objects(status='CRTD', callback_module='akshare')  # Slice here to limit task number
        return task_list


class BaostockDatahubTask(DatahubTask):
    def __init__(self):
        super().__init__(runner_name='Baostock', task_obj=DatahubTaskDoc)

    def get_task_list(self):
        task_list = self.task_obj.objects(status='CRTD', callback_module='baostock')  # Slice here to limit task number
        return task_list

    def before_task_list_exec(self):
        baostock.interface.establish_baostock_conn()

    def after_task_list_exec(self):
        baostock.interface.terminate_baostock_conn()


class ScheduledDatahubTask(DatahubTask):
    """
    REPEAT: DAY, T-DAY(EACH TRADING DAY), WEEK, BI-WEEK, MONTH, YEAR
    """
    # TODO: INITIALIZE SCHEDULED DATAHUB TASK
    # TODO: EXEC SCHEDULED DATAHUB TASK

    def __init__(self):
        super().__init__(runner_name='Scheduled', task_obj=ScheduledDatahubTaskDoc)

    def initialize_scheduled_task(self):
        pass
        # TODO: first 2 task

    def get_task_list(self):
        task_list = self.task_obj.objects(status='CRTD').order_by('-scheduled_time')  # Slice here to limit task number
        return task_list

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
        pass
        # TODO handle repeat

    def create_task(self, name, module, handler, task_args_list=None, task_kwarg_dict=None, **extra_kw):
        new_task = self.task_obj()
        new_task.scheduled_process_time = extra_kw["scheduled_time"]
        new_task.name = name
        new_task.callback_module = module
        new_task.callback_handler = handler
        new_task.repeat = extra_kw["repeat"]
        new_task.args = task_args_list
        new_task.kwargs = convert_dict_to_kwarg(task_kwarg_dict)
        if check_task_uniqueness(new_task, task_kwarg_dict):
            new_task.save()
            logger.debug(f'Scheduled datahub task {new_task.name} created')
        else:
            logger.debug(f'Found duplicate task {new_task.name}')