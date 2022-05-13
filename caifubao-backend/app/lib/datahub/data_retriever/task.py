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

    def get_task_list(self):
        task_list = self.task_obj.objects(status='CRTD')  # Slice here to limit task number
        return task_list

    def dispatch(self):
        logger.info(f'Executing {self.runner_name} datahub tasks')
        self.setup_before_exec()

        # get task list and calculate list length
        self.task_list = self.get_task_list()
        self.task_list_length = len(self.task_list)
        logger.info(f'Found {self.task_list_length} data retrieve task(s), executing')
        self.exec_task_list()

        self.cleanup_after_exec()

    def setup_before_exec(self):
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

    def exec_task_list(self):
        prog_bar = progress_bar()
        counter = {
            "COMP": 0,
            "FAIL": 0
        }
        for i, item in enumerate(self.task_list):
            result = exec_data_retrieve_task(item)
            if result['code'] == 'GOOD':
                counter["COMP"] += 1
            else:
                counter["FAIL"] += 1
            prog_bar(i, self.task_list_length)
        logger.info(f'Processed {self.task_list_length} tasks, '
                    f'{counter["COMP"]} success, {counter["FAIL"]} failed.')

    def exec_task(self):
        pass

    def cleanup_after_exec(self):
        pass


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

    def setup_before_exec(self):
        baostock.interface.establish_baostock_conn()

    def cleanup_after_exec(self):
        baostock.interface.terminate_baostock_conn()


class ScheduledDatahubTask(DatahubTask):
    """
    REPEAT: DAY, T-DAY(EACH TRADING DAY), WEEK, BI-WEEK, MONTH, YEAR
    """
    # TODO: INITIALIZE SCHEDULED DATAHUB TASK
    # TODO: EXEC SCHEDULED DATAHUB TASK

    def __init__(self):
        super().__init__(runner_name='Scheduled', task_obj=ScheduledDatahubTaskDoc)

    def dispatch_scheduled_tasks(self):
        pass

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

    def exec_scheduled_task():
        pass
