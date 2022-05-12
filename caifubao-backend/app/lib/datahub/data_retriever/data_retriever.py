import datetime
import logging
from importlib import import_module
from app.model.data_retrive import DataRetrieveTask
from app.utilities.progress_bar import progress_bar
from app.lib.datahub.remote_data import baostock
from app.lib.datahub.data_retriever.common import convert_dict_to_kwarg, check_task_uniqueness, convert_kwarg_to_dict

logger = logging.getLogger()


class DataRetriever(object):

    def __init__(self):
        # self.dispatcher()
        pass

    def dispatch(self):
        logger.info(f'Data retriever dispatcher running')
        task_list = DataRetrieveTask.objects(status='CRTD')        # Slice here to limit task number
        task_list_length = len(task_list)
        logger.info(f'Found {task_list_length} data retrieve task(s), executing')

        akshare_task_list = task_list(callback_module='akshare')
        self.exec_task_list('Akshare', akshare_task_list)
        baostock_task_list = task_list(callback_module='baostock')[:10]
        self.exec_baostock_datahub_task_list(baostock_task_list)

    @staticmethod
    def create_data_retrieve_task(name, module, handler, args=None, kwarg_dict=None):
        new_task = DataRetrieveTask()
        new_task.name = name
        new_task.callback_module = module
        new_task.callback_handler = handler
        new_task.args = args
        new_task.kwargs = convert_dict_to_kwarg(kwarg_dict)
        if check_task_uniqueness(new_task, kwarg_dict):
            new_task.save()
            logger.debug(f'Data retrieve task {new_task.name} created')
        else:
            logger.debug(f'Found duplicate data retrieve task {new_task.name}')

    def exec_baostock_datahub_task_list(self, task_list):
        baostock.interface.establish_baostock_conn()
        self.exec_task_list('Baostock', task_list)
        baostock.interface.terminate_baostock_conn()

    def exec_task_list(self, name, task_list):
        task_list_length = task_list.count()
        prog_bar = progress_bar()
        counter = {
            "COMP": 0,
            "FAIL": 0
        }
        for i, item in enumerate(task_list):
            result = self.exec_data_retrieve_task(item)
            if result['code'] == 'GOOD':
                counter["COMP"] += 1
            else:
                counter["FAIL"] += 1
            prog_bar(i, task_list_length)
        logger.info(f'{name} datahub tasks completed, {task_list_length} in total, '
                    f'{counter["COMP"]} success, {counter["FAIL"]} failed.')

    @staticmethod
    def exec_data_retrieve_task(item):
        func = getattr(import_module(f'app.lib.datahub.remote_data.{item.callback_module}.handler'),
                       item.callback_handler)
        kwarg_dict = convert_kwarg_to_dict(item.kwargs)
        item.processed_at = datetime.datetime.now()
        result = func(*item.args, **kwarg_dict)

        if result['code'] == 'GOOD':
            item.completed_at = datetime.datetime.now()
            item.status = 'COMP'
        else:
            item.status = 'FAIL'
            item.message = result['message']
        item.save()
        return result

