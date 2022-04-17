import datetime
import logging
from importlib import import_module
from app.model.data_retrive import DataRetriveTask, KwArgs

logger = logging.getLogger()


class DataRetriever(object):

    def __init__(self):
        pass

    def dispatcher(self):
        task_list = DataRetriveTask.objects(status='CRTD')
        for item in task_list:
            self.exec_data_retrieve_task(item)

    @staticmethod
    def create_data_retrieve_task(module, handler, args, kwargs):
        new_task = DataRetriveTask()
        new_task.callback_module = module
        new_task.callback_handler = handler
        new_task.args = args
        new_task.save()

    @staticmethod
    def exec_data_retrieve_task(item):

        func = getattr(import_module(f'{item.callback_package}.{item.callback_module}'), item.callback_handler)
        result = func(*item.args, **item.kwargs)
        item.processed_at = datetime.datetime.now()
        if result.code == 'GOOD':
            item.completed_at = datetime.datetime.now()
            item.status = 'COMP'
        elif result.code == 'FAIL':
            item.status = 'FAIL'
            item.message = result.message
        else:
            item.status = 'PEND'
            item.message = result.message
        item.save()
