import datetime
import logging
from importlib import import_module
from app.model.data_retrive import DataRetriveTask, KwArg

logger = logging.getLogger()


class DataRetriever(object):

    def __init__(self):
        pass

    def dispatcher(self):
        task_list = DataRetriveTask.objects(status='CRTD')
        for item in task_list:
            self.exec_data_retrieve_task(item)

    def create_data_retrieve_task(self, name, module, handler, args=None, kwarg_dict=None):
        new_task = DataRetriveTask()
        new_task.name = name
        new_task.callback_module = module
        new_task.callback_handler = handler
        new_task.args = args
        new_task.kwargs = self.convert_kwarg_dict(kwarg_dict)
        if self.check_task_uniqueness(new_task):
            new_task.save()
            logger.debug(f'Data retrieve task {new_task.name} created')
        else:
            logger.debug(f'Found duplicate data retrieve task {new_task.name}')

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

    @staticmethod
    def convert_kwarg_dict(kwarg_dict):
        kwarg_list = []
        for item in kwarg_dict.items():
            kwarg_obj = KwArg()
            kwarg_obj.keyword = item[0]
            kwarg_obj.arg = item[1]
            kwarg_list.append(kwarg_obj)
        return kwarg_list

    def check_task_uniqueness(self, task_obj):
        task_obj.uid = self.generate_task_uid(task_obj)
        current_task = DataRetriveTask.objects(uid=task_obj.uid).first()
        if current_task:
            return False
        else:
            return True

    @staticmethod
    def generate_task_uid(task_obj):
        obj_str = task_obj.name + task_obj.callback_module + task_obj.callback_handler
        args_hash_str = hash(task_obj.args)
        kwargs_hash_str = hash(task_obj.kwargs)
        hash_str = obj_str + args_hash_str + kwargs_hash_str
        uid = hash(hash_str)
        return uid
