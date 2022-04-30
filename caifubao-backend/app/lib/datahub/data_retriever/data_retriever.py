import datetime
import logging
from importlib import import_module
from app.model.data_retrive import DataRetrieveTask, KwArg, ScheduledDataRetrieveTask
from app.utilities.progress_bar import progress_bar

logger = logging.getLogger()


class DataRetriever(object):

    def __init__(self):
        # self.dispatcher()
        pass

    def dispatch(self):
        logger.info(f'Data retriever dispatcher running...')
        task_list = DataRetrieveTask.objects(status='CRTD')[:10]        # use slice at here to limit the task number
        task_list_length = len(task_list)
        logger.info(f'Found {len(task_list)} data retrieve task(s), executing')
        prog_bar = progress_bar()
        task_complete_counter = 0
        task_fail_counter = 0
        for i, item in enumerate(task_list):
            result = self.exec_data_retrieve_task(item)
            if result['code'] == 'GOOD':
                task_complete_counter += 1
            else:
                task_fail_counter += 1
            prog_bar(i, task_list_length)

        logger.info(f'Data retrieve tasks completed, {task_complete_counter} success, {task_fail_counter} failed')

    def create_data_retrieve_task(self, name, module, handler, scheduled_time=None, args=None, kwarg_dict=None):
        if scheduled_time:
            new_task = ScheduledDataRetrieveTask()
            new_task.scheduled_process_time = scheduled_time
        else:
            new_task = DataRetrieveTask()
        new_task.name = name
        new_task.callback_module = module
        new_task.callback_handler = handler
        new_task.args = args
        new_task.kwargs = self.convert_dict_to_kwarg(kwarg_dict)
        if self.check_task_uniqueness(new_task, kwarg_dict):
            new_task.save()
            logger.debug(f'Data retrieve task {new_task.name} created')
        else:
            logger.debug(f'Found duplicate data retrieve task {new_task.name}')

    def exec_data_retrieve_task(self, item):
        func = getattr(import_module(f'app.lib.datahub.remote_data.{item.callback_module}.handler'),
                       item.callback_handler)
        kwarg_dict = self.convert_kwarg_to_dict(item.kwargs)
        result = func(*item.args, **kwarg_dict)
        item.processed_at = datetime.datetime.now()
        if result['code'] == 'GOOD':
            item.completed_at = datetime.datetime.now()
            item.status = 'COMP'
        else:
            item.status = 'FAIL'
            item.message = result.message
        item.save()
        return result

    @staticmethod
    def convert_dict_to_kwarg(kwarg_dict):
        kwarg_list = []
        for item in kwarg_dict.items():
            kwarg_obj = KwArg()
            kwarg_obj.keyword = item[0]
            kwarg_obj.arg = item[1]
            kwarg_list.append(kwarg_obj)
        return kwarg_list

    @staticmethod
    def convert_kwarg_to_dict(kwarg_doc_list):
        kwarg_dict = {}
        for item in kwarg_doc_list:
            kwarg_dict[item.keyword] = item.arg
        return kwarg_dict

    def check_task_uniqueness(self, task_obj, kwarg_dict):
        task_obj.uid = self.generate_task_uid(task_obj, kwarg_dict)
        current_task = DataRetrieveTask.objects(uid=task_obj.uid).first()
        if current_task:
            return False
        else:
            return True

    @staticmethod
    def generate_task_uid(task_obj, kwarg_dict):
        """
        generate hash uid according to the attributes of the object
        :param task_obj:
        :param kwarg_dict:
        :return:
        """
        obj_str = str(task_obj.name + task_obj.callback_module + task_obj.callback_handler)
        datetime_str = ""
        # convert datetime to str
        if task_obj.scheduled_process_time:
            datetime_str = datetime.datetime.strftime(task_obj.scheduled_process_time, "%Y%m%d%H%M%S")
        args_hash_str = str(hash(tuple(task_obj.args)))         # list is unable to hash, convert to tuple
        kwargs_str = ''
        for item in kwarg_dict.items():
            kwargs_str += item[0] + item[1]
        kwargs_hash_str = str(hash(kwargs_str))
        hash_str = obj_str + args_hash_str + kwargs_hash_str + datetime_str
        uid = str(hash(hash_str))
        return uid
