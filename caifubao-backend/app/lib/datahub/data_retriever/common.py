import logging
import datetime
from app.model.data_retrive import DataRetrieveTask, ScheduledDatahubTask, KwArg
from importlib import import_module

logger = logging.getLogger()


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


def convert_dict_to_kwarg(kwarg_dict):
    kwarg_list = []
    for item in kwarg_dict.items():
        kwarg_obj = KwArg()
        kwarg_obj.keyword = item[0]
        kwarg_obj.arg = item[1]
        kwarg_list.append(kwarg_obj)
    return kwarg_list


def convert_kwarg_to_dict(kwarg_doc_list):
    kwarg_dict = {}
    for item in kwarg_doc_list:
        kwarg_dict[item.keyword] = item.arg
    return kwarg_dict


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
    if isinstance(task_obj, ScheduledDatahubTask):
        datetime_str = datetime.datetime.strftime(task_obj.scheduled_process_time, "%Y%m%d%H%M%S")
    args_hash_str = str(hash(tuple(task_obj.args)))  # list is unable to hash, convert to tuple
    kwargs_str = ''
    for item in kwarg_dict.items():
        kwargs_str += str(item[0]) + str(item[1])
    kwargs_hash_str = str(hash(kwargs_str))
    hash_str = obj_str + args_hash_str + kwargs_hash_str + datetime_str
    uid = str(hash(hash_str))
    return uid


def check_task_uniqueness(task_obj, kwarg_dict):
    task_obj.uid = generate_task_uid(task_obj, kwarg_dict)
    task_query = DataRetrieveTask.objects(uid=task_obj.uid, status='CRTD').first()
    if task_query:
        return False
    else:
        return True
