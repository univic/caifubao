import logging
import datetime
import hashlib
from app.model.data_retrive import DatahubTaskDoc, ScheduledDatahubTaskDoc, KwArg
from importlib import import_module

logger = logging.getLogger()


def exec_datahub_task(item):
    func = getattr(import_module(f'app.lib.datahub.{item.callback_package}.{item.callback_module}.handler'),
                   item.callback_handler)
    kwarg_dict = convert_kwarg_to_dict(item.kwargs)
    item.processed_at = datetime.datetime.now()
    result = func(*item.args, **kwarg_dict)

    if result['code'] == 'GOOD':
        item.completed_at = datetime.datetime.now()
        item.status = 'COMP'
    elif result['code'] == 'WARN':
        item.completed_at = datetime.datetime.now()
        item.status = 'COMP'
        item.message = result['message']
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
    obj_str = str(task_obj.name + task_obj.callback_package + task_obj.callback_module + task_obj.callback_handler)
    datetime_str = ""
    kwargs_str = ""
    args_str = ""
    # convert datetime to str
    if isinstance(task_obj, ScheduledDatahubTaskDoc):
        datetime_str = datetime.datetime.strftime(task_obj.scheduled_process_time, "%Y%m%d%H%M%S")
    if task_obj.args:
        args_str = "-".join(task_obj.args)
    # convert kwarg_dict to str
    if kwarg_dict:
        for item in kwarg_dict.items():
            kwargs_str += str(item[0]) + str(item[1])
    hash_str = obj_str + args_str + kwargs_str + datetime_str
    uid = hashlib.md5(hash_str.encode(encoding='UTF-8')).hexdigest()
    return uid


def check_task_uniqueness(task_obj, kwarg_dict):
    task_obj.uid = generate_task_uid(task_obj, kwarg_dict)
    task_query = DatahubTaskDoc.objects(uid=task_obj.uid, status='CRTD').first()
    if task_query:
        return False
    else:
        return True
