import logging
import datetime
import hashlib

from app.model.task import Task, KwArg


logger = logging.getLogger(__name__)


def convert_dict_to_kwarg(kwarg_dict):
    kwarg_list = []
    for item in kwarg_dict.items():
        kwarg_obj = KwArg()
        kwarg_obj.keyword = item[0]
        if item[1] is True:
            kwarg_obj.arg = 'True'
        elif item[1] is False:
            kwarg_obj.arg = 'False'
        else:
            kwarg_obj.arg = item[1]
        # if item[1] == 'True':
        #     kwarg_obj.arg = True
        # elif item[1] == 'False':
        #     kwarg_obj.arg = False
        # else:
        #     kwarg_obj.arg = item[1]
        kwarg_list.append(kwarg_obj)
    return kwarg_list


def convert_kwarg_to_dict(kwarg_doc_list):
    kwarg_dict = {}
    for item in kwarg_doc_list:
        if item.arg == 'True':
            kwarg_dict[item.keyword] = True
        elif item.arg == 'False':
            kwarg_dict[item.keyword] = False
        else:
            kwarg_dict[item.keyword] = item.arg
    return kwarg_dict


def generate_task_uid(task_obj):
    """
    generate hash uid according to the attributes of the object
    :param task_obj:
    :return:
    """
    obj_str = str(task_obj.name + task_obj.callback_package + task_obj.callback_module + task_obj.callback_handler)
    datetime_str = ""
    kwargs_str = ""
    args_str = ""
    # convert datetime to str
    if hasattr(task_obj, 'scheduled_process_time') and task_obj.scheduled_process_time:
        datetime_str = datetime.datetime.strftime(task_obj.scheduled_process_time, "%Y%m%d%H%M%S")
    if task_obj.args:
        args_str = "-".join(task_obj.args)
    # convert kwargs to str
    if task_obj.kwargs:
        for item in task_obj.kwargs:
            kwargs_str += str(item.keyword) + str(item.arg)
    hash_str = obj_str + args_str + kwargs_str + datetime_str
    uid = hashlib.md5(hash_str.encode(encoding='UTF-8')).hexdigest()
    return uid


def check_task_uniqueness(task_obj):
    task_obj.uid = generate_task_uid(task_obj)
    task_query = Task.objects(uid=task_obj.uid, status='CRTD').first()
    if task_query:
        return False
    else:
        return True
