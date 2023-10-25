import logging
import datetime
import hashlib
from importlib import import_module
from app.utilities import trading_day_helper
from app.model.task import Task


logger = logging.getLogger(__name__)


def exec_task(task):
    obj = getattr(import_module(f'app.lib.{task.callback_package}.{task.callback_module}'),
                  task.callback_object)
    kwarg_dict = convert_kwarg_to_dict(task.kwargs)
    func = getattr(obj, task.callback_handler)
    task.processed_at = datetime.datetime.now()
    result = func(*task.args, **kwarg_dict)

    if result['code'] == 'GOOD':
        task.completed_at = datetime.datetime.now()
        task.status = 'COMP'
    elif result['code'] == 'WARN':
        task.completed_at = datetime.datetime.now()
        task.status = 'COMP'
        task.message = result['message']
    elif result['code'] == 'ERR':
        task.status = 'ERR'
        task.message = result['message']
    else:
        task.status = 'FAIL'
        task.message = result['message']
    task.save()
    return result


def handle_repeat_task(task):
    if task.repeat:
        if task.repeat == 'T-DAY':
            trade_calendar = trading_day_helper.get_a_stock_market_trade_calendar()
            curr_run_time = task.scheduled_process_time
            next_run_time = trading_day_helper.next_trading_day(trade_calendar)
            next_run_time += datetime.timedelta(hours=curr_run_time.hour,
                                                minutes=curr_run_time.minute,
                                                seconds=curr_run_time.second)
        else:
            next_run_time = None
        # create task
        kw_dict = convert_kwarg_to_dict(task.kwargs)
        self.create_task(name=trading_day_helper.update_title_date_str(task.name, next_run_time),
                         package=task.callback_package,
                         module=task.callback_module,
                         obj=task.callback_object,
                         handler=task.callback_handler,
                         interface=task.callback_interface,
                         repeat=task.repeat,
                         args=task.args,
                         task_kwarg_dict=kw_dict,
                         scheduled_time=next_run_time)


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
    # convert kwarg_dict to str
    if task_obj.kwargs:
        for item in task_obj.kwargs.items():
            kwargs_str += str(item[0]) + str(item[1])
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
