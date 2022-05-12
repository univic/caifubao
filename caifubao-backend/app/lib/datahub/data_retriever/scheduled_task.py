import logging
from app.model.data_retrive import KwArg, ScheduledDatahubTask
from app.lib.datahub.data_retriever.common import convert_dict_to_kwarg, check_task_uniqueness, convert_kwarg_to_dict

logger = logging.getLogger()


# TODO: INITIALIZE SCHEDULED DATAHUB TASK
# TODO: EXEC SCHEDULED DATAHUB TASK

def init_scheduled_task():
    task_list = ScheduledDatahubTask()
    if not task_list:
        pass


def dispatch_scheduled_tasks():
    pass


def create_scheduled_datahub_task(name, module, handler, scheduled_time, repeat=None,
                                  args=None, kwarg_dict=None):
    new_task = ScheduledDatahubTask()
    new_task.scheduled_process_time = scheduled_time
    new_task.name = name
    new_task.callback_module = module
    new_task.callback_handler = handler
    new_task.repeat = repeat
    new_task.args = args
    new_task.kwargs = convert_dict_to_kwarg(kwarg_dict)
    if check_task_uniqueness(new_task, kwarg_dict):
        new_task.save()
        logger.debug(f'Scheduled datahub task {new_task.name} created')
    else:
        logger.debug(f'Found duplicate task {new_task.name}')


def exec_scheduled_task():
    pass