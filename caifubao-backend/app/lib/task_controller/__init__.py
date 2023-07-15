import os
import time
import logging
import datetime
from multiprocessing import Process
from app.conf import app_config
from app.model.task import Task
from app.lib.task_controller.common import exec_task

# akshare_datahub_task = AkshareDatahubTask()
# baostock_datahub_task = BaostockDatahubTask()
# scheduled_datahub_task = ScheduledDatahubTask()

logger = logging.getLogger()


def data_retriever_init():
    logger.info(f'Starting data retriever processes, master process id {os.getpid()}')
    p1 = Process(target=akshare_datahub_task.dispatch)
    p1.start()
    p2 = Process(target=baostock_datahub_task.dispatch)
    p2.start()
    p3 = Process(target=scheduled_datahub_task.dispatch)
    p3.start()

    # p.apply_async(akshare_data_retriever.dispatch)
    # p.apply_async(baostock_data_retriever.dispatch)
    # p.apply_async(scheduled_data_retriever.dispatch)
    # p.close()
    # p.join()


class Queue(object):
    def __init__(self, name):
        self.name: str = name
        self.queue: list = []
        logger.info(f'TaskController - Creating task queue: {self.name}, process PID {os.getpid()}')

    def add_task(self):
        pass

    def dispatch(self):
        for task in self.queue:
            result = exec_task(task)

    def get_queue_length(self):
        pass

class TaskQueueController(object):
    def __init__(self):
        logger.info(f'TaskController - TaskQueueController is initializing')
        self.task_queues: dict = {}
        self.queue_num: int = app_config.TASK_CONTROLLER_SETTINGS["DEFAULT_TASK_QUEUE_NUM"]
        self.max_queue_num: int = app_config.TASK_CONTROLLER_SETTINGS["MAX_TASK_QUEUE_NUM"]
        self.task_exec_interval: float = app_config.TASK_CONTROLLER_SETTINGS["TASK_EXEC_INTERVAL"]
        self.initialize()

    def initialize(self):
        """
        create a default queue
        :return:
        """
        self.setup_queue("default")

    def setup_queue(self, name):
        """
        setup a new queue
        :return:
        """
        queue = Queue(name)
        self.task_queues[name] = {
            "queue_object": queue,
            "interface": None
        }

    def consume_queue(self):
        pass

    def add_tasks(self, task_list: list, find_queue_by="interface"):
        self.distribute_tasks_to_queue(task_list, find_queue_by)

    def distribute_tasks_to_queue(self, task_list: list, find_queue_by):
        """
        add exec unit to corresponding queue according to its interface,
        if no interface was designated, put it into default queue
        :param:
        :return:
        """
        for task in task_list:
            q = None
            # if exec_unit have the corresponding attribute, then try to find the queue
            if hasattr(task, find_queue_by):
                queue_founded = False
                for queue in self.task_queues:
                    if queue[find_queue_by] == getattr(task, find_queue_by):
                        q = queue
                        queue_founded = True
                # if no queue matches the attribute, put the task into default queue
                if not queue_founded:
                    q = self.task_queues["default"]["queue_object"]
            # if exec_unit does not have the corresponding attribute, put the task into default queue
            else:
                q = self.task_queues["default"]["queue_object"]
            q.add_task(task)


class TaskController(object):

    def __init__(self, runner_name='General'):
        self.runner_name = runner_name
        self.db_alias = runner_name
        self.task_obj = Task
        self.task_list = []
        self.task_list_length = 0
        self.task_scan_interval = app_config.TASK_CONTROLLER_SETTINGS["TASK_SCAN_INTERVAL"]
        self.continue_scan = True
        self.task_queue_controller = None
        logger.info(f'TaskController is initializing')
        logger.info(f'TaskController - process PID {os.getpid()}')
        self.initialize()

    def initialize(self):
        self.task_queue_controller = TaskQueueController()
        self.check_historical_tasks()

    def check_historical_tasks(self):
        logger.info(f'TaskController - Checking historical tasks')
        # Check obsolete tasks
        task_list = self.task_obj.objects(status='CRTD', valid_before=datetime.datetime.now())
        task_list_len = task_list.count()
        for task in task_list:
            task.status = 'ABORT'
            task.exec_msg = 'Cancelled by TaskController due to task validation expired'
            task.processed_at = datetime.datetime.now()
            task.save()
        if task_list_len == 0:
            logger.info(f'TaskController - Found {task_list_len} obsolete tasks')
        else:
            logger.info(f'TaskController - Found {task_list_len} obsolete tasks, all of them had been deactivated')

    def dispatch(self):

        # if nothing goes wrong check new tasks regularly
        while self.continue_scan:
            logger.info(f'TaskController - Scanning tasks by interval {self.task_scan_interval} minutes')
            self.get_task_list()
            self.task_list_length = len(self.task_list)
            if self.task_list_length > 0:
                logger.info(f'TaskController - Found {self.task_list_length} task(s) and executing, '
                            f'next scan in {self.task_scan_interval} minutes')
                self.exec_task_list()
            else:
                logger.info(f'TaskController - No new task，next scan in {self.task_scan_interval} minutes')
            time_to_wait = self.task_scan_interval * 60
            time.sleep(time_to_wait)

    def get_task_list(self):
        # get task list and calculate list length
        self.task_list = self.task_obj.objects(status='CRTD')  # Slice here to limit task number
        return self.task_list

    def exec_task_list(self):
        self.task_queue_controller.add_tasks(self.task_list)
        prog_bar = progress_bar()
        counter = {
            "COMP": 0,
            "WARN": 0,
            "FAIL": 0,
            "ERR": 0,
        }
        for i, item in enumerate(self.task_list):
            result = self.exec_task(item)
            if result['code'] == 'GOOD':
                counter["COMP"] += 1
            elif result['code'] == 'WARN':
                counter["WARN"] += 1
            elif result['code'] == 'ERR':
                counter["ERR"] += 1
            else:
                counter["FAIL"] += 1
            prog_bar(i, self.task_list_length)
        logger.info(f'{self.runner_name} task worker - Processed {self.task_list_length} tasks, '
                    f'{counter["COMP"]} success, {counter["WARN"]} completed with warning, {counter["FAIL"]} failed, '
                    f'{counter["ERR"]} encountered error. ')



    def create_task(self, name, package, module, obj, handler, interface, task_args_list=None, task_kwarg_dict=None, **extra_kw):
        new_task = self.task_obj()
        new_task.name = name
        new_task.callback_package = package
        new_task.callback_module = module
        new_task.callback_object = obj
        new_task.callback_handler = handler
        new_task.callback_interface = interface
        new_task.args = task_args_list
        if task_kwarg_dict:
            new_task.kwargs = convert_dict_to_kwarg(task_kwarg_dict)
        if check_task_uniqueness(new_task, task_kwarg_dict):
            new_task.save()
            logger.debug(f'{self.runner_name} task worker - Data retrieve task {new_task.name} created')
        else:
            logger.debug(f'{self.runner_name} task worker - Found duplicate data retrieve task {new_task.name}')
