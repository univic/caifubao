import os
import time
import logging
import datetime
from multiprocessing import Process
from app.conf import app_config
from app.model.task import Task
from app.lib.task_controller.common import exec_task, convert_dict_to_kwarg, check_task_uniqueness

# akshare_datahub_task = AkshareDatahubTask()
# baostock_datahub_task = BaostockDatahubTask()
# scheduled_datahub_task = ScheduledDatahubTask()

logger = logging.getLogger()


# def data_retriever_init():
# logger.info(f'Starting data retriever processes, master process id {os.getpid()}')
# p1 = Process(target=akshare_datahub_task.dispatch)
# p1.start()
# p2 = Process(target=baostock_datahub_task.dispatch)
# p2.start()
# p3 = Process(target=scheduled_datahub_task.dispatch)
# p3.start()

# p.apply_async(akshare_data_retriever.dispatch)
# p.apply_async(baostock_data_retriever.dispatch)
# p.apply_async(scheduled_data_retriever.dispatch)
# p.close()
# p.join()


class Queue(object):
    def __init__(self, name: str, attributes: dict = None):
        self.name: str = name
        self.queue: list = []
        self.attributes: dict = attributes
        self.task_exec_interval: float = app_config.TASK_CONTROLLER_SETTINGS["TASK_EXEC_INTERVAL"]
        self.task_scan_interval = app_config.TASK_CONTROLLER_SETTINGS["TASK_SCAN_INTERVAL"]
        logger.info(f'TaskController - Creating task queue: {self.name}, process PID {os.getpid()}')

    def add_task(self, task):
        logger.info(f'TaskController - Adding task {task.name} to queue: {self.name}')
        self.queue.append(task)

    def dispatch(self):
        logger.info(f'TaskController - Queue {self.name} dispatched')
        continue_flag = True
        while continue_flag:
            self.consume_queue()
            time.sleep(self.task_scan_interval)

    def consume_queue(self):
        queue_length = len(self.queue)
        # Execute first task in the queue, until all the tasks had been popped out
        while queue_length > 0:
            logger.info(f'TaskController - Queue {self.name} - Found {queue_length} tasks, running')
            result = exec_task(self.queue[0])
            self.queue.pop(0)
            queue_length = len(self.queue)
            time.sleep(self.task_exec_interval)

    def get_queue_length(self):
        pass


class TaskQueueController(object):
    def __init__(self):
        logger.info(f'TaskController - TaskQueueController is initializing')
        self.task_queues: dict = {}
        self.queue_num: int = app_config.TASK_CONTROLLER_SETTINGS["DEFAULT_TASK_QUEUE_NUM"]
        self.max_queue_num: int = app_config.TASK_CONTROLLER_SETTINGS["MAX_TASK_QUEUE_NUM"]
        self.initialize()

    def initialize(self):
        """
        create a default queue
        :return:
        """
        self.setup_queue("default")

    def setup_queue(self, name, attributes: dict = None):
        """
        setup a new queue
        :return:
        """
        queue = Queue(name, attributes)
        self.task_queues[name] = queue
        return queue

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
                    # if corresponding queue exists, put the task into it
                    if hasattr(queue, find_queue_by) and queue[find_queue_by] == getattr(task, find_queue_by):
                        q = queue
                        queue_founded = True
                    else:
                        # if no queue matches the attribute, create a new queue
                        queue_name = getattr(task, find_queue_by)
                        queue_attr = {
                            find_queue_by: getattr(task, find_queue_by)
                        }
                        q = self.setup_queue(queue_name, queue_attr)
                # if no queue matches the attribute, put the task into default queue
                if not queue_founded:
                    q = self.task_queues["default"]
            # if exec_unit does not have the corresponding attribute, put the task into default queue
            else:
                q = self.task_queues["default"]
            q.add_task(task)


class TaskController(object):

    def __init__(self):
        self.task_list = []
        self.task_list_length: int = 0
        self.task_scan_interval = app_config.TASK_CONTROLLER_SETTINGS["TASK_SCAN_INTERVAL"]
        self.continue_scan = True
        self.task_queue_controller = TaskQueueController()
        logger.info(f'TaskController is initializing')
        logger.info(f'TaskController - process PID {os.getpid()}')

    @staticmethod
    def check_historical_tasks():
        logger.info(f'TaskController - Checking historical tasks')
        # Check obsolete tasks
        task_list = Task.objects(status='CRTD', valid_before__lte=datetime.datetime.now())
        task_list_len = task_list.count()
        logger.info(f'TaskController - Found {task_list_len} obsolete tasks, all of those tasks will be deactivated')
        for task in task_list:
            task.status = 'ABORT'
            task.exec_msg = 'Cancelled by TaskController due to task validation expired'
            task.processed_at = datetime.datetime.now()
            task.save()

    def dispatch(self):
        self.check_historical_tasks()

        # if nothing goes wrong check new tasks regularly
        while self.continue_scan:
            logger.info(f'TaskController - Scanning tasks by interval {self.task_scan_interval} seconds')
            self.get_task_list()
            self.task_list_length = len(self.task_list)
            if self.task_list_length > 0:
                self.task_queue_controller.add_tasks(self.task_list)
                logger.info(f'TaskController - Added {self.task_list_length} task(s) to the queue, '
                            f'next scan in {self.task_scan_interval} minutes')
            else:
                logger.info(f'TaskController - No new task，next scan in {self.task_scan_interval} seconds')
            time_to_wait = self.task_scan_interval
            time.sleep(time_to_wait)

    def get_task_list(self):
        # get task list and calculate list length
        self.task_list = Task.objects(status='CRTD')  # Slice here to limit task number
        return self.task_list

    def create_task(self, name, desc, callback_package, callback_module, callback_object, callback_handler,
                    callback_interface, priority: int, scheduled_process_time=None, valid_before=None,
                    repeat_duration=None, repeat_amount: int = None, repeat_ends_at=None,
                    args_list: list = None, kwarg_dict=None, **extra_kw):
        new_task = Task()
        new_task.name = name
        new_task.desc = desc
        new_task.callback_package = callback_package
        new_task.callback_module = callback_module
        new_task.callback_object = callback_object
        new_task.callback_handler = callback_handler
        new_task.callback_interface = callback_interface
        new_task.priority = priority
        new_task.scheduled_process_time = scheduled_process_time
        new_task.valid_before = valid_before
        new_task.repeat_duration = repeat_duration
        new_task.repeat_amount = repeat_amount
        new_task.repeat_ends_at = repeat_ends_at
        new_task.args = args_list
        if kwarg_dict:
            new_task.kwargs = convert_dict_to_kwarg(kwarg_dict)
        if check_task_uniqueness(new_task, kwarg_dict):
            new_task.save()
            logger.debug(f'{self.runner_name} task worker - Data retrieve task {new_task.name} created')
        else:
            logger.debug(f'{self.runner_name} task worker - Found duplicate data retrieve task {new_task.name}')


task_controller = TaskController()
