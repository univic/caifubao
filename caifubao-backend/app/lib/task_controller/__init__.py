import os
import time, threading
import logging
import datetime
import traceback
from multiprocessing import Pool
from importlib import import_module
from app.conf import app_config
from app.model.task import Task
from app.utilities import trading_day_helper
from app.lib.db_watcher.mongoengine_tool import db_watcher
from app.lib.task_controller.common import convert_kwarg_to_dict, convert_dict_to_kwarg, check_task_uniqueness

# akshare_datahub_task = AkshareDatahubTask()
# baostock_datahub_task = BaostockDatahubTask()
# scheduled_datahub_task = ScheduledDatahubTask()

logger = logging.getLogger(__name__)


class Queue(object):
    def __init__(self, name: str, attributes: dict = None):
        self.name: str = name
        self.queue: list = []
        self.attributes: dict = attributes
        self.task_exec_interval: float = app_config.TASK_CONTROLLER_SETTINGS["TASK_EXEC_INTERVAL"]
        self.task_scan_interval = app_config.TASK_CONTROLLER_SETTINGS["TASK_SCAN_INTERVAL"]
        logger.info(f'TaskController - Creating task queue: {self.name}, process PID {os.getpid()}')

    def add_task(self, task):
        self.queue.append(task)
        logger.info(f'TaskController - Added task {task.name} to queue: {self.name}')

    def dispatch(self):
        try:
            db_watcher.connect_to_db()
            logger.info(f'TaskController - Queue {self.name} dispatched, process PID {os.getpid()}')
            continue_flag = True
            while continue_flag:
                self.consume_queue()
                logger.info(
                    f'TaskController - Task queue {self.name} cleared, wait {self.task_scan_interval} '
                    f'seconds for next scan')
                time.sleep(self.task_scan_interval)
        except Exception as e:
            # print(traceback.format_exception(e))
            logger.error(traceback.print_exception(e))
            # Rethrow the exception to master process
            raise e

    def consume_queue(self):
        self.queue = list(Task.objects(status='CRTD', queue=self.name).order_by('-scheduled_time'))
        queue_length = len(self.queue)
        logger.info(f'TaskController - Task queue {self.name} length is {queue_length}')
        # Execute first task in the queue, until all the tasks had been popped out
        while queue_length > 0:
            logger.info(f'TaskController - Queue {self.name} - Found {queue_length} tasks, running')
            result = self.exec_task(self.queue[0])
            self.queue.pop(0)
            queue_length = len(self.queue)
            time.sleep(self.task_exec_interval)

    def exec_task(self, task):
        obj = getattr(import_module(f'app.lib.{task.callback_package}.{task.callback_module}'),
                      task.callback_object)
        instance = obj()
        kwarg_dict = convert_kwarg_to_dict(task.kwargs)
        func = getattr(instance, task.callback_handler)
        task.processed_at = datetime.datetime.now()
        result = func(*task.args, **kwarg_dict)
        if result and result['code']:
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
            self.handle_repeat_task(task)
            task.save()
            logger.info(f"TaskController - Queue {self.name} - Task '{task.name}>' completed successfully")
        else:
            logger.error(f"TaskController - Queue {self.name} - Task '{task.name}>' did not return valid result info\n"
                         f"callback_module: {task.callback_module}\n"
                         f"callback_package: {task.callback_package}\n"
                         f"callback_object: {task.callback_object}\n"
                         f"callback_handler: {task.callback_handler}\n")
        return result

    def handle_repeat_task(self, task):
        if task.repeat_duration:
            if task.repeat_duration == 'T-DAY':
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
            task_controller.create_task(name=trading_day_helper.update_title_date_str(task.name, next_run_time),
                                        callback_package=task.callback_package,
                                        callback_module=task.callback_module,
                                        callback_object=task.callback_object,
                                        callback_handler=task.callback_handler,
                                        callback_interface=task.callback_interface,
                                        repeat_duration=task.repeat_duration,
                                        args_list=task.args,
                                        kwargs=kw_dict,
                                        scheduled_process_time=next_run_time)


class TaskQueueController(object):
    """
    TaskQueueController is in charge of queue management, such as initial queue setup, task distribution
    """
    def __init__(self):

        self.task_queues: dict = {}
        self.queue_num: int = app_config.TASK_CONTROLLER_SETTINGS["DEFAULT_TASK_QUEUE_NUM"]
        self.max_queue_num: int = app_config.TASK_CONTROLLER_SETTINGS["MAX_TASK_QUEUE_NUM"]
        self.process_pool = None
        self.thread_pool = {}

    def initialize(self):
        """
        create a default queue
        :return:
        """
        logger.info(f'TaskController - TaskQueueController is initializing')
        self.process_pool = Pool(4)
        logger.info(f'TaskController - Process pool ready, parent process PID {os.getpid()}')
        self.setup_queue("default")

    def setup_queue(self, name, attributes: dict = None):
        """
        setup a new queue
        :return:
        """
        queue = Queue(name, attributes)
        self.task_queues[name] = queue
        # p = Process(target=queue.dispatch)
        # p.start()
        if queue.attributes["module"] == "datahub":
            logger.info(f'TaskQueueController - Setting up queue in new thread')
            t = threading.Thread(target=queue.dispatch, name=queue.name)
        else:
            logger.info(f'TaskQueueController - Setting up queue in new process')
            self.process_pool.apply_async(queue.dispatch, error_callback=self.err_callback)

        return queue

    @staticmethod
    def err_callback(err):
        logger.error(f"TaskQueueController - Error occurred in queue: {err}")
        # traceback.print_exc(err)

    def consume_queue(self):
        pass

    def add_tasks_to_queue(self, task_list: list, find_queue_by="callback_interface"):
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
            if hasattr(task, find_queue_by) and getattr(task, find_queue_by) in self.task_queues.keys():
                q = self.task_queues[getattr(task, find_queue_by)]
                # queue_founded = False
            # if no queue matches the attribute, try create a new queue
            elif hasattr(task, find_queue_by) and getattr(task, find_queue_by):
                queue_name = getattr(task, find_queue_by)
                queue_attr = {
                    find_queue_by: getattr(task, find_queue_by),
                    "module": task.callback_module
                }
                q = self.setup_queue(queue_name, queue_attr)
            # if task does not come with attr to determine its queue, put the task into default queue
            else:
                if "default" in self.task_queues.keys():
                    q = self.task_queues["default"]
                else:
                    logger.error("TaskQueueController - Default queue not found")
            task.queue = q.name
            task.save()
            q.add_task(task)
            logger.info(f"Added task {task.name} to queue: {q.name}")


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
                self.task_queue_controller.add_tasks_to_queue(self.task_list)
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

    def initialize(self):
        self.task_queue_controller.initialize()

    def create_task(self, name, callback_package, callback_module, callback_object, callback_handler, desc=None,
                    callback_interface=None, priority: int = 5, scheduled_process_time=None, valid_before=None,
                    repeat_duration=None, repeat_amount: int = None, repeat_ends_at=None,
                    args_list: list = None, kwargs: dict = None, **extra_kw):
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
        if kwargs:
            new_task.kwargs = convert_dict_to_kwarg(kwargs)
        if check_task_uniqueness(new_task):
            new_task.save()
            logger.debug(f'TaskController - task {new_task.name} created')
            # Add new task to queue
            self.task_queue_controller.add_tasks_to_queue([new_task])
        else:
            logger.debug(f'TaskController - Found duplicate task {new_task.name}')


task_controller = TaskController()
