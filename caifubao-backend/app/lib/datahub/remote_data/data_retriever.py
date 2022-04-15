import datetime
import logging
from app.model.data_retrive import DataRetriveTask, KwArgs

logger = logging.getLogger()


class DataRetriever(object):

    def __init__(self):
        pass

    def dispatcher(self):
        pass

    @staticmethod
    def create_data_retrieve_task(module, handler, args, kwargs):
        new_task = DataRetriveTask()
        new_task.callback_module = module
        new_task.callback_handler = handler
        new_task.args = args
        new_task.save()

