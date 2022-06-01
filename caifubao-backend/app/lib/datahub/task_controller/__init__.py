import os
import logging
from multiprocessing import Process, Pool
from app.lib.datahub.task_controller.task import AkshareDatahubTask, BaostockDatahubTask, ScheduledDatahubTask

akshare_datahub_task = AkshareDatahubTask()
baostock_datahub_task = BaostockDatahubTask()
scheduled_datahub_task = ScheduledDatahubTask()

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
