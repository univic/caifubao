#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from app.conf import app_config

# from app.lib import GeneralWorker
# # from app.lib.datahub import markets
from app.lib.datahub import processors

# from pymongo.errors import ServerSelectionTimeoutError
# # from app.lib.scenario_director import scenario_director
# from app.lib.strategy import strategy_director
from app.lib.task_controller import task_controller
from app.utilities.trading_day_helper import (
    is_trading_day,
    get_a_stock_market_trade_calendar,
)

logger = logging.getLogger(__name__)


def scheduled_job():
    try:
        trade_calendar = get_a_stock_market_trade_calendar()
        if trade_calendar and is_trading_day(trade_calendar, datetime.now()):
            logger.info(f"Executing scheduled task on trading day: {datetime.now()}")
            datahub_instance = Datahub()
            datahub_instance.start()
        else:
            logger.info(f"Skipping scheduled task on non-trading day: {datetime.now()}")
    except Exception as e:
        logger.error(f"Error in scheduled job: {e}")


# class Datahub(object):
#
#     def __init__(self):
#         logger.info("Initializing datahub")
#
#     def initialize(self):
#         try:
#             markets.initialize_markets()
#             data_retriever_init()
#
#         except ServerSelectionTimeoutError:
#             logger.error("Timed out when establishing DB connection")
#             exit()


class Datahub(object):
    def __init__(self):
        self.module_name = "Datahub"
        self.processor_registry = processors.registry
        # super().__init__(module_name, processor_registry)
        self.market_list: list = []
        self.exec_plan_list = []
        self.scheduler = None

    def start(self):
        logger.info(f"Starting {self.module_name}")
        self.get_todo_list()
        self.generate_exec_plan()
        self.commit_tasks()

    def start_scheduled(self):
        logger.info(f"Starting scheduled {self.module_name}")

        jobstore = MongoDBJobStore(
            host=app_config.MONGODB_HOST,
            port=app_config.MONGODB_PORT,
            database=app_config.MONGODB_DB,
            collection="apscheduler_jobs",
            username=app_config.MONGODB_USERNAME,
            password=app_config.MONGODB_PASSWORD,
            authsource="admin",
        )

        job_defaults = {
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 3600,
        }

        self.scheduler = BackgroundScheduler(
            jobstores={"default": jobstore},
            executors={"default": ThreadPoolExecutor(20)},
            job_defaults=job_defaults,
        )

        self.scheduler.add_job(
            "app.lib.datahub:scheduled_job",
            trigger=CronTrigger(hour=18, minute=0),
            id="datahub_daily_job",
            name="Daily datahub job at 18:00",
            replace_existing=True,
        )

        self.scheduler.start()
        logger.info(
            "Scheduled job added: run at 18:00 every trading day (persisted in MongoDB)"
        )

    def stop_scheduled(self):
        if self.scheduler and self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduled job stopped")

    def get_todo_list(self):
        # self.market = strategy_director.get_market_name()
        self.market_list = self.processor_registry.keys()
        logger.info(f"{self.module_name} market list: {self.market_list}")
        if not self.market_list:
            logger.critical(
                f"{self.module_name} - Initialization failed, no market was found"
            )
            exit()

    def generate_exec_plan(self):
        for market_name in self.market_list:
            processor_dict = self.processor_registry[market_name]
            processor_obj = processor_dict["processor_object"]
            instance = processor_obj()
            func = getattr(instance, processor_dict["handler"])
            func()
            # exec_plan_item = {
            #     "name": market_name,
            #     "processor": processor,
            #     "module": processor_dict['module'],
            #     "handler": processor_dict['handler']
            # }
            # self.exec_plan_list.append(exec_plan_item)

    def commit_tasks(self):
        for item in self.exec_plan_list:
            task_controller.create_task(
                name=f"Initialize market {item['name']}",
                callback_package="datahub",
                callback_module=item["module"],
                callback_object="ChinaAStock",
                callback_handler=item["handler"],
            )


if __name__ == "__main__":
    import sys

    instance = Datahub()

    if len(sys.argv) > 1 and sys.argv[1] == "--scheduled":
        instance.start_scheduled()
        try:
            import time

            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            instance.stop_scheduled()
    else:
        instance.start()
