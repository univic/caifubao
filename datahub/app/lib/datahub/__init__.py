#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from apscheduler.jobstores.base import JobLookupError
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
from app.lib.db_watcher.mongoengine_tool import mongo_watcher
from app.lib.utilities.trading_day_helper import (
    is_trading_day,
    get_a_stock_market_trade_calendar,
)

logger = logging.getLogger(__name__)
LEGACY_SCHEDULED_JOB_IDS = ("datahub_daily_job",)
LEGACY_SCHEDULER_ENV = "DATAHUB_ENABLE_LEGACY_SCHEDULER"


def _run_scheduled_job(job_name, runner_name):
    try:
        # Initialize MongoDB connection before executing the job
        mongo_watcher.get_db_connection()
        trade_calendar = get_a_stock_market_trade_calendar()
        if trade_calendar and is_trading_day(trade_calendar, datetime.now()):
            logger.info(
                "Executing scheduled task on trading day: job=%s datetime=%s",
                job_name,
                datetime.now(),
            )
            datahub_instance = Datahub()
            job_runner = getattr(datahub_instance, runner_name)
            summary = job_runner()
            logger.info(
                "Scheduled job finished: job=%s status=%s failed_phase=%s pulled_total=%s written_total=%s",
                job_name,
                summary["status"],
                summary["failed_phase"] or "-",
                summary["pulled_total"],
                summary["written_total"],
            )
            return summary
        else:
            logger.info(
                "Skipping scheduled task on non-trading day: job=%s datetime=%s",
                job_name,
                datetime.now(),
            )
            return {
                "job_name": job_name,
                "status": "SKIPPED",
                "failed_phase": None,
                "pulled_total": 0,
                "written_total": 0,
            }
    except Exception as e:
        logger.exception("Error in scheduled job %s: %s", job_name, e)
        raise


def scheduled_job():
    scheduled_index_job()
    scheduled_stock_job()


def scheduled_index_job():
    return _run_scheduled_job("datahub_index_daily_job", "start_index_job")


def scheduled_stock_job():
    return _run_scheduled_job("datahub_stock_daily_job", "start_stock_job")


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
    def __init__(self, quote_as_of_date=None):
        self.module_name = "Datahub"
        self.processor_registry = processors.registry
        # super().__init__(module_name, processor_registry)
        self.market_list: list = []
        self.exec_plan_list = []
        self.scheduler = None
        self.quote_as_of_date = quote_as_of_date
        self.quote_run_started_at = datetime.now(ZoneInfo("Asia/Shanghai"))
        self.last_job_summary = None

    def start(self):
        logger.info(f"Starting {self.module_name}")
        self.get_todo_list()
        self.generate_exec_plan()
        self.commit_tasks()
        return {
            "status": "SUCCESS",
            "failed_phase": None,
            "pulled_total": 0,
            "written_total": 0,
        }

    def _build_processor(self, market_name="ChinaAStock_daily"):
        processor_dict = self.processor_registry[market_name]
        processor_obj = processor_dict["processor_object"]
        return processor_obj(
            as_of_date=self.quote_as_of_date,
            run_started_at=self.quote_run_started_at,
        )

    def _run_processor_job(self, runner_name):
        self.last_job_summary = None
        processor = None
        try:
            processor = self._build_processor()
            return getattr(processor, runner_name)()
        finally:
            if processor is not None:
                self.last_job_summary = processor.last_job_summary

    def start_index_job(self):
        logger.info("Starting scheduled index-only Datahub job")
        return self._run_processor_job("run_index_job")

    def start_stock_job(self):
        logger.info("Starting scheduled stock-only Datahub job")
        return self._run_processor_job("run_stock_job")

    def start_stock_quote_job(self):
        logger.info("Starting stock quote-only Datahub job")
        return self._run_processor_job("run_stock_quote_job")

    def _remove_legacy_jobs(self):
        for job_id in LEGACY_SCHEDULED_JOB_IDS:
            try:
                legacy_job = self.scheduler.get_job(job_id)
                if legacy_job:
                    self.scheduler.remove_job(job_id)
                    logger.info("Removed legacy APScheduler job: %s", job_id)
            except JobLookupError:
                logger.info("Legacy APScheduler job already absent: %s", job_id)

    def start_scheduled(self):
        logger.info(f"Starting scheduled {self.module_name}")

        legacy_enabled = os.getenv(LEGACY_SCHEDULER_ENV, "false").strip().lower()
        if legacy_enabled not in {"1", "true", "yes", "on"}:
            logger.info(
                "Legacy APScheduler disabled by %s; quote CronJobs take over scheduling.",
                LEGACY_SCHEDULER_ENV,
            )
            self.scheduler = None
            return {
                "status": "DISABLED",
                "failed_phase": None,
                "pulled_total": 0,
                "written_total": 0,
            }

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
            "app.lib.datahub:scheduled_index_job",
            trigger=CronTrigger(hour=10, minute=0),  # 10:00 UTC = 18:00 北京时间
            id="datahub_index_daily_job",
            name="Daily datahub index job at 18:00 北京时间",
            replace_existing=True,
        )
        self.scheduler.add_job(
            "app.lib.datahub:scheduled_stock_job",
            trigger=CronTrigger(hour=10, minute=10),  # 10:10 UTC = 18:10 北京时间
            id="datahub_stock_daily_job",
            name="Daily datahub stock job at 18:10 北京时间",
            replace_existing=True,
        )

        self.scheduler.start(paused=True)
        self._remove_legacy_jobs()
        self.scheduler.resume()
        logger.info(
            "Scheduled jobs added: index at 18:00 and stock at 18:10 every trading day (persisted in MongoDB)"
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
