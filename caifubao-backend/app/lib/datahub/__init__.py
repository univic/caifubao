#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging
from app.lib import GeneralWorker
# from app.lib.datahub import markets
from app.lib.datahub.data_source.handler import zh_a_data
from app.lib.datahub import processors
from pymongo.errors import ServerSelectionTimeoutError
from app.lib.scenario_director import scenario_director
from app.lib.strategy import strategy_director
from app.lib.task_controller import task_controller

logger = logging.getLogger()


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


class Datahub(GeneralWorker):
    def __init__(self):
        module_name = 'Datahub'
        processor_registry = processors.registry
        super().__init__(module_name, processor_registry)
        self.market_list = []
        self.exec_plan_list = []

    def get_todo_list(self):
        market_list = strategy_director.get_market_list()

    def generate_exec_plan(self):
        for market_name in self.market_list:
            processor_dict = self.processor_registry[market_name]
            processor = processor_dict['processor_object']
            exec_plan_item = {
                "name": market_name,
                "processor": processor
            }
            self.exec_plan_list.append(exec_plan_item)

    def commit_tasks(self):
        for item in self.exec_plan_list:
            task_controller.create_task(name="",
                                        package="",
                                        module="",
                                        obj="",
                                        handler="",
                                        interface="")


if __name__ == '__init__':
    instance = Datahub()
    # instance.run()
    instance.initialize()
