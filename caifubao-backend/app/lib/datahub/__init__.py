#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging
from pymongo.errors import ServerSelectionTimeoutError
from app.lib.datahub import markets
from app.lib.datahub.task_controller import data_retriever_init
from app.lib.datahub.remote_data.handler import zh_a_data

logger = logging.getLogger()


class Datahub(object):

    def __init__(self):
        logger.info("Initializing datahub")

    def initialize(self):
        try:
            markets.initialize_markets()
            # data_retriever_init()

        except ServerSelectionTimeoutError:
            logger.error("Timed out when establishing DB connection")
            exit()
