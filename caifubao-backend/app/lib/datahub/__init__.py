#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging
from pymongo.errors import ServerSelectionTimeoutError
from app.lib.datahub import markets
from app.lib.datahub.data_retriever import data_retriever_init
from app.lib.datahub.remote_data.akshare import handler

logger = logging.getLogger()


class Datahub(object):

    def __init__(self):
        logger.info("Initializing datahub")

    @staticmethod
    def initialize():
        try:
            markets.initialize_markets()
            # data_retriever_init()

        except ServerSelectionTimeoutError:
            logger.error("Timed out when establishing DB connection")
            exit()

    @staticmethod
    def run_handler():
        handler.update_zh_stock_index_daily_spot()
