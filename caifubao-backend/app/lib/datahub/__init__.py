#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import datetime
import logging
import akshare as ak
from pymongo.errors import ServerSelectionTimeoutError
from app.model.stock import FinanceMarket
from app.lib.datahub import markets
from app.lib.datahub.data_retriever import data_retriever

logger = logging.getLogger()


class Datahub(object):

    def __init__(self):
        logger.info("Initializing datahub")
        self.initialize()

    @staticmethod
    def initialize():
        try:
            markets.initialize_markets()
            data_retriever.dispatch()
        except ServerSelectionTimeoutError:
            logger.error("Timed out when establishing DB connection")
            exit()

