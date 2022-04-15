#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import akshare as ak
import logging
from app.model.stock import FinanceMarket
from app.lib.datahub import markets

logger = logging.getLogger()


class Datahub(object):

    def __init__(self):
        logger.info("Initializing datahub")
        self.most_recent_trade_date = None
        self.local_data_obj = LocalData()
        self.remote_data_obj = RemoteData()
        self.initialize()

    @staticmethod
    def initialize():
        markets.initialize_markets()

    def get_data(self, query, bypass_local_data):
        pass

    def cache_data(self):
        pass

    def get_overall_market_info(self):
        pass

    def get_a_stock_code_list(self):
        pass

    def get_individual_stock_quote(self):
        pass

    def get_individual_stock_valuation_indicator(self):
        pass

    def latest_trade_date(self):
        self.today = datetime.date.today()


class LocalData(object):

    def get_trade_date_hist(self):

        return None

    def get_overall_market_info(self):
        pass

    def get_a_stock_code_list(self):
        pass

    def get_individual_stock_quote(self):
        pass

    def get_individual_stock_valuation_indicator(self):
        pass


class RemoteData(object):

    def get_trade_date_hist(self):
        tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
        return tool_trade_date_hist_sina_df

    def get_overall_market_info(self):
        pass

    def get_a_stock_code_list(self):
        pass

    def get_individual_stock_quote(self):
        pass

    def get_individual_stock_valuation_indicator(self):
        pass


class DataIntegrityHelper(object):

    def __init__(self):
        pass


