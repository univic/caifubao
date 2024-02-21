# -*- coding: utf-8 -*-
# Author : univic
# Date: 2023-07-01


import logging
from app.lib.strategy import strategy_director
from app.lib.datahub import Datahub
# from app.lib import web_server
from app.lib.db_watcher.mongoengine_tool import db_watcher
from app.lib.task_controller import task_controller


logger = logging.getLogger(__name__)


class MainSession(object):

    def __init__(self, scenario):
        pass

    @staticmethod
    def dispatch():

        pass

        # Main Sequence Start

        # Establish DB Connection
        # db_watcher.initialize()
        # db_watcher.connect_to_db()

        # Start Factor Factory

        # Start Signal Man

        # Start Opportunity Seeker

        # Trade Planner

        # Trader




