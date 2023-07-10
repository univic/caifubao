# -*- coding: utf-8 -*-
# Author : univic
# Date: 2023-07-01


import logging
from app.lib.datahub import Datahub
from app.lib import web_server
from app.lib.db_tool import mongoengine_tool


logger = logging.getLogger()


class MainDispatcher(object):

    def __init__(self):
        pass

    @staticmethod
    def dispatch():

        # Main Sequence Start

        # Establish DB Connection

        # Start Web Server
        # web_server.create_web_app()

        # Load Scenario and Strategy

        # Start Task Controller

        # Start Datahub
        datahub = Datahub()
        datahub.initialize()

        # Start Factor Factory

        # Start Signal Man

        # Start Opportunity Seeker

        # Trade Planner

        # Trader




