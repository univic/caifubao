# -*- coding: utf-8 -*-
# Author : univic
# Date: 2022-02-13


import logging
from app.lib.datahub import Datahub
from app.lib.task_controller import task_controller
from app.lib.db_watcher.mongoengine_tool import db_watcher
# from app.lib.dispatcher import MainDispatcher
from app.lib.strategy import strategy_director


logger = logging.getLogger(__name__)


def create_app():
    logger.info('Stellaris initializing')

    # Establish DB Connection
    db_watcher.initialize()
    db_watcher.connect_to_db()

    # Start Datahub
    datahub = Datahub()
    datahub.start()

    # Start Task Controller
    task_controller.initialize()

    # Start web server


    # Load Scenario and Strategy
    strategy_director.load_strategy("Strategy01")

    # MainDispatcher.dispatch()

    while True:
        pass