# -*- coding: utf-8 -*-
# Author : univic
# Date: 2022-02-13


import logging
from app.lib.datahub import Datahub
from app.lib.task_controller import task_controller
from app.lib.db_watcher.mongoengine_tool import db_watcher
# from app.lib.dispatcher import MainDispatcher
from app.lib.strategy import strategy_director
from app.lib.back_tester import BasicBackTester


logger = logging.getLogger(__name__)


def create_app():
    logger.info('Stellaris initializing')

    # Establish DB Connection
    db_watcher.initialize()
    db_watcher.connect_to_db()

    # # Start Datahub
    # datahub = Datahub()
    # datahub.start()

    # Start Task Controller
    # task_controller.initialize()

    # start a backtest
    backtester = BasicBackTester(portfolio_name="test_portfolio", strategy_name="Strategy01", start_date="2020-01-01")
    backtester.run()

    # Start web server


    # Load Scenario and Strategy
    # strategy_director.load_strategy("Strategy01")

    # MainDispatcher.dispatch()
