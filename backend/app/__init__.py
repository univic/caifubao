# -*- coding: utf-8 -*-
# Author : univic
# Date: 2022-02-13


import logging
from app.lib.db_watcher.mongoengine_tool import mongo_watcher
from app.lib.web_server import create_web_app

logger = logging.getLogger(__name__)


def create_app():
    logger.info("Stellaris initializing")

    # Establish DB Connection
    mongo_watcher.initialize()
    mongo_watcher.get_db_connection()

    # Start web server
    app = create_web_app()

    return app


def check_workdir():
    pass
