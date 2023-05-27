# -*- coding: utf-8 -*-
# Author : univic
# Date: 2022-02-13


from app.utilities.logger import create_logger
from app.lib.db_tool.mongoengine_tool import db_init
from app.lib.datahub import Datahub


logger = create_logger()


def create_app():
    logger.info('Stellaris initializing')
    datahub = Datahub()
    datahub.initialize()
