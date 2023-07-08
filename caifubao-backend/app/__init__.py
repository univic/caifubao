# -*- coding: utf-8 -*-
# Author : univic
# Date: 2022-02-13


from app.utilities.logger import create_logger
from app.lib.dispatcher import MainDispatcher


logger = create_logger()


def create_app():
    logger.info('Stellaris initializing')
    MainDispatcher.dispatch()

