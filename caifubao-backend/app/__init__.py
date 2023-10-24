# -*- coding: utf-8 -*-
# Author : univic
# Date: 2022-02-13


import logging
from app.lib.dispatcher import MainDispatcher


logger = logging.getLogger(__name__)


def create_app():
    logger.info('Stellaris initializing')
    MainDispatcher.dispatch()

