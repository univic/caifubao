# -*- coding: utf-8 -*-
# Author : univic
# Date: 2022-02-13


from flask import Flask
from app.utilities.logger import create_logger
from app.conf import app_config
from app.api import register_blueprint
from app.lib.database import db_init
from app.lib.datahub import Datahub
from app.extensions import config_extensions

logger = create_logger()


def create_app():
    logger.info('Stellaris initializing')
    app = Flask(__name__)
    app.debug = True
    app.config.from_object(app_config)
    register_blueprint(app)
    config_extensions(app)
    db_init(app)
    obj = Datahub()
