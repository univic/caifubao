# -*- coding: utf-8 -*-
# Author : univic
# Date: 2022-02-13


from flask import Flask
from app.utilities.logger import create_logger
from app.conf import app_config
from app.api import register_blueprint
from app.extensions import config_extensions

logger = create_logger()


def create_web_app():
    logger.info('initializing web app')
    app = Flask(__name__)
    app.debug = True
    app.config.from_object(app_config)
    register_blueprint(app)
    config_extensions(app)
    