# -*- coding: utf-8 -*-
# Author : univic
# Date: 2022-02-13


import logging
from flask import Flask
from flask_jwt_extended import JWTManager
from app.conf import app_config
from app.api import register_blueprint
from app.api.v1.auth import init_auth

logger = logging.getLogger(__name__)

jwt = JWTManager()


def create_web_app():
    logger.info("initializing web app")
    app = Flask(__name__)
    app.debug = app_config.FLASK_DEBUG
    app.config.from_object(app_config)

    # Initialize JWT
    jwt.init_app(app)

    # Register blueprints
    register_blueprint(app)

    # Initialize auth
    init_auth(app)

    return app
