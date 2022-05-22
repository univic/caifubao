import os
import logging
from flask_mongoengine import MongoEngine
from mongoengine import connect, disconnect
from app.conf import app_config


logger = logging.getLogger()

db = MongoEngine()


def db_init(app):
    db.init_app(app)


def connect_to_db(alias='default'):
    logger.info(f'Opening database connection with alias {alias} in process {os.getpid()}')
    connect(db=app_config.MONGODB_SETTINGS["db"],
            host=app_config.MONGODB_SETTINGS["host"],
            port=app_config.MONGODB_SETTINGS["port"],
            alias=alias)


def disconnect_from_db(alias='default'):
    logger.info(f'Disconnecting database connection with alias {alias} in process {os.getpid()}')
    disconnect(alias=alias)
