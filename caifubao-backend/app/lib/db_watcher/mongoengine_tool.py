import os
import logging
from flask_mongoengine import MongoEngine
from mongoengine import connect, disconnect
from app.conf import app_config


logger = logging.getLogger(__name__)


# def db_init(app):
#     db.init_app(app)


class DBWatcher(object):

    def __init__(self):
        db = MongoEngine()
        self.db_conn = None

    def initialize(self):
        logger.info(f'DBWatcher - Initializing')

    def get_db_connection(self):
        if self.db_conn:
            pass
        else:
            self.connect_to_db()
        return self.db_conn

    def connect_to_db(self, alias='default'):
        logger.info(f'Opening database connection with alias {alias} in process {os.getpid()}')
        conn = connect(db=app_config.MONGODB_SETTINGS["db"],
                       host=app_config.MONGODB_SETTINGS["host"],
                       port=app_config.MONGODB_SETTINGS["port"],
                       alias=alias)

        self.db_conn = conn

    @staticmethod
    def disconnect_from_db(alias='default'):
        logger.info(f'Disconnecting database connection with alias {alias} in process {os.getpid()}')
        disconnect(alias=alias)


db_watcher = DBWatcher()
