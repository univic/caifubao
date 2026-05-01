import os
import logging
import traceback

# from flask_mongoengine import MongoEngine
from mongoengine import connect, disconnect, Document, StringField
from app.conf import app_config

# Create a db object for Flask-Security compatibility
# This allows using db.Document instead of Document
import mongoengine

db = mongoengine


logger = logging.getLogger(__name__)


# def db_init(app):
#     db.init_app(app)


class MongoWatcher(object):
    def __init__(self):
        self.db_conn = None

    def initialize(self):
        logger.info("DBWatcher - Initializing")
        # db preset

    def get_db_connection(self):
        if self.db_conn:
            pass
        else:
            self.connect_to_db()
        return self.db_conn

    def connect_to_db(self, alias="default"):
        logger.info(
            f"Opening database connection with alias {alias} in process {os.getpid()}"
        )

        if not app_config.MONGODB_USERNAME and app_config.MONGODB_PASSWORD:
            logger.error("MongoDB AUTH CONFIG NOT FOUND")
        try:
            conn = connect(
                db=app_config.MONGODB_DB,
                host=app_config.MONGODB_HOST,
                port=app_config.MONGODB_PORT,
                username=app_config.MONGODB_USERNAME,
                password=app_config.MONGODB_PASSWORD,
                authentication_source="admin",
                alias=alias,
            )
            # try to establish connection
            ConnTestDocument.objects.first()
            self.db_conn = conn
        except Exception as e:
            msg_text = (
                f"Failed to establish MongoDB connection: \r\n"
                f"{traceback.format_exception(e)}"
            )
            # daily_report_maker.add_content('summary', msg_text)
            logger.critical(msg_text)
            exit(code=1)

    @staticmethod
    def disconnect_from_db(alias="default"):
        logger.info(
            f"Disconnecting database connection with alias {alias} in process {os.getpid()}"
        )
        disconnect(alias=alias)


class ConnTestDocument(Document):
    name = StringField()


mongo_watcher = MongoWatcher()
