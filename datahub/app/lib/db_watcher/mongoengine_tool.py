import logging
import os
import time
import traceback

# Create a db object for Flask-Security compatibility
# This allows using db.Document instead of Document
import mongoengine

# from flask_mongoengine import MongoEngine
from mongoengine import Document, StringField, connect, disconnect

from app.conf import app_config

db = mongoengine

logger = logging.getLogger(__name__)

# Retry budget for the initial MongoDB connection: a MongoDB that is still
# starting (node restart, pod recreation) gets ~50s to come up before the
# service gives up with the original exit(1) contract. Mirrors the backend
# fix (#149).
MONGODB_CONNECT_RETRIES = 10
MONGODB_CONNECT_RETRY_DELAY_SECONDS = 5


# def db_init(app):
#     db.init_app(app)


class MongoWatcher:
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

        # Retry so a MongoDB that is still starting up (node restart,
        # MongoDB pod recreating — the 2026-08-28/29 dev datahub crash-loop)
        # does not kill the service on the first probe. Keeps the original
        # fail-hard contract: exit(1) after all attempts.
        last_error = None
        for attempt in range(1, MONGODB_CONNECT_RETRIES + 1):
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
                if attempt > 1:
                    logger.info(
                        "MongoDB connection established after %s attempts",
                        attempt,
                    )
                return conn
            except Exception as e:
                last_error = e
                if attempt < MONGODB_CONNECT_RETRIES:
                    logger.warning(
                        "MongoDB connection attempt %s/%s failed: %s; retrying in %ss",
                        attempt,
                        MONGODB_CONNECT_RETRIES,
                        e,
                        MONGODB_CONNECT_RETRY_DELAY_SECONDS,
                    )
                    disconnect(alias=alias)
                    time.sleep(MONGODB_CONNECT_RETRY_DELAY_SECONDS)
        msg_text = (
            f"Failed to establish MongoDB connection after "
            f"{MONGODB_CONNECT_RETRIES} attempts: \r\n"
            f"{traceback.format_exception(last_error)}"
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
