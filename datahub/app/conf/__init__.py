# -*- coding: utf-8 -*-

import os
import sys
from dotenv import load_dotenv


def prerequisite_check():
    is_unittest = "unittest" in sys.modules

    if not is_unittest:
        if os.path.exists(".env"):
            load_dotenv()
            print("ENV VAR LOADED from .env file.")
        else:
            print(".env file not found, using environment variables from system.")
    else:
        print(".env file not loaded (unittest mode)")


prerequisite_check()


class BaseConfig(object):
    LOGGING_MAX_LOG_SIZE = 512
    LOGGING_BACKUP_COUNT = 1

    # Use class-level properties to defer validation until access time
    @property
    def MONGODB_DB(self):
        value = os.getenv("MONGODB_NAME")
        if not value:
            raise ValueError("MONGODB_NAME environment variable is required")
        return value

    @property
    def MONGODB_HOST(self):
        value = os.getenv("MONGODB_HOST")
        if not value:
            raise ValueError("MONGODB_HOST environment variable is required")
        return value

    @property
    def MONGODB_PORT(self):
        value = os.getenv("MONGODB_PORT")
        if not value:
            raise ValueError("MONGODB_PORT environment variable is required")
        try:
            return int(value)
        except ValueError:
            raise ValueError("MONGODB_PORT must be a valid integer")

    @property
    def MONGODB_USERNAME(self):
        value = os.getenv("MONGODB_USER")
        if not value:
            raise ValueError("MONGODB_USER environment variable is required")
        return value

    @property
    def MONGODB_PASSWORD(self):
        value = os.getenv("MONGODB_PASS")
        if not value:
            raise ValueError("MONGODB_PASS environment variable is required")
        return value

    # Source MongoDB (used by data sync engine — dev pulls from prod)
    MONGODB_SRC_HOST = os.getenv("MONGODB_SRC_HOST", "")
    MONGODB_SRC_PORT = int(os.getenv("MONGODB_SRC_PORT", "27017"))
    MONGODB_SRC_USERNAME = os.getenv("MONGODB_SRC_USER", "")
    MONGODB_SRC_PASSWORD = os.getenv("MONGODB_SRC_PASS", "")
    MONGODB_SRC_NAME = os.getenv("MONGODB_SRC_NAME", "")

    DATAHUB_TASK_SCAN_INTERVAL = 5
    TASK_CONTROLLER_DEFAULT_TASK_QUEUE_NUM = 3
    TASK_CONTROLLER_MAX_TASK_QUEUE_NUM = 5
    TASK_CONTROLLER_TASK_EXEC_INTERVAL = 0.5
    TASK_CONTROLLER_TASK_SCAN_INTERVAL = 10
    MAIL_SENDER_EMAIL = os.getenv("SMTP_SENDER_EMAIL", "")
    MAIL_RECIPIENT_EMAIL_LIST = []
    MAIL_SMTP_SERVER_ADDR = "smtp.office365.com"
    MAIL_SMTP_PORT = 587
    MAIL_SMTP_USERNAME = os.getenv("SMTP_SENDER_EMAIL", "")
    MAIL_SMTP_PASSWORD = os.getenv("SMTP_SENDER_PASSWORD", "")
    MAIL_SMTP_SENDER_DISPLAY_NAME = "CAIFUBAO"
    BARK_URL = os.getenv("BARK_URL", "")


class DevConfig(BaseConfig):
    pass


class ProductionConfig(BaseConfig):
    pass


def get_config():
    env = os.getenv("APP_ENV", "DEV").upper()
    if env == "PRODUCTION" or env == "PROD":
        return ProductionConfig()
    else:
        return DevConfig()


app_config = get_config()
