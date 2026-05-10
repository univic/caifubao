# -*- coding: utf-8 -*-
# Author : univic
# Date: 2021-02-03

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

# Check if we're in test mode
_IS_TEST = os.getenv("APP_ENV", "").upper() == "TEST"


class BaseConfig(object):
    SECRET_KEY = os.getenv("SECRET_KEY", "")
    JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "")
    LOGGING_MAX_LOG_SIZE = 512
    LOGGING_BACKUP_COUNT = 1

    # Use properties to defer validation until access time
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

    @property
    def MONGODB_CONNECT_TIMEOUT_MS(self):
        value = os.getenv("MONGODB_CONNECT_TIMEOUT_MS", "5000")
        try:
            return int(value)
        except ValueError:
            raise ValueError("MONGODB_CONNECT_TIMEOUT_MS must be a valid integer")

    @property
    def MONGODB_SERVER_SELECTION_TIMEOUT_MS(self):
        value = os.getenv("MONGODB_SERVER_SELECTION_TIMEOUT_MS", "5000")
        try:
            return int(value)
        except ValueError:
            raise ValueError(
                "MONGODB_SERVER_SELECTION_TIMEOUT_MS must be a valid integer"
            )

    USER_MIN_USERNAME_LENGTH = 3
    USER_MAX_USERNAME_LENGTH = 25
    USER_MIN_PWD_LENGTH = 8
    USER_MAX_PWD_LENGTH = 32
    DATAHUB_TASK_SCAN_INTERVAL = 5
    FLASK_HOST = os.getenv("FLASK_HOST", "0.0.0.0")
    FLASK_PORT = int(os.getenv("FLASK_PORT", 8000))
    FLASK_DEBUG = os.getenv("FLASK_DEBUG", "False").lower() == "true"
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


def get_config():
    env = os.getenv("APP_ENV", "DEV").upper()
    if env == "PRODUCTION" or env == "PROD":
        from app.conf.production_config import ProductionConfig

        return ProductionConfig()
    elif env == "TEST":
        from app.conf.test_config import TestConfig

        return TestConfig()
    else:
        from app.conf.dev_config import DevConfig

        return DevConfig()


app_config = get_config()
