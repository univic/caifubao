# -*- coding: utf-8 -*-
# Author : univic
# Date: 2021-02-03


class BaseConfig(object):
    SECRET_KEY = ""
    USE_CONFIG = 'DEV'
    LOGGING = {
        'MAX_LOG_SIZE': 512,    # in KBytes
        'BACKUP_COUNT': 1
    }
    # mongodb 配置
    MONGODB_SETTINGS = {
        'db': 'stellaris',
        'host': 'mongodb://localhost:27017/',
        'port': 27017,
    }
    # user config
    USER_SETTINGS = {
        'MIN_USERNAME_LENGTH': 3,
        'MAX_USERNAME_LENGTH': 25,
        'MIN_PWD_LENGTH': 8,
        'MAX_PWD_LENGTH': 32,
    }
    DATAHUB_SETTINGS = {
        'TASK_SCAN_INTERVAL': 1,      # in minutes
    }


def get_config():
    if BaseConfig.USE_CONFIG == 'DEV':
        from app.conf.dev_config import DevConfig
        return DevConfig
    elif BaseConfig.USE_CONFIG == 'PRODUCTION':
        from app.conf.production_config import ProductionConfig
        return ProductionConfig
    # import app.config.flask_security_config


app_config = get_config()

