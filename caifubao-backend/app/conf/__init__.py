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
        'db': 'caifubao-dev',
        'host': '127.0.0.1',
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
        # 'TASK_SCAN_INTERVAL': 5,      # in minutes
    }
    TASK_CONTROLLER_SETTINGS = {
        'DEFAULT_TASK_QUEUE_NUM': 3,
        'MAX_TASK_QUEUE_NUM': 5,
        'TASK_EXEC_INTERVAL': 0.5,     # in seconds
        'TASK_SCAN_INTERVAL': 60,      # in seconds
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
