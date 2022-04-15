import os
import sys
import logging
from logging import handlers
from app.conf import app_config


def create_logger(log_file='stellaris.log'):
    pwd = os.getcwd()
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Create a standard formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s:%(lineno)s - %(levelname)s - %(message)s')

    # create console handler and set level to debug
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    log_file_path = os.path.join(pwd, 'app', 'log', log_file)
    log_file_path = f"E:\\DEV\\stellaris\\stellaris-backend\\app\\log\\{log_file}"

    # Create a file handler
    file_handler = handlers.RotatingFileHandler(log_file_path,
                                                mode='a',
                                                maxBytes=app_config.LOGGING['MAX_LOG_SIZE'] * 1024,
                                                backupCount=app_config.LOGGING['BACKUP_COUNT'],
                                                encoding=None)

    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Attach the handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
