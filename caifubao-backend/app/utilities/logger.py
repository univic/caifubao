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

    # create console hist_quote_handler and set level to debug
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    # dynamically determine the log file path
    splited_path = pwd.split(os.sep)
    if splited_path[-1] == 'caifubao-backend':
        log_file_path = os.path.join(pwd, 'app', 'log', log_file)
    else:
        i = splited_path.index('caifubao-backend')
        path_str = os.sep.join(splited_path[:i + 1])
        log_file_path = os.path.join(path_str, 'app', 'log', log_file)

    # Create a file hist_quote_handler
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
