import time
import logging

logger = logging.getLogger()


def func_performance_timer(func):
    start = time.process_time()

    def wrapper(*args, **kwargs):
        resp = func(*args, **kwargs)
        end = time.process_time()
        t = end - start
        logger.info(f'Elapsed time during the function {func.__name__} run: {t:.2f} seconds')
        return resp
    return wrapper
