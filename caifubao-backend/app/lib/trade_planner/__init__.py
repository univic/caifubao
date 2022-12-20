import logging
# from app.lib import GeneralFactory
from app.lib.trade_planner import processors


logger = logging.getLogger()


class TradePlanner(object):
    def __init__(self, stock, processor_name):
        module_name = 'TradePlanner'
        meta_type = 'trade_planner'
        processor_registry = processors.registry

    def run(self):
        pass

