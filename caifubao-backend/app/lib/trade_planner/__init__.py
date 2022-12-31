import logging
# from app.lib import GeneralFactory
from app.lib.trade_planner import processors


logger = logging.getLogger()


class TradePlanner(object):
    """
    for each trading day, check all the available trading opportunities
    choose most suitable trading opportunity for each portfolio
    a suitable trading opportunity must meet following requirements:
    - the trade must be accomplishable
    - the trade is best suitable for the portfolio's risk preference
    - the trade have the best chance for profiting
    """
    def __init__(self, stock, processor_name):
        module_name = 'TradePlanner'
        meta_type = 'trade_planner'
        processor_registry = processors.registry

    def run(self):
        pass

