"""
how we deal with information and make effective decision?
"""


class StrategyInterpreter(object):

    def __init__(self, strategy_scheme):
        self.scheme = strategy_scheme

    def parse_stock_scope(self):
        stock_list = []
        if self.scheme.stock_scope == "single":
            pass

    def parse_factors(self):
        pass

    def parse_signals(self):
        pass

    def parse_opportunity_seeker(self):
        pass

