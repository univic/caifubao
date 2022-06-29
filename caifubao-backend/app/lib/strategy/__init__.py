from app.model.stock import BasicStock

"""
how we deal with information and make effective decision?
"""


class StrategyInterpreter(object):

    def __init__(self, strategy_scheme):
        self.scheme = strategy_scheme

    def get_stock_list(self):
        stock_obj_list = []
        if self.scheme.stock_scope == "single":
            for code in self.scheme.stock_code_list:
                stock_obj = BasicStock.objects(code=code).first()
                stock_obj_list.append(stock_obj)
        return stock_obj_list

    def parse_factors(self):
        for factor in self.scheme.factor_list:
            pass

    def parse_signals(self):
        for signal in self.scheme.signal_list:
            pass

    def parse_opportunity_seeker(self):
        pass

