import app.lib.back_tester
from app.schemes import strategy


class BasicScenarioProcessor(object):
    def __init__(self, scheme):
        self.portfolio = None
        self.scheme = scheme
        self.strategy_scheme = None

    def exec_scheme(self):
        print('OH YEAH')

    def load_strategy_scheme(self):
        """
        load strategy scheme according to the name provided by scenario scheme
        """
        self.strategy_scheme = getattr(strategy, self.scheme.strategy_name)

    def load_portfolio(self):
        """
        load strategy scheme according to the name provided by scenario scheme
        """
        # self.portfolio = getattr(strategy, self.scenario_scheme.strategy_name)
        pass


class RealOperationScenarioProcessor(BasicScenarioProcessor):
    def __init__(self, scheme):
        super().__init__(scheme)


class BackTestScenarioProcessor(BasicScenarioProcessor):
    """
    Single stock single strategy back testing scenario
    """
    def __init__(self, scheme):
        super().__init__(scheme)
        self.start_date = None
        self.end_date = None

    def exec_scheme(self):
        back_tester_obj = getattr(app.lib.back_tester, self.scheme.back_tester_name)
        back_tester = back_tester_obj()
        back_tester.run_back_test()
