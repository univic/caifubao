import app.lib.back_tester
from app.schemes import strategy


class BasicScenarioProcessor(object):
    def __init__(self, scheme):
        self.portfolio = None
        self.scenario = scheme
        self.strategy = None
        self.setup_scenario()

    def setup_scenario(self):
        """
        load strategy and portfolios
        :return:
        """
        self.load_strategy_scheme()
        # self.load_portfolio()

    def load_strategy_scheme(self):
        """
        load strategy according to the name provided by scenario strategy
        """
        self.strategy = getattr(strategy, self.scenario.strategy_name)

    def load_portfolio(self):
        """
        load strategy according to the name provided by scenario strategy
        """
        # self.portfolio = getattr(strategy, self.scenario_scheme.strategy_name)
        pass

    def exec_scheme(self):
        print('OH YEAH')


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
        back_tester_obj = getattr(app.lib.back_tester, self.scenario.back_tester_name)
        back_tester = back_tester_obj(scenario=self.scenario, portfolio=self.portfolio, strategy=self.strategy)
        back_tester.run_back_test()
