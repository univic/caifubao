import datetime
from app.lib.db_tool import mongoengine_tool
from app.lib.strategy import StrategyInterpreter
from app.lib.portfolio_manager import PortfolioManager
from app.utilities import trading_day_helper


class BasicBackTester(object):

    def __init__(self, scenario, portfolio, strategy):
        self.scenario = scenario
        self.trading_day_list = []
        self.stock_list = []
        self.strategy_interpreter = StrategyInterpreter(strategy)
        self.portfolio_manager = PortfolioManager()
        self.backtest_date_list = None

    def run_back_test(self):
        print('backtest running')
        self.get_backtest_date_range()
        for trading_day in self.backtest_date_list:
            self.perform_daily_analysis()
        # self.before_back_test()
        # self.after_back_test()

    def before_back_test(self):
        mongoengine_tool.connect_to_db()

    def after_back_test(self):
        mongoengine_tool.disconnect_from_db()

    def get_backtest_date_range(self):
        start_date_str = self.scenario.start_date
        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d')
        trade_calendar = trading_day_helper.get_a_stock_market_trade_calendar()
        self.backtest_date_list = [date for date in trade_calendar if date >= start_date]

    def setup_backtest(self, scenario):
        self.scenario = scenario

    def get_stock_list(self):
        pass

    def get_quote_list(self):
        pass

    def perform_daily_analysis(self):
        self.generate_factors()
        self.generate_signals()
        self.generate_trade_plan()

    def generate_factors(self):
        pass

    def generate_signals(self):
        pass

    def generate_trade_plan(self):
        pass

    def exec_trade_plan(self):
        pass

    def generate_backtest_report(self):
        pass


class BackTesterA(BasicBackTester):
    """
    Single stock single strategy back tester
    """
    pass
