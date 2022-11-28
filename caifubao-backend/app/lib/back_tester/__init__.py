import logging
import datetime
from app.lib.db_tool import mongoengine_tool
from app.lib.strategy import StrategyInterpreter
from app.lib.portfolio_manager import PortfolioManager
from app.lib.factor_facotry import FactorFactory
from app.lib.signal_man import SignalMan
from app.utilities import trading_day_helper

logger = logging.getLogger()


class BasicBackTester(object):

    def __init__(self, scenario, portfolio, strategy):
        self.scenario = scenario
        self.trading_day_list = []
        self.current_trading_day = None
        self.stock_list = []
        self.strategy_interpreter = StrategyInterpreter(strategy)
        self.portfolio_manager = PortfolioManager()
        self.backtest_date_list = None

    def run_back_test(self):
        logger.info(f'Running backtest {self.scenario.back_tester_name}')
        logger.info(f'Using strategy {self.scenario.strategy_name}')
        self.before_back_test()
        self.get_backtest_date_range()
        self.get_stock_list()

        # overall analysis
        pass

        # generate factors

        self.generate_factors()
        self.generate_signals()
        self.generate_trade_plan()

        # daily analysis
        # for trading_day in self.backtest_date_list:
        #     self.current_trading_day = trading_day

        self.after_back_test()

    def before_back_test(self):
        mongoengine_tool.connect_to_db()

    def after_back_test(self):
        mongoengine_tool.disconnect_from_db()

    def get_backtest_date_range(self):
        start_date_str = self.scenario.start_date
        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d')
        trade_calendar = trading_day_helper.get_a_stock_market_trade_calendar()
        self.backtest_date_list = [date for date in trade_calendar if date >= start_date]

    def get_stock_list(self):
        self.stock_list = self.strategy_interpreter.get_stock_list()

    def generate_factors(self):
        for stock in self.stock_list:
            stock_factor_name_list = self.strategy_interpreter.get_stock_factor_list()
            factor_factory = FactorFactory(stock, stock_factor_name_list)
            factor_factory.run_factor_factory()

    def generate_signals(self):
        logger.info(f'Preparing to generate signals')
        stock_signal_name_list = self.strategy_interpreter.get_signal_list()
        for stock in self.stock_list:
            signal_man = SignalMan(stock, stock_signal_name_list)
            signal_man.run()

    def scan_trade_opportunities(self):
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


if __name__ == '__main__':
    pass

