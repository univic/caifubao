import logging
import datetime
from app.lib.factor_facotry import FactorFactory
from app.lib.signal_man import SignalMan
from app.lib.opportunity_seeker import OpportunitySeeker


logger = logging.getLogger(__name__)


class PeriodicTaskDispatcher(object):

    def __init__(self, strategy_director, portfolio_manager, scenario):
        # get class name
        self.module_name = self.__class__.__name__
        self.strategy_director = strategy_director
        self.portfolio_manager = portfolio_manager
        self.scenario = scenario
        self.current_date = None
        self.current_time = None
        self.is_trading_day: bool = True

    def run(self):
        self.generate_factors()
        self.generate_signals()
        self.generate_trade_plan()

    def generate_factors(self):
        logger.info(f'Preparing to generate factors')
        factor_factory = FactorFactory(strategy_director=self.strategy_director, scenario=self.scenario)
        factor_factory.run()

    def generate_signals(self):
        logger.info(f'Preparing to generate signals')
        stock_signal_name_list = self.strategy_director.get_signal_list()
        for stock in self.stock_list:
            signal_man = SignalMan(stock, stock_signal_name_list)
            signal_man.run()

    def find_trade_opportunities(self):
        logger.info(f'Looking for trade opportunities')
        opportunity_scanner_name_list = self.strategy_director.get_opportunity_scanner_list()
        for stock in self.stock_list:
            opportunity_seeker = OpportunitySeeker(stock, opportunity_scanner_name_list)
            opportunity_seeker.run()

    def generate_trade_plan(self):
        logger.info(f'Determing trading plans')

    def exec_trade_plan(self):
        logger.info(f'Trying to make some money')
