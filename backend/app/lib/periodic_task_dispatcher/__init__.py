import logging
from app.lib.datahub import Datahub
from app.lib.signal_man import SignalMan
from app.lib.factor_factory import FactorFactory
from app.lib.opportunity_seeker import OpportunitySeeker
from app.lib.db_watcher.mongoengine_tool import mongo_watcher


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
        mongo_watcher.get_db_connection()
        datahub = Datahub()
        datahub.start()
        self.generate_factors()
        # Deprecated: Legacy signals are replaced by datahub automated pipeline
        # self.generate_signals()
        # self.find_trade_opportunities()
        # self.generate_trade_plan()

    def generate_factors(self):
        logger.info("Preparing for factor generation")
        factor_factory = FactorFactory(
            strategy_director=self.strategy_director,
            portfolio_manager=self.portfolio_manager,
            scenario=self.scenario,
        )
        factor_factory.run()

    def generate_signals(self):
        logger.info("Preparing for signal generation")
        signal_man = SignalMan(
            strategy_director=self.strategy_director,
            portfolio_manager=self.portfolio_manager,
            scenario=self.scenario,
        )
        signal_man.run()

    def find_trade_opportunities(self):
        logger.info("Looking for trade opportunities")
        opportunity_seeker = OpportunitySeeker(
            strategy_director=self.strategy_director,
            portfolio_manager=self.portfolio_manager,
            scenario=self.scenario,
        )
        opportunity_seeker.run()

    def generate_trade_plan(self):
        logger.info("Determing trading plans")

    def exec_trade_plan(self):
        logger.info("Trying to make some money")
