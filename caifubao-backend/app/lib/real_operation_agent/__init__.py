import logging
import datetime
from app.lib.strategy import StrategyDirecter
from app.lib.scenario_director import ScenarioDirector
from app.lib.portfolio_manager import PortfolioManager
from app.lib.periodic_task_dispatcher import PeriodicTaskDispatcher


logger = logging.getLogger(__name__)


class RealOperationAgent(object):
    def __init__(self, portfolio_name, strategy_name):
        # get class name
        self.module_name = self.__class__.__name__
        logger.info(f'Module {self.module_name} is initializing')
        self.trade_calendar: list = []
        self.scenario = None
        self.strategy_director = None
        self.portfolio_manager = None

        self.strategy_name = strategy_name
        self.portfolio_name = portfolio_name
        self.periodic_task_dispatcher = None

    def run(self):
        logger.info(f'Starting - Using Strategy {self.strategy_name}, portfolio {self.portfolio_name}')
        self.before_run()
        self.main_sequence()
        self.after_run()
        logger.info(f'Scheduled run completed.')

    def before_run(self):
        self.scenario = ScenarioDirector()

        self.strategy_director = StrategyDirecter()
        self.strategy_director.load_strategy(self.strategy_name)

        self.trade_calendar = self.strategy_director.get_market_trade_calendar()
        self.scenario.update_dt(trade_calendar=self.trade_calendar)

        self.portfolio_manager = PortfolioManager()
        self.portfolio_manager.load_portfolio(self.portfolio_name)

        self.periodic_task_dispatcher = PeriodicTaskDispatcher(strategy_director=self.strategy_director,
                                                               portfolio_manager=self.portfolio_manager,
                                                               scenario=self.scenario)

    def main_sequence(self):
        self.periodic_task_dispatcher.run()

    def after_run(self):
        pass

    def compose_report(self):
        pass
