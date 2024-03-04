import logging
import datetime
from app.lib.db_watcher import mongoengine_tool
from app.utilities.progress_bar import progress_bar
from app.lib.strategy import StrategyDirecter
from app.lib.portfolio_manager import PortfolioManager
from app.lib.periodic_task_dispatcher import PeriodicTaskDispatcher

from app.utilities import trading_day_helper

logger = logging.getLogger(__name__)


class BasicBackTester(object):

    def __init__(self, portfolio_name, strategy_name, start_date, end_date=None):
        # get class name
        self.module_name = self.__class__.__name__

        self.trading_day_list = []

        self.stock_list = []
        self.strategy_director = None
        self.portfolio_manager = None

        self.strategy_name = strategy_name
        self.portfolio_name = portfolio_name

        self.periodic_task_dispatcher = None

        self.start_date = start_date
        self.end_date = None
        self.backtest_periodic_task_list = None
        self.current_trading_day = None
        logger.info(f'Module {self.module_name} is initializing')

    def run(self):
        logger.info(f'Running backtest, using strategy {self.strategy_name}')
        self.before_run()
        self.get_backtest_periodic_task()
        self.periodic_task_dispatcher = PeriodicTaskDispatcher()
        task_list_len = len(self.backtest_periodic_task_list)
        logger.info(f'Starting periodic tasks, {task_list_len} tasks in total')
        prog_bar = progress_bar()
        for i, t_day in enumerate(self.backtest_periodic_task_list):
            self.exec_backtest_periodic_task(t_day)
            prog_bar(i, task_list_len)

        self.after_run()

    def get_strategy_director(self):
        self.strategy_director = StrategyDirecter()
        self.strategy_director.load_strategy(self.strategy_name)

    def get_portfolio_manager(self):
        self.portfolio_manager = PortfolioManager()
        self.portfolio_manager.load_portfolio(self.portfolio_name)

    def exec_backtest_periodic_task(self, current_date):
        self.periodic_task_dispatcher.run(self.strategy_director, self.portfolio_manager, current_date)

    def before_run(self):
        pass

    def after_run(self):
        pass

    def get_backtest_periodic_task(self):
        start_date_str = self.start_date
        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d')
        trade_calendar = trading_day_helper.get_a_stock_market_trade_calendar()
        self.backtest_periodic_task_list = [date for date in trade_calendar if date >= start_date]

    def generate_backtest_report(self):
        pass


if __name__ == '__main__':
    pass

