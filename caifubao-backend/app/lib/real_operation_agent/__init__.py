import logging
import datetime
import time
import traceback

from app.utilities import trading_day_helper
from app.lib.strategy import StrategyDirecter
from app.lib.scenario_director import ScenarioDirector
from app.lib.portfolio_manager import PortfolioManager
from app.lib.report_maker import daily_report_maker
from app.lib.db_watcher.mongoengine_tool import db_watcher
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
        self.msg_content_dict = {
            "Summary": [],
            "Finding": [],
        }

    def run(self):
        continue_flag = True
        if continue_flag:
            logger.info(f'Starting - Using Strategy {self.strategy_name}, portfolio {self.portfolio_name}')
            try:
                self.before_run()
                self.main_sequence()
                self.after_run()
                daily_report_maker.add_content('summary', 'Real operation tick successfully completed.')
            except Exception as e:
                msg_text = f'Real operation tick encountered following exception: \r\n' \
                           f'{traceback.format_exception(e)}'
                daily_report_maker.add_content('summary', msg_text)
                logger.error(msg_text)
            logger.info(f'Scheduled run completed.')
            next_run_wait_time = self.determine_next_run_wait_time()
            time.sleep(next_run_wait_time)

    def before_run(self):
        db_watcher.get_db_connection()
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
        current_date_str = trading_day_helper.get_current_date_str()
        msg_subject = f"CFB Real Operation Report - {current_date_str}"
        daily_report_maker.set_subject(msg_subject)


    def main_sequence(self):
        self.periodic_task_dispatcher.run()

    def after_run(self):
        pass

    def compose_report(self):
        pass

    def determine_next_run_wait_time(self):
        next_run_time = trading_day_helper.determine_the_next_trading_day_end(trade_calendar=self.trade_calendar,
                                                                              given_time=datetime.datetime.now(),
                                                                              end_hour=15)
        wait_time = trading_day_helper.measure_time_difference(datetime.datetime.now(), next_run_time)
        return wait_time
