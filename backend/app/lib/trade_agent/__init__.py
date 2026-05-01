import logging
import datetime
import traceback

from app.utilities import trading_day_helper
from app.lib.strategy import StrategyDirecter
from app.lib.scenario_director import ScenarioDirector
from app.lib.portfolio_manager import PortfolioManager
from app.lib.report_maker import daily_report_maker
from app.lib.db_watcher.mongoengine_tool import mongo_watcher
from app.lib.periodic_task_dispatcher import PeriodicTaskDispatcher
from app.lib.signal_man import SignalMan
from app.lib.factor_factory import FactorFactory
from app.lib.opportunity_seeker import OpportunitySeeker


logger = logging.getLogger(__name__)


class RealOperationAgent(object):
    def __init__(self, portfolio_name, strategy_name):
        # get class name
        self.module_name = self.__class__.__name__
        logger.info(f"Module {self.module_name} is initializing")
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

    # def initialize(self):
    #     pass
    #     # load scenario
    #
    #     # load strategy
    #
    #     self.check_prerequisite()
    #
    #     # start tick control
    #
    #     self.main_sequence()
    #
    #     self.compose_report()
    #
    #     # send message

    def run(self):

        continue_flag = True
        if continue_flag:
            logger.info(
                f"Starting - Using Strategy {self.strategy_name}, portfolio {self.portfolio_name}"
            )
            try:
                self.check_prerequisite()
                self.before_run()
                self.main_sequence()
                self.after_run()
                daily_report_maker.add_content(
                    "summary", "Operation tick successfully completed."
                )
                # time.sleep(wait_time)
            except Exception as e:
                msg_text = (
                    f"Operation tick encountered following exception: \r\n"
                    f"{traceback.format_exception(e)}"
                )
                daily_report_maker.add_content("summary", msg_text)
                traceback.print_exc()
                logger.error(msg_text)
            logger.info("Scheduled run completed.")
            # time.sleep(next_run_wait_time)

    def before_run(self):
        mongo_watcher.get_db_connection()
        self.scenario = ScenarioDirector()

        self.strategy_director = StrategyDirecter()
        self.strategy_director.load_strategy(self.strategy_name)

        self.trade_calendar = self.strategy_director.get_market_trade_calendar()
        self.scenario.update_dt(trade_calendar=self.trade_calendar)

        self.portfolio_manager = PortfolioManager()
        self.portfolio_manager.load_portfolio(self.portfolio_name)

        self.periodic_task_dispatcher = PeriodicTaskDispatcher(
            strategy_director=self.strategy_director,
            portfolio_manager=self.portfolio_manager,
            scenario=self.scenario,
        )
        current_date_str = trading_day_helper.get_current_date_str()
        msg_subject = f"Real Operation Report - {current_date_str}"
        daily_report_maker.set_subject(msg_subject)

    def main_sequence(self):
        self.generate_factors()
        self.generate_signals()
        # self.find_trade_opportunities()
        # self.generate_trade_plan()

    def after_run(self):
        pass

    def compose_report(self):
        pass

    def determine_next_run_wait_time(self):
        next_run_time = trading_day_helper.determine_the_next_trading_day_end(
            trade_calendar=self.trade_calendar,
            given_time=datetime.datetime.now(),
            end_hour=15,
        )
        wait_time = trading_day_helper.measure_time_difference(
            datetime.datetime.now(), next_run_time
        )
        return wait_time

    def check_prerequisite(self):
        pass

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
