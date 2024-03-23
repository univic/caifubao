import logging
import datetime
from app.utilities import trading_day_helper

# from app.lib.scenario_director import scenario_processors

logger = logging.getLogger(__name__)


class ScenarioDirector(object):
    """
    Scenario director will organize the whole logic process,
    it starts from scenario and turn into corresponding movement.
    """

    def __init__(self):
        self.module_name = self.__class__.__name__
        logger.info(f'Module {self.module_name} is initializing')

        self.is_backtest: bool = False
        self.backtest_name: str = ""
        self.backtest_current_datetime = None
        self.backtest_current_most_recent_trading_day = None
        self.real_world_datetime = None
        self.real_world_most_recent_trading_datetime = None
        self.current_datetime = None
        self.current_datetime_most_recent_trading_datetime = None

    def activate_backtest_mode(self, backtest_name:str):
        pass

    def update_dt(self, trade_calendar, backtest_current_datetime=None):
        self.real_world_datetime = datetime.datetime.now()
        self.real_world_most_recent_trading_datetime = trading_day_helper.determine_most_recent_previous_trading_dt(
            trade_calendar=trade_calendar,
            given_time=self.real_world_datetime
        )
        if self.is_backtest:
            if backtest_current_datetime:
                self.backtest_current_datetime = backtest_current_datetime
                self.current_datetime = backtest_current_datetime
                self.backtest_current_most_recent_trading_day = trading_day_helper.determine_most_recent_previous_trading_dt(
                    trade_calendar=trade_calendar,
                    given_time=self.real_world_datetime
                )
                self.current_datetime_most_recent_trading_datetime = self.backtest_current_most_recent_trading_day
            else:
                logger.error(f'backtest time not filled')
        else:
            self.current_datetime = self.real_world_datetime
            self.current_datetime_most_recent_trading_datetime = self.real_world_most_recent_trading_datetime

    # def load_scenario(self, scenario_name):
    #     """
    #     load the strategy class according to the strategy name provided by user
    #     """
    #     self.scenario_name = scenario_name
    #     self.scenario = getattr(scenario_schemes, self.scenario_name)
    #     logger.info(f'Module {self.module_name} - Scenario {scenario_name} loaded')
    #
    # def get_scenario(self, scenario_name):
    #     self.load_scenario(scenario_name)
    #     return self.scenario

    # def load_scenario_processor(self):
    #     """
    #     load scenario processor according to the name provided by scenario strategy
    #     """
    #     self.scenario_processor = getattr(scenario_processors, self.scenario.scenario_processor_name)

    # def run_scenario(self):
    #     """
    #     run the scenario strategy with designated processor
    #     """
    #     self.load_scenario()
    #     self.load_scenario_processor()
    #     logger.info(f'Preparing scenario {self.scenario_name}')
    #     obj = self.scenario_processor(self.scenario)
    #     obj.exec_scheme()


# scenario_director = ScenarioDirector()


