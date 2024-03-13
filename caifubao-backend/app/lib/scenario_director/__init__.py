import logging

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
        self.real_world_datetime = None
        self.real_world_most_recent_trading_day = None
        self.current_datetime = None
        self.current_most_recent_trading_day = None
        self.backtest_name: str = ""

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


