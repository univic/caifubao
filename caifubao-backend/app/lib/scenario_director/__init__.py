import logging
from app.schemes import scenario as scenario_schemes

from app.lib.scenario_director import scenario_processors
"""
how would you like the world to turn?
"""

logger = logging.getLogger(__name__)

# load scenario

# real world run or backtest?

# load the strategy file

# do the jobs


class ScenarioDirector(object):
    """
    Scenario director will organize the whole logic process,
    it starts from scenario and turn into corresponding movement.
    """

    def __init__(self, scenario_name, strategy_name, portfolio_name):
        self.module_name = 'ScenarioDirector'
        self.scenario = None
        self.scenario_name = scenario_name
        self.strategy_name = strategy_name
        self.portfolio_name = portfolio_name
        logger.info(f'Module {self.module_name} is initializing')

    def load_scenario(self, scenario_name):
        """
        load the strategy class according to the strategy name provided by user
        """
        self.scenario_name = scenario_name
        self.scenario = getattr(scenario_schemes, self.scenario_name)
        logger.info(f'Module {self.module_name} - Scenario {scenario_name} loaded')

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


scenario_director = ScenarioDirector()

