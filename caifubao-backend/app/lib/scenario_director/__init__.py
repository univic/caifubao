import logging
from app.schemes import scenario as scenario_schemes

from app.lib.scenario_director import scenario_processors
"""
how would you like the world to turn?
"""

logger = logging.getLogger()

# load scenario

# real world run or backtest?

# load the strategy file

# do the jobs


class ScenarioDirector(object):
    """
    Scenario director will organize the whole logic process,
    it starts from scenario and turn into corresponding movement.
    """

    def __init__(self, scenario_scheme_name):
        self.scenario_processor = None
        self.portfolio = None
        self.scenario = None
        self.scenario_scheme_name = scenario_scheme_name

    def load_scenario_scheme(self):
        """
        load the strategy class according to the strategy name provided by user
        """
        self.scenario = getattr(scenario_schemes, self.scenario_scheme_name)

    def load_scenario_processor(self):
        """
        load scenario processor according to the name provided by scenario strategy
        """
        self.scenario_processor = getattr(scenario_processors, self.scenario.scenario_processor_name)

    def run_scenario(self):
        """
        run the scenario strategy with designated processor
        """
        self.load_scenario_scheme()
        self.load_scenario_processor()
        logger.info(f'Preparing scenario {self.scenario_scheme_name}')
        obj = self.scenario_processor(self.scenario)
        obj.exec_scheme()


if __name__ == '__main__':
    scenario_director = ScenarioDirector(scenario_scheme_name='BackTestScenarioScheme')
    scenario_director.run_scenario()
