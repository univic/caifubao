from app.schemes import scenario as scenario_schemes

from app.lib.scenario_director import scenario_processors
"""
how would you like the world to turn?
"""

# load scenario

# real world run or backtest?

# load the strategy file

# do the jobs


class ScenarioDirector(object):

    def __init__(self, scenario_scheme_name):
        self.scenario_processor = None
        self.portfolio = None
        self.scenario_scheme = None
        self.scenario_scheme_name = scenario_scheme_name

    def load_scenario_scheme(self):
        """
        load the scheme class according to the scheme name provided by user
        """
        self.scenario_scheme = getattr(scenario_schemes, self.scenario_scheme_name)

    def load_scenario_processor(self):
        """
        load scenario processor according to the name provided by scenario scheme
        """
        self.scenario_processor = getattr(scenario_processors, self.scenario_scheme.scenario_processor_name)

    def run_scenario(self):
        """
        run the scenario scheme with designated processor
        """
        self.load_scenario_scheme()
        self.load_scenario_processor()
        obj = self.scenario_processor(self.scenario_scheme)
        obj.exec_scheme()


if __name__ == '__main__':
    obj_a = ScenarioDirector(scenario_scheme_name='BackTestScenario')
    obj_a.run_scenario()

