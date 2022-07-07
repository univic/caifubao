from app.schemes import scenario as scenario_schemes
from app.lib.scenario_director import scenarios
"""
how would you like the world to turn?
"""

# load scenario

# real world run or backtest?

# load the strategy file

# do the jobs


class ScenarioDirector(object):

    def __init__(self, scenario_scheme_name):
        self.scenario = None
        self.scenario_scheme = None
        self.scenario_scheme_name = scenario_scheme_name

    def load_scenario_scheme(self):
        self.scenario_scheme = getattr(scenario_schemes, self.scenario_scheme_name)

    def load_scenario(self):
        self.scenario = getattr(scenarios, self.scenario_scheme_name)

    def run_scenario(self):
        pass


if __name__ == '__main__':
    obj = ScenarioDirector(scenario.ScenarioScheme)
