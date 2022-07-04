from app.schemes import scenario
from app.lib.scenario_director import processors
"""
how would you like the world to turn?
"""

# load scenario

# real world run or backtest?

# load the strategy file

# do the jobs


class ScenarioDirector(object):

    def __init__(self, scenario_scheme):
        scenario_director_obj = getattr(processors, scenario_scheme.scenario_name)


if __name__ == '__main__':
    obj = ScenarioDirector(scenario.ScenarioScheme)
