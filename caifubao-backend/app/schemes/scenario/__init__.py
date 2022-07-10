import datetime


class BasicScenarioScheme(object):
    market_name = 'A股'
    scenario_processor_name = 'BackTestScenario'
    strategy_name = ""
    portfolio_name = ""
    test_start_date = None


class BackTestScenario(BasicScenarioScheme):
    scenario_processor_name = 'BackTestScenario'
    strategy_name = "Strategy01"
