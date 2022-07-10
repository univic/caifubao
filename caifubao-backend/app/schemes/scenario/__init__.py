

class BasicScenarioScheme(object):
    market_name = 'A股'
    strategy_name = ""
    portfolio_name = ""
    test_start_date = None


class BackTestScenario(BasicScenarioScheme):
    scenario_processor_name = 'BackTestScenarioProcessor'
    back_tester_name = "BackTesterA"
    strategy_name = "Strategy01"
    start_date = "2016-01-01"
