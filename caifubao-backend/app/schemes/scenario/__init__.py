

class BasicScenarioScheme(object):
    scenario_name = None
    scenario_type = None
    market_name = 'A股'
    strategy_name = None
    portfolio_name = ""
    test_start_date = None


class BackTestScenarioScheme(BasicScenarioScheme):
    scenario_name = "backtest"
    scenario_type = "backtest"
    scenario_processor_name = 'BackTestScenarioProcessor'
    back_tester_name = "BackTesterA"
    strategy_name = "Strategy01"
    start_date = "2016-01-01"


class RealOperationScenarioScheme(BasicScenarioScheme):
    scenario_name = "real_operation"
    scenario_type = "real"
    strategy_name = "Strategy01"
