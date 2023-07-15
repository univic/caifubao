#!/usr/bin/env python
# -*- coding: utf-8 -*-

class Default(object):
    scenario_name = None
    scenario_type = None
    market_code_list: list = ['China_A_Stock', ]
    portfolio_name = ""
    start_date = None
    end_date = None
    current_date = None


class BackTest(Default):
    scenario_name = "backtest"
    scenario_type = "backtest"
    scenario_processor_name = 'BackTestScenarioProcessor'
    back_tester_name = "BackTesterA"
    strategy_name = "Strategy01"
    start_date = "2016-01-01"


class RealOperation(Default):
    scenario_name = "real_operation"
    scenario_type = "real"
    strategy_name = "Strategy01"
