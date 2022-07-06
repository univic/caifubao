class BasicScenario(object):
    def __init__(self):
        portfolio = None
        strategy = None


class RealOperationScenario(BasicScenario):
    def __init__(self):
        super().__init__()


class BackTestScenario(BasicScenario):
    def __init__(self):
        super().__init__()
        start_date = None
        end_date = None
