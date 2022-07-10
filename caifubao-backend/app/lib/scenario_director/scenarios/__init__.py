class BasicScenario(object):
    def __init__(self, scheme):
        portfolio = None
        strategy = None

    def exec_scheme(self):
        print('OH YEAH')


class RealOperationScenario(BasicScenario):
    def __init__(self):
        super().__init__()


class BackTestScenario(BasicScenario):
    def __init__(self):
        super().__init__()
        start_date = None
        end_date = None
