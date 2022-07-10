class BasicScenario(object):
    def __init__(self, scheme):
        self.portfolio = None
        self.strategy = None
        self.scheme = scheme

    def exec_scheme(self):
        print('OH YEAH')


class RealOperationScenario(BasicScenario):
    def __init__(self, scheme):
        super().__init__(scheme)


class BackTestScenario(BasicScenario):
    def __init__(self, scheme):
        super().__init__(scheme)
        self.start_date = None
        self.end_date = None
