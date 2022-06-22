

class FactorProcessor(object):
    """
    Base class for all the factor processors
    """

    def __init__(self, stock_obj):
        self.stock_obj = stock_obj

    @classmethod
    def check_exec_availiablity(cls, stock_obj):
        """
        decide whether to run this processor for the incoming stock
        """

    def before_exec(self):
        self.check_dependency()

    def exec(self):
        self.before_exec()
        self.perform_factor_calc()
        self.perform_db_upsert()
        self.after_exec()

    def perform_factor_calc(self):
        pass

    def check_dependency(self):
        pass

    def perform_db_upsert(self):
        pass

    def after_exec(self):
        pass
