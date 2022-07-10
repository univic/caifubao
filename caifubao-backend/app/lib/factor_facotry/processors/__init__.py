from app.utilities import freshness_meta_helper
from app.lib.factor_facotry.processors import fq_factor, moving_average


factor_registry = {
    "FQ_FACTOR": {
        'processor_object': fq_factor.FQFactorProcessor,
        'handler': 'perform_factor_calc'
    },
    "MA_10": {
        'processor_object': moving_average.MovingAverageFactorProcessor,
        'handler': 'perform_ma_10_calc'
    }
}


class FactorProcessor(object):
    """
    Base class for all the factor scenario_processors
    """

    def __init__(self, stock_obj):
        self.stock_obj = stock_obj
        self.factor_name = None
        self.most_recent_factor_date = None

    @classmethod
    def check_exec_availiablity(cls, stock_obj):
        """
        decide whether to run this processor for the incoming stock
        """

    def before_exec(self):
        self.check_dependency()

    def exec(self):
        self.before_exec()
        self.read_freshness_meta()
        if self.most_recent_factor_date:
            self.read_existing_factors()
        self.perform_factor_calc()
        self.perform_db_upsert()
        self.after_exec()

    def perform_factor_calc(self):
        pass

    def check_dependency(self):
        pass

    def read_freshness_meta(self):
        self.most_recent_factor_date = freshness_meta_helper.read_freshness_meta(self.stock_obj.code, self.factor_name)

    def read_existing_factors(self):
        pass

    def perform_db_upsert(self):
        pass

    def after_exec(self):
        pass
