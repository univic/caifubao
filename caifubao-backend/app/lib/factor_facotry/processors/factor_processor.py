
from app.utilities import freshness_meta_helper


class FactorProcessor(object):
    """
    Base class for all the factor scenario_processors
    """

    def __init__(self, stock, quote_df, *args, **kwargs):
        self.stock = stock
        self.quote_df = quote_df
        self.factor_name = None
        self.most_recent_factor_date = None

    def before_exec(self):
        pass

    def after_exec(self):
        pass

    def perform_factor_calc(self, *args, **kwargs):
        pass

    def generate_factor(self):
        self.before_exec()
        self.perform_factor_calc()
        self.perform_db_upsert()
        self.update_freshness_meta()
        self.after_exec()

    def update_freshness_meta(self):
        last_factor_date = self.quote_df.index[-1]
        freshness_meta_helper.upsert_freshness_meta(self.stock, self.factor_name, 'factor', last_factor_date)

    def read_existing_factors(self):
        pass

    def perform_db_upsert(self):
        pass
