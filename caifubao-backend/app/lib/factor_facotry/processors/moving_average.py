import talib
from app.utilities import freshness_meta_helper
from app.model.factor import FactorDataEntry
from app.lib.factor_facotry.processors import FactorProcessor


class MovingAverageFactorProcessor(FactorProcessor):

    def __init__(self, stock_obj, ma_days):
        super().__init__(stock_obj)
        self.ma_days = ma_days
        self.factor_name = f'ma_{ma_days}'

    def perform_factor_calc(self):
        self.stock_obj.quote_df[self.factor_name] = talib.MA(self.stock_obj.quote_df[self.factor_name],
                                                             timeperiod=self.ma_days)

    def read_existing_factors(self):
        pass