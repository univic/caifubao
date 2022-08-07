import talib
from app.utilities import freshness_meta_helper
from app.model.factor import FactorDataEntry
from app.lib.factor_facotry.processors.factor_processor import FactorProcessor


class MovingAverageFactorProcessor(FactorProcessor):

    def __init__(self, stock, quote_df):
        super().__init__(stock, quote_df)
        self.ma_days = 0
        self.factor_name = ''

    def perform_factor_calc(self):

        self.stock.quote_df[self.factor_name] = talib.MA(self.stock.quote_df[self.factor_name],
                                                         timeperiod=self.ma_days)

    def perform_ma_10_calc(self):
        self.ma_days = 10
        self.factor_name = f'ma_{self.ma_days}'
        self.perform_factor_calc()

    def read_existing_factors(self):
        pass
