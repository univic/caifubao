import logging
import talib
from app.utilities import freshness_meta_helper
from app.model.factor import FactorDataEntry
from app.lib.factor_facotry.processors.factor_processor import FactorProcessor

logger = logging.getLogger()


class MovingAverageFactorProcessor(FactorProcessor):

    def __init__(self, stock, quote_df, ma_days):
        super().__init__(stock, quote_df)
        self.ma_days = ma_days
        self.factor_name = f'MA_{self.ma_days}'

    def perform_factor_calc(self):
        # TODO: use bulk insert
        self.quote_df[self.factor_name] = talib.MA(self.quote_df[self.factor_name],
                                                   timeperiod=self.ma_days)
        # update database

    def read_existing_factors(self):
        pass
