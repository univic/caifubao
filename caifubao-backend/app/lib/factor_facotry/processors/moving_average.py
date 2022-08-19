import logging
import talib
from app.utilities import freshness_meta_helper
from app.model.factor import FactorDataEntry
from app.lib.factor_facotry.processors.factor_processor import FactorProcessor

logger = logging.getLogger()


class MovingAverageFactorProcessor(FactorProcessor):

    def __init__(self, stock, quote_df, *args, **kwargs):
        super().__init__(stock, quote_df)
        self.ma_days = kwargs['MA']
        self.factor_name = f'MA_{self.ma_days}'

    def perform_factor_calc(self):
        # TODO: use bulk insert
        self.quote_df[self.factor_name] = talib.MA(self.quote_df['close_hfq'],
                                                   timeperiod=self.ma_days)
        pass
        # update database

    def read_existing_factors(self):
        pass
