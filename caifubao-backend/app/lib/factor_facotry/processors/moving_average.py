import logging
import talib
from app.utilities import freshness_meta_helper
from app.model.factor import FactorDataEntry
from app.lib.factor_facotry.processors.factor_processor import FactorProcessor

logger = logging.getLogger(__name__)


class MovingAverageFactorProcessor(FactorProcessor):

    def __init__(self, stock, quote_df, latest_factor_date, *args, **kwargs):
        super().__init__(stock, quote_df, latest_factor_date)
        self.ma_days = kwargs['MA']
        self.factor_name = f'MA_{self.ma_days}'

    def perform_factor_calc(self):
        self.quote_df[self.factor_name] = talib.MA(self.quote_df['close_hfq'],
                                                   timeperiod=self.ma_days)
        pass
        # update database
        bulk_insert_list = []
        trimmed_quote_df = self.quote_df[self.quote_df[self.factor_name].notna()]
        if self.latest_factor_date:
            trimmed_quote_df = trimmed_quote_df[trimmed_quote_df.index > self.latest_factor_date]
        for i, row in trimmed_quote_df.iterrows():
            factor_data = FactorDataEntry()
            factor_data.name = self.factor_name
            factor_data.stock_code = self.stock.code
            factor_data.value = row[self.factor_name]
            factor_data.date = i
            bulk_insert_list.append(factor_data)
        FactorDataEntry.objects.insert(bulk_insert_list, load_bulk=False)

        # update_freshness_meta
        latest_factor_date = max(self.quote_df.index)
        freshness_meta_helper.upsert_freshness_meta(self.stock, self.factor_name, 'factor', latest_factor_date)

    def read_existing_factors(self):
        pass
