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
        bulk_insert_list = []
        for i, row in self.quote_df[self.quote_df[self.factor_name].notna()].iterrows():
            factor_data = FactorDataEntry()
            factor_data.name = self.factor_name
            factor_data.stock_code = self.stock.code
            factor_data.value = row[self.factor_name]
            factor_data.date = i
            bulk_insert_list.append(factor_data)
        FactorDataEntry.objects.insert(bulk_insert_list, load_bulk=False)

        # update_freshness_meta
        latest_factor_date = max(self.quote_df.index)
        freshness_meta_helper.upsert_freshness_meta(self.stock, self.factor_name, latest_factor_date)

    def read_existing_factors(self):
        pass
