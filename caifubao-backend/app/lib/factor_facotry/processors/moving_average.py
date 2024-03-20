import logging
import talib
from app.utilities import freshness_meta_helper
from app.model.factor import FactorDataEntry
from app.lib.factor_facotry.processors.factor_processor import FactorProcessor

logger = logging.getLogger(__name__)


class MovingAverageFactorProcessor(FactorProcessor):
    def __init__(self, stock_obj, scenario, processor_dict, input_df, *args, **kwargs):
        super().__init__(stock_obj, scenario, processor_dict, input_df)
        self.ma_days = kwargs['MA']
        self.meta_name = f'MA_{self.ma_days}'
        self.factor_name = f'MA_{self.ma_days}'
        self.bulk_insert_list: list = []

    def perform_calc(self):
        self.process_df[self.factor_name] = talib.MA(self.process_df['close_hfq'],
                                                     timeperiod=self.ma_days)

        self.output_df = self.process_df[self.process_df[self.factor_name].notna()]
        # if self.latest_factor_date:
        #     self.output_df = self.output_df[self.output_df.index > self.latest_factor_date]
        for i, row in self.output_df.iterrows():
            factor_data = FactorDataEntry()
            factor_data.name = self.factor_name
            factor_data.stock_code = self.stock_obj.code
            factor_data.value = row[self.factor_name]
            factor_data.date = i
            self.bulk_insert_list.append(factor_data)

    def perform_db_upsert(self):
        FactorDataEntry.objects.insert(self.bulk_insert_list, load_bulk=False)

    def read_existing_factors(self):
        pass
