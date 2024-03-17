import logging
import datetime
import pandas as pd
import numpy as np

from app.utilities import trading_day_helper
from app.model.stock import StockDailyQuote, BasicStock
from app.lib.factor_facotry.processors.factor_processor import FactorProcessor

logger = logging.getLogger(__name__)


class FQFactorProcessor(FactorProcessor):

    def __init__(self, stock_obj, scenario, processor_dict, input_df, *args, **kwargs):
        super().__init__(stock_obj, scenario, processor_dict, input_df)
        self.meta_name = 'FQ_FACTOR'
        self.process_df = self.input_df

    def exec(self):
        self.perform_factor_calc()

    def perform_factor_calc(self):

        # raw_df = self.input_df
        # most_recent_factor_date = datetime.datetime(2022, 6, 20, 0, 0, 0)
        # if found existing factor, slice the df to reduce calculate work
        # if self.most_recent_factor_date:
        #     # get previous quote data to make cumprod possible
        #     head_index = raw_df.index.get_loc(self.most_recent_factor_date) - 1
        #     df = raw_df.iloc[head_index:][:]
        #
        # else:
        #     df = raw_df

        # do the maths
        self.process_df["fq_factor"] = (self.process_df["close"] / self.process_df["previous_close"]).cumprod()
        self.process_df["close_hfq"] = (self.process_df["fq_factor"] * self.input_df.iloc[0]['previous_close']).round(
            decimals=4)
        self.process_df["open_hfq"] = (self.process_df["open"] * (self.process_df["close_hfq"] / self.process_df["close"])).round(
            decimals=4)
        self.process_df["high_hfq"] = (self.process_df["high"] * (self.process_df["close_hfq"] / self.process_df["close"])).round(
            decimals=4)
        self.process_df["low_hfq"] = (self.process_df["low"] * (self.process_df["close_hfq"] / self.process_df["close"])).round(
            decimals=4)
        self.output_df = self.process_df

    def perform_db_upsert(self):
        # update database
        for i, row in self.output_df.iterrows():
            field_list = ["fq_factor", "close_hfq", "open_hfq", "high_hfq", "low_hfq"]
            quote_obj = StockDailyQuote.objects(code=row["code"], date=i).first()
            for field in field_list:
                quote_obj[field] = row[field]
            quote_obj.save()


# if __name__ == '__main__':
#     from app.lib.db_tool import mongoengine_tool
#     mongoengine_tool.connect_to_db()
#     stock_obj = BasicStock.objects(code="sh601166").first()
#     obj = FQFactorProcessor(stock_obj)
#     obj.perform_factor_calc()
