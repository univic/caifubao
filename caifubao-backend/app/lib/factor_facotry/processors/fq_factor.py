import datetime
import pandas as pd
import numpy as np

from app.utilities import freshness_meta_helper, trading_day_helper
from app.model.stock import StockDailyQuote, BasicStock
from app.lib.factor_facotry.processors import FactorProcessor


class FQFactorProcessor(FactorProcessor):

    def __init__(self, stock_obj):
        super().__init__(stock_obj)
        self.factor_name = 'fq_factor'

    def read_freshness_meta(self):
        self.most_recent_factor_date = freshness_meta_helper.read_freshness_meta(self.stock_obj, 'fq_factor')

    def perform_factor_calc(self):

        raw_df = self.quote_df
        # most_recent_factor_date = datetime.datetime(2022, 6, 20, 0, 0, 0)
        # if found existing factor, slice the df to reduce calculate work
        if self.most_recent_factor_date:
            # get previous quote data to make cumprod possible
            head_index = raw_df.index.get_loc(self.most_recent_factor_date) - 1
            df = raw_df.iloc[head_index:][:]
        else:
            df = raw_df

        # do the maths
        df["fq_factor"] = (df["close"] / df["previous_close"]).cumprod()
        df["close_hfq"] = (df["fq_factor"] * raw_df.iloc[0]['previous_close']).round(
            decimals=4)
        df["open_hfq"] = (df["open"] * (df["close_hfq"] / df["close"])).round(
            decimals=4)
        df["high_hfq"] = (df["high"] * (df["close_hfq"] / df["close"])).round(
            decimals=4)
        df["low_hfq"] = (df["low"] * (df["close_hfq"] / df["close"])).round(
            decimals=4)

        # update database
        # for i, row in df.iterrows():
        #     field_list = ["fq_factor", "close_hfq", "open_hfq", "high_hfq", "low_hfq"]
        #     quote_obj = StockDailyQuote.objects(code=row["code"], date=i).first()
        #     for field in field_list:
        #         quote_obj[field] = row[field]
        #     quote_obj.save()

        # upsert freshness meta
        last_factor_date = df.iloc[-1]['date']
        trading_day_helper.update_freshness_meta(self.stock_obj, self.factor_name, last_factor_date)


if __name__ == '__main__':
    from app.lib.db_tool import mongoengine_tool
    mongoengine_tool.connect_to_db()
    stock_obj = BasicStock.objects(code="sh601166").first()
    obj = FQFactorProcessor(stock_obj)
    obj.perform_factor_calc()
