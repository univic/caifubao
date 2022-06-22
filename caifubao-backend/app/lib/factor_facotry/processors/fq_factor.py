import datetime
import pandas as pd
import numpy as np

from app.utilities import freshness_meta_helper
from app.model.stock import StockDailyQuote
from app.lib.factor_facotry.processors import FactorProcessor


class FQFactorProcessor(FactorProcessor):

    def perform_factor_calc(self):
        most_recent_factor_date = freshness_meta_helper.read_freshness_meta(self.stock_obj.stock_obj, 'fq_factor')
        raw_df = self.stock_obj.quote_df
        # most_recent_factor_date = datetime.datetime(2022, 6, 20, 0, 0, 0)
        # if found existing factor, slice the df to reduce calculate work
        if most_recent_factor_date:
            # get previous quote data to make cumprod possible
            head_index = raw_df.index.get_loc(most_recent_factor_date) - 1
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
        for i, row in df.iterrows():
            field_list = ["fq_factor", "close_hfq", "open_hfq", "high_hfq", "low_hfq"]
            quote_obj = StockDailyQuote.objects(code=row["code"], date=i).first()
            for field in field_list:
                quote_obj[field] = row[field]
            quote_obj.save()

