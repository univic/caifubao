import logging
from pymongo import UpdateOne
from app.model.stock import StockDailyQuote, IndividualStock
from app.lib.factor_factory.processors.factor_processor import FactorProcessor

logger = logging.getLogger(__name__)


class FQFactorProcessor(FactorProcessor):
    def __init__(
        self, stock_obj, scenario, processor_dict, input_df, meta_obj, *args, **kwargs
    ):
        super().__init__(stock_obj, scenario, processor_dict, input_df, meta_obj)
        self.meta_name = "FQ_FACTOR"
        self.process_df = self.input_df

    def get_latest_entry_date(self):
        entry = (
            StockDailyQuote.objects(code=self.stock_obj.code, fq_factor__exists=True)
            .order_by("-date")
            .first()
        )
        if entry:
            self.latest_entry_date = entry.date

    def check_prerequisite(self):
        super().check_prerequisite()

        if not isinstance(self.stock_obj, IndividualStock):
            self.set_exec_result_state(
                "SKIP",
                f"{self.meta_name} calculation for {self.stock_obj.code}-{self.stock_obj.name} "
                f"kipped due to not an instance of IndividualStock",
            )
            return self.exec_result_dict

    def perform_calc(self):
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
        self.process_df["fq_factor"] = (
            self.process_df["close"] / self.process_df["previous_close"]
        ).cumprod()
        self.process_df["close_hfq"] = (
            self.process_df["fq_factor"] * self.input_df.iloc[0]["previous_close"]
        ).round(decimals=4)
        self.process_df["open_hfq"] = (
            self.process_df["open"]
            * (self.process_df["close_hfq"] / self.process_df["close"])
        ).round(decimals=4)
        self.process_df["high_hfq"] = (
            self.process_df["high"]
            * (self.process_df["close_hfq"] / self.process_df["close"])
        ).round(decimals=4)
        self.process_df["low_hfq"] = (
            self.process_df["low"]
            * (self.process_df["close_hfq"] / self.process_df["close"])
        ).round(decimals=4)

    def perform_db_upsert(self):
        bulk_operations = []
        # update database
        for i, row in self.output_df.iterrows():
            filter_query = {"code": row["code"], "date": i}
            update_query = {
                "$set": {
                    "fq_factor": row["fq_factor"],
                    "close_hfq": row["close_hfq"],
                    "open_hfq": row["open_hfq"],
                    "high_hfq": row["high_hfq"],
                    "low_hfq": row["low_hfq"],
                }
            }
            bulk_operations.append(UpdateOne(filter_query, update_query, upsert=True))
            # quote_obj = StockDailyQuote.objects(code=row["code"], date=i).first()
            # for field in field_list:
            #     quote_obj[field] = row[field]
            #     quote_obj.save()

        if bulk_operations:
            StockDailyQuote._get_collection().bulk_write(bulk_operations)
        self.freshness_meta_update_flag = True


# if __name__ == '__main__':
#     from app.lib.db_tool import mongoengine_tool
#     mongoengine_tool.connect_to_db()
#     stock_obj = BasicStock.objects(code="sh601166").first()
#     obj = FQFactorProcessor(stock_obj)
#     obj.perform_calc()
