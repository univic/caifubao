import logging

import pandas as pd

from app.model.signal import SignalData
from app.model.trade_opportunity import TradeOpportunity
from app.lib.opportunity_seeker.processors.trading_opportunity_processor import TradingOpportunityProcessor


logger = logging.getLogger()


class LongTradeCondition1(TradingOpportunityProcessor):
    """
    this processor will generate LONG trade opportunity when the following conditions were met:
      - Found MA10 up cross MA20 signal
      - MA120 is going upward
    """
    def __init__(self, stock, processor_name, latest_process_date, *args, **kwargs):
        super().__init__(stock, processor_name, latest_process_date, *args, **kwargs)

    def read_signal_data(self):
        logger.info(f'Reading signal data for {self.stock.code} - {self.stock.name}')
        # queryset
        signal_qs_1 = SignalData.objects(stock_code=self.stock.code, name="MA_10_UPCROSS_20")
        signal_qs_2 = SignalData.objects(stock_code=self.stock.code, name="HFQ_PRICE_ABOVE_MA_120")
        # convert queryset to json
        signal_json_1 = signal_qs_1.as_pymongo()
        signal_json_2 = signal_qs_2.as_pymongo()
        # convert json to df
        signal_df_1 = pd.DataFrame(signal_json_1)
        signal_df_2 = pd.DataFrame(signal_json_2)
        # set index
        signal_df_1.set_index("date", inplace=True)
        signal_df_2.set_index("date", inplace=True)
        # add mark column
        signal_df_1["MA_10_UPCROSS_20"] = True
        signal_df_2["HFQ_PRICE_ABOVE_MA_120"] = False
        # remove abundant columns
        signal_df_1.drop(['_id', 'name', 'stock_code'], axis=1, inplace=True)
        signal_df_2.drop(['_id', 'name', 'stock_code'], axis=1, inplace=True)
        # merge df
        self.data_df = pd.merge(signal_df_1, signal_df_2, how="outer", left_index=True, right_index=True)

    def scan_trade_opportunities(self):
        self.data_df["match_condition"] = (self.data_df["MA_10_UPCROSS_20"] & self.data_df["HFQ_PRICE_ABOVE_MA_120"])
        opportunity_df = self.data_df[self.data_df["match_condition"] is True]
        for i, row in opportunity_df.iterrows():
            data = TradeOpportunity()
            data.date = i
            data.name = self.processor_name
            data.stock_code = self.stock.code
            data.direction = "LONG"
            self.bulk_insert_list.append(data)

    def perform_db_upsert(self):
        TradeOpportunity.objects.insert(self.bulk_insert_list, load_bulk=False)
