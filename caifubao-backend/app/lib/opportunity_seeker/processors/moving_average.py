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


    def scan_trade_opportunities(self):
        pass

    def perform_db_upsert(self):
        pass
