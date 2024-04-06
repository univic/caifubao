import logging
import pandas as pd
from app.model.signal import SignalData
from app.model.trade_opportunity import TradeOpportunity
from app.lib.opportunity_seeker.processors.trading_opportunity_processor import TradingOpportunityProcessor

logger = logging.getLogger(__name__)


# class MALongCondition1(GeneralProcessor):
#     def __init__(self, stock, processor_name, latest_process_date, *args, **kwargs):
#         super().__init__(stock, processor_name, latest_process_date, *args, **kwargs)


class MAOpportunityProcessor(TradingOpportunityProcessor):
    """
  this processor will generate LONG trade opportunity when the following conditions were met:
    - Found MA10 up cross MA20 signal
    - MA120 is going upward
  """

    def __init__(self, stock_obj, scenario, processor_dict, input_df, *args, **kwargs):
        super().__init__(stock_obj, scenario, processor_dict, input_df, *args, **kwargs)
        self.meta_name = processor_dict["name"]
        self.db_document_object = TradeOpportunity
        self.include_signal = processor_dict["include_signal"]
        self.exclude_signal = processor_dict["exclude_signal"]

    def prepare_input(self):
        logger.info(f'Reading signal data for {self.stock_obj.code} - {self.stock_obj.name}')
        for signal in self.include_signal:
            query_set = SignalData.objects(stock=self.stock_obj, name=signal)
            query_json = query_set.as_pymongo()
            df = pd.DataFrame(query_json)
            df.set_index("date", inplace=True)
            if self.input_df.empty:
                self.input_df = df
            else:
                pass
                # add a column



        pass

    def perform_calc(self):
        pass

    def prepare_bulk_insert_list(self):
        pass
