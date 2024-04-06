import logging
from app.model.signal import SignalData
from app.model.trade_opportunity import TradeOpportunity
from app.lib.opportunity_seeker.processors import TradingOpportunityProcessor


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

  def prepare_input(self):
      logger.info(f'Reading signal data for {self.stock_obj.code} - {self.stock_obj.name}')
      
      pass

  def perform_calc(self):
     pass
  
  def prepare_bulk_insert_list(self):
     pass