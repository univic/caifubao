from app.lib import GeneralProcessor
from app.model.trade_opportunity import TradeOpportunity


class MALongCondition1(GeneralProcessor):
    """
    this processor will generate LONG trade opportunity when the following conditions were met:
      - Found MA10 up cross MA20 signal
      - MA120 is going upward
    """
    def __init__(self, stock, processor_name, latest_process_date, *args, **kwargs):
        super().__init__(stock, processor_name, latest_process_date, *args, **kwargs)
