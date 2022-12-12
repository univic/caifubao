from app.lib import GeneralProcessor


class TradingOpportunityProcessor(GeneralProcessor):

    def __init__(self, stock, processor_unit_name, latest_process_date, *args, **kwargs):
        super().__init__(stock, processor_unit_name, latest_process_date, *args, **kwargs)
        pass


