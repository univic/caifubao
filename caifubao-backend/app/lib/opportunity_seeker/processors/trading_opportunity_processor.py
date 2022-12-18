from app.lib import GeneralProcessor
from app.model.signal import SpotSignalData


class TradingOpportunityProcessor(GeneralProcessor):

    def __init__(self, stock, processor_name, latest_process_date, *args, **kwargs):
        super().__init__(stock, processor_name, latest_process_date, *args, **kwargs)
        self.processor_type = 'OpportunityScanner'
        pass

    def exec(self):
        self.read_signal_data()
        self.scan_trade_opportunities()

    def read_signal_data(self):
        pass

    def scan_trade_opportunities(self):
        pass


