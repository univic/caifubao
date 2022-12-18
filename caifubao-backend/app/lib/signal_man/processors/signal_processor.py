from app.lib import GeneralProcessor


class SignalProcessor(GeneralProcessor):
    def __init__(self, stock, processor_name, latest_process_date, *args, **kwargs):
        super().__init__(stock, processor_name, latest_process_date, *args, **kwargs)
        self.processor_type = 'Signal'
        self.signal_df = None
        self.bulk_insert_list = []

    def exec(self):
        self.read_factor_data()
        self.generate_signal()

    def read_factor_data(self):
        pass

    def generate_signal(self):
        pass
