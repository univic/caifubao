
from app.utilities import freshness_meta_helper
from app.lib.signal_man.processors import moving_average


factor_registry = {
    "MA_120_UPCROSS_250": {
        'processor_object': moving_average.MACrossSignalProcessor,
        'handler': 'run',
        'kwargs': {
            'PRI_MA': 120,        # Primary MA line
            'REF_MA': 250,        # MA line for reference
            'CROSS_TYPE': 'UP',   # MA lines can up or down cross each other,
        },
        'factor_dependency': ['MA_120', 'MA_250'],
        'quote_dependent': False,
    }
}


class SignalProcessor(object):
    """
    Base class for all the signal processors
    """

    def __init__(self, stock, signal_name, *args, **kwargs):
        self.stock = stock
        self.signal_name = signal_name
        self.most_recent_signal_date = None

    def run(self):
        self.before_exec()
        self.exec()
        self.after_exec()

    def before_exec(self):
        pass

    def exec(self):
        self.perform_db_upsert()
        self.update_freshness_meta()

    def after_exec(self):
        pass

    def read_factor_data(self):
        pass

    def generate_signal(self, *args, **kwargs):
        pass

    def update_freshness_meta(self):
        pass

    def read_existing_factors(self):
        pass

    def perform_db_upsert(self):
        pass
