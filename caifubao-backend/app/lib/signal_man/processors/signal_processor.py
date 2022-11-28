

class SignalProcessor(object):
    """
    Base class for all the signal processors
    """

    def __init__(self, stock, signal_name, *args, **kwargs):
        self.stock = stock
        self.signal_name = signal_name
        self.most_recent_signal_date = None
        self.exec_result_dict = {
            "flag": "FINI",
            "msg": ""
        }

    def run(self):
        self.before_exec()
        self.exec()
        self.after_exec()
        return self.exec_result_dict

    def before_exec(self):
        pass

    def exec(self):
        self.read_factor_data()
        self.generate_signal()
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

    def perform_db_upsert(self):
        pass

