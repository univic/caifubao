from app.utilities import freshness_meta_helper


class SignalProcessor(object):
    """
    Base class for all the signal processors
    """

    def __init__(self, stock, signal_name, latest_signal_date, *args, **kwargs):
        self.stock = stock
        self.signal_name = signal_name
        self.most_recent_signal_date = None
        self.latest_signal_date = latest_signal_date
        self.latest_factor_date = None
        self.factor_df = None
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
        self.latest_factor_date = max(self.factor_df.index)
        freshness_meta_helper.upsert_freshness_meta(self.stock, self.signal_name,
                                                    'signal', self.latest_factor_date)

    def perform_db_upsert(self):
        pass

