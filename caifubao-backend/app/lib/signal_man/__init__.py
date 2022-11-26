import logging
import pandas as pd
from app.model.factor import FactorDataEntry

from app.utilities import trading_day_helper, freshness_meta_helper


logger = logging.getLogger()


class SignalMan(object):

    def __init__(self, stock, signal_name_list):
        logger.info('Initializing SignalMan')
        self.stock = stock
        self.signal_name_list = signal_name_list
        self.signal_processor_list = []
        self.signal_processor_exec_list = []
        self.counter_dict = {
            'FINI': 0,
            'SKIP': 0,
            'ERR': 0,
        }

    def run(self):
        self.before_exec()
        self.exec()
        self.after_exec()

    def before_exec(self):
        pass

    def exec(self):
        self.generate_exec_plan()
        self.run_processors()
        logger.info(f'Signal generated for {self.stock.name}, '
                    f'{self.counter_dict["FINI"]} finished, '
                    f'{self.counter_dict["SKIP"]} skipped.')

    def after_exec(self):
        pass

    def generate_exec_plan(self):
        # TODO: how to determine exec plan
        # Check meta data and determine whether to run the processor
        for signal_name in self.signal_name_list:
            latest_signal_analysis_date = freshness_meta_helper.read_freshness_meta(self.stock, signal_name)
            if not latest_signal_analysis_date or self.latest_factor_date > latest_signal_analysis_date:
                self.signal_processor_exec_list.append(signal_name)
                self.counter_dict['FINI'] += 1
            else:
                self.counter_dict['SKIP'] += 1

    def run_processors(self):
        logger.info(f'Running signal processors for {self.stock.code} - {self.stock.name}')
