import logging
import pandas as pd
from app.model.factor import FactorDataEntry
from app.lib.signal_man import processors
from app.utilities import trading_day_helper, freshness_meta_helper


logger = logging.getLogger()


class SignalMan(object):

    def __init__(self, stock, signal_name_list):
        logger.info('Initializing SignalMan')
        self.stock = stock
        self.signal_name_list = signal_name_list
        self.signal_processor_list = []
        self.signal_processor_exec_list = []
        self.latest_quote_date = None
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
        self.latest_quote_date = trading_day_helper.read_freshness_meta(self.stock, 'daily_quote')
        for signal_name in self.signal_name_list:
            latest_signal_analysis_date = freshness_meta_helper.read_freshness_meta(self.stock, name=signal_name)
            if not latest_signal_analysis_date or self.latest_quote_date > latest_signal_analysis_date:
                self.signal_processor_exec_list.append(signal_name)
                self.counter_dict['FINI'] += 1
            else:
                self.counter_dict['SKIP'] += 1

    def run_processors(self):
        logger.info(f'Running signal processors for {self.stock.code} - {self.stock.name}')
        for signal_name in self.signal_processor_exec_list:
            logger.info(f'Running factor processor {signal_name}')
            processor_object = processors.factor_registry[signal_name]['processor_object']
            kwargs = {}
            if 'kwargs' in processors.factor_registry[signal_name].keys():
                kwargs = processors.factor_registry[signal_name]['kwargs']
            processor_instance = processor_object(self.stock, **kwargs)
            process_handler_func = getattr(processor_instance, processors.factor_registry[signal_name]['handler'])
            process_handler_func()

