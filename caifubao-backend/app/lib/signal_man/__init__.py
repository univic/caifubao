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
        self.latest_signal_date = None
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
            exec_flag = True
            # if signal analysis has never happend, or analysis date is behind quote date, run the processor
            self.latest_signal_date = freshness_meta_helper.read_freshness_meta(self.stock, name=signal_name)
            if self.latest_signal_date and self.latest_quote_date <= self.latest_signal_date:
                self.counter_dict['SKIP'] += 1
                exec_flag = False
                logger.info(f'Signal processor {signal_name} skipped')
            if exec_flag:
                self.signal_processor_exec_list.append(signal_name)

    def run_processors(self):
        logger.info(f'Running signal processors for {self.stock.code} - {self.stock.name}')
        for signal_name in self.signal_processor_exec_list:
            logger.info(f'Running signal processor {signal_name}')
            processor_object = processors.factor_registry[signal_name]['processor_object']
            kwargs = {}
            if 'kwargs' in processors.factor_registry[signal_name].keys():
                kwargs = processors.factor_registry[signal_name]['kwargs']
            processor_instance = processor_object(self.stock, signal_name, self.latest_signal_date, **kwargs)
            process_handler_func = getattr(processor_instance, processors.factor_registry[signal_name]['handler'])
            exec_result_dict = process_handler_func()
            result_flag = exec_result_dict["flag"]
            self.counter_dict[result_flag] += 1
            logger.info(f'Signal processor {signal_name} exec result: {result_flag} {exec_result_dict["msg"]}')

