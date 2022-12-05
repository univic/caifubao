import logging
import pandas as pd
from app.model.trade_opportunity import TradeOpportunity
from app.lib.opportunity_seeker import processors
from app.utilities import trading_day_helper, freshness_meta_helper


logger = logging.getLogger()


class OpportunitySeeker(object):
    def __init__(self, stock, processor_name_list, quote_df=None):
        logger.info('Initializing OpportunitySeeker')
        self.stock = stock
        self.quote_df = quote_df
        self.latest_quote_date = None
        self.latest_analysis_date = None
        self.processor_name_list = processor_name_list
        self.processor_list = []
        self.processor_exec_list = []
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
        logger.info(f'Trade opportunity discovery completed for {self.stock.name}, '
                    f'{self.counter_dict["FINI"]} finished, '
                    f'{self.counter_dict["SKIP"]} skipped.')

    def after_exec(self):
        pass

    def generate_exec_plan(self):
        pass

    def run_processors(self):
        logger.info(f'Running opportunity seeker processors for {self.stock.code} - {self.stock.name}')