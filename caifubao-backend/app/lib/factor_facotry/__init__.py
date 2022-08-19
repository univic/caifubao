import logging
import pandas as pd
from app.model.stock import BasicStock, StockDailyQuote
from app.lib.factor_facotry import processors
from app.utilities import trading_day_helper

logger = logging.getLogger()


class FactorFactory(object):
    def __init__(self, stock, factor_name_list, quote_df=None):
        logger.info('Initializing factor factory')
        self.stock = stock
        self.quote_df = quote_df
        self.factor_name_list = factor_name_list
        self.factor_processor_list = []

    def before_exec(self):
        pass

    def after_exec(self):
        pass

    def run_factor_factory(self):
        self.generate_exec_plan()
        self.read_quote_data()
        self.run_processors()

    def generate_exec_plan(self):
        pass

    def read_quote_data(self):
        if not self.quote_df:
            logger.info(f'Reading quote df for {self.stock.code} - {self.stock.name}')
            # field_exclude_list = ['volume', 'trade_amount']
            field_exclude_list = []
            most_recent_factor_date = trading_day_helper.read_freshness_meta(self.stock, 'fq_factor')
            if most_recent_factor_date:
                quote_qs = StockDailyQuote.objects(code=self.stock.code,
                                                   date__gt=most_recent_factor_date) \
                    .exclude(*field_exclude_list) \
                    .order_by('+date')

            else:
                quote_qs = StockDailyQuote.objects(code=self.stock.code) \
                    .exclude(*field_exclude_list) \
                    .order_by('+date')
            # convert to df
            quote_json = quote_qs.as_pymongo()
            self.quote_df = pd.DataFrame(quote_json)
            self.quote_df.set_index("date", inplace=True)

    def run_processors(self):
        logger.info(f'Running factor processors for {self.stock.code} - {self.stock.name}')
        for factor_name in self.factor_name_list:
            logger.info(f'Running factor processors {factor_name}')
            processor_object = processors.factor_registry[factor_name]['processor_object']
            kwargs = {}
            if 'kwargs' in processors.factor_registry[factor_name].keys():
                kwargs = processors.factor_registry[factor_name]['kwargs']
            processor_instance = processor_object(self.stock, self.quote_df, **kwargs)
            process_handler_func = getattr(processor_instance, processors.factor_registry[factor_name]['handler'])
            process_handler_func()


if __name__ == '__main__':
    from app.lib.db_tool import mongoengine_tool
    mongoengine_tool.connect_to_db()
    obj = FactorProcesser()
    obj.generate_factors("sh601166")
