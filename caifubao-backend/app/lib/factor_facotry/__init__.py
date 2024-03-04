import logging
import pandas as pd
from app.model.stock import BasicStock, StockDailyQuote
from app.lib.factor_facotry import processors
from app.lib import GeneralWorker
from app.utilities import trading_day_helper, freshness_meta_helper

logger = logging.getLogger(__name__)


class FactorFactory(GeneralWorker):
    def __init__(self, strategy_director, scenario):

        # get class name
        super().__init__(scenario, strategy_director)
        self.factor_processor_list = []
        self.factor_processor_exec_list = []
        self.counter_dict = {
            'FINI': 0,
            'SKIP': 0,
            'ERR': 0,
        }

    def before_exec(self):
        pass

    def after_exec(self):
        pass

    def get_todo(self):
        stock_list = self.scenario

    def get_stock_list(self):
        self.stock_list = self.strategy_director.get_stock_list()

    def run_factor_factory(self):

        self.read_quote_data()
        self.generate_exec_plan()
        self.run_processors()
        logger.info(f'Factor generated for {self.stock.name}, '
                    f'{self.counter_dict["FINI"]} finished, '
                    f'{self.counter_dict["SKIP"]} skipped.')

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
        self.latest_quote_date = self.quote_df.index[-1]

    def generate_exec_plan(self):
        # Check metadata and determine whether to run the processor
        for factor_name in self.factor_name_list:
            self.latest_factor_date = freshness_meta_helper.read_freshness_meta(self.stock, factor_name)
            if not self.latest_factor_date or self.latest_quote_date > self.latest_factor_date:
                self.factor_processor_exec_list.append(factor_name)
                self.counter_dict['FINI'] += 1
            else:
                self.counter_dict['SKIP'] += 1

    def run_processors(self):
        logger.info(f'Running factor processors for {self.stock.code} - {self.stock.name}')
        for factor_name in self.factor_processor_exec_list:
            logger.info(f'Running factor processor {factor_name}')
            processor_object = processors.factor_registry[factor_name]['processor_object']
            kwargs = {}
            if 'kwargs' in processors.factor_registry[factor_name].keys():
                kwargs = processors.factor_registry[factor_name]['kwargs']
            processor_instance = processor_object(self.stock, self.quote_df, self.latest_factor_date, **kwargs)
            process_handler_func = getattr(processor_instance, processors.factor_registry[factor_name]['handler'])
            process_handler_func()


if __name__ == '__main__':
    # TODO: make it decoupled
    from app.lib.db_tool import mongoengine_tool
    mongoengine_tool.connect_to_db()
    obj = FactorFactory()
    obj.generate_factors("sh601166")
