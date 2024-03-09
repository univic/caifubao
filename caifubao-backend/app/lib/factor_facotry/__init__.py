import logging
import itertools
import pandas as pd
from app.model.stock import BasicStock, StockDailyQuote
from app.lib.factor_facotry import processors
from app.lib import GeneralWorker
from app.utilities import trading_day_helper, freshness_meta_helper

logger = logging.getLogger(__name__)


class FactorFactory(GeneralWorker):
    def __init__(self, strategy_director, portfolio_manager, scenario):

        # get class name
        super().__init__(strategy_director, portfolio_manager, scenario)
        self.stock_df = None
        self.factor_processor_list:list = []
        self.factor_processor_exec_list:list = []
        self.counter_dict = {
            'TODO': 0,
            'FINI': 0,
            'SKIP': 0,
            'ERR': 0,
        }

    def get_todo(self):
        stock_list = self.strategy_director.get_stock_list()
        factor_list: list = self.strategy_director.get_factor_list()
        factor_rule_list = self.strategy_director.get_factor_rule_list()
        if len(factor_rule_list) == 1 and factor_rule_list[0] == "*":
            # Obtain the Cartesian product of two lists
            self.todo_list = itertools.product(stock_list, factor_list)
        else:
            logger.error("Unsupported factor rule")

    def run(self):
        self.get_todo()
        for todo_item in self.todo_list:
            skip_flag = self.check_metadata(todo_item)
            if not skip_flag:
                stock_obj = BasicStock.objects(code=todo_item[0]).first()
                self.run_processor(stock_obj)

        logger.info(f'Factor generation complete, '
                    f'{self.counter_dict["FINI"]} finished, '
                    f'{self.counter_dict["SKIP"]} skipped.')

    def check_metadata(self, todo_item):
        skip_flag = False
        stock_code = todo_item[0]
        factor_name = todo_item[1]
        latest_quote_date = freshness_meta_helper.read_freshness_meta(stock_code, 'quote', 'daily_quote')
        latest_factor_date = freshness_meta_helper.read_freshness_meta(stock_code, 'factor', factor_name)
        if not latest_factor_date or latest_quote_date > latest_factor_date:
            self.counter_dict['TODO'] += 1
        else:
            self.counter_dict['SKIP'] += 1
            skip_flag = True
        return skip_flag

    def run_processor(self, stock_obj):
        # if new stock object is different from previous, then update quote df
        if stock_obj != self.stock_obj:
            self.stock_obj = stock_obj
            self.read_quote_data(stock_obj)
        logger.info(f'Running factor processor {} for {self.stock_obj.code} - {self.stock_obj.name}')
        #TODO: finish this

    def read_quote_data(self, stock_obj):
        if not self.quote_df:
            logger.info(f'Reading quote df for {stock_obj.code} - {stock_obj.name}')
            # field_exclude_list = ['volume', 'trade_amount']
            field_exclude_list = []
            most_recent_factor_date = freshness_meta_helper.read_freshness_meta(stock_code=self.stock.code,
                                                                                meta_type='factor',
                                                                                name='fq_factor')
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
    pass
