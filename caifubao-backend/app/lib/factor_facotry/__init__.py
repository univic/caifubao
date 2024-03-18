import logging
import itertools
import pandas as pd
from app.model.stock import BasicStock, StockDailyQuote
from app.lib.factor_facotry import processors
from app.lib import GeneralWorker
from app.lib.factor_facotry.processors import factor_processor_registry
from app.utilities import trading_day_helper, freshness_meta_helper

logger = logging.getLogger(__name__)


class FactorFactory(GeneralWorker):
    def __init__(self, strategy_director, portfolio_manager, scenario):

        # get class name
        super().__init__(strategy_director, portfolio_manager, scenario)
        self.quote_df = None
        self.processor_registry = factor_processor_registry
        self.factor_processor_list: list = []
        self.factor_processor_exec_list: list = []
        self.counter_dict = {
            'TODO': 0,
            'FINI': 0,
            'SKIP': 0,
            'ERR': 0,
        }

    def before_run(self):
        self.backtest_name = self.scenario.backtest_name
        self.processor_instance = processors.factor_processor_registry

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
        self.before_run()
        self.get_todo()
        prev_stock_code = None
        for todo_item in self.todo_list:
            self.stock_obj = todo_item[0]
            # if previous stock code is different from current one, reload the quote data
            if prev_stock_code != self.stock_obj.code:
                self.read_quote_data()
            prev_stock_code = self.stock_obj.code

            factor_name = todo_item[1]
            processor_dict = self.processor_registry[factor_name]

            self.processor_instance = self.get_processor_instance(factor_name)
            process_handler_func = getattr(self.processor_instance, processor_dict['handler'])
            logger.info(f'Doing factor analysis {factor_name} for {self.stock_obj.code} - {self.stock_obj.name}')
            process_handler_func()

        logger.info(f'Factor generation complete, '
                    f'{self.counter_dict["FINI"]} finished, '
                    f'{self.counter_dict["SKIP"]} skipped.')

    def get_processor_instance(self, factor_name):
        logger.info(f'Looking for {factor_name} factor processor for {self.stock_obj.code} - {self.stock_obj.name}')
        processor_dict = processors.factor_processor_registry[factor_name]
        processor_object = processor_dict['processor_object']
        kwargs = {}
        if 'kwargs' in processors.factor_processor_registry[factor_name].keys():
            kwargs = processors.factor_processor_registry[factor_name]['kwargs']
        processor_instance = processor_object(stock_obj=self.stock_obj,
                                              scenario=self.scenario,
                                              input_df=self.quote_df,
                                              processor_dict=processor_dict, **kwargs)
        return processor_instance

    def read_quote_data(self):
        logger.info(f'Reading quote df for {self.stock_obj.code} - {self.stock_obj.name}')
        # field_exclude_list = ['volume', 'trade_amount']
        field_exclude_list = []
        # if stock code remain unchanged, do not load quote df again

        # quote_qs = StockDailyQuote.objects(code=self.stock_obj.code,
        #                                    date__gt=self.current_day) \
        #     .exclude(*field_exclude_list) \
        #     .order_by('+date')
        quote_qs = StockDailyQuote.objects(code=self.stock_obj.code) \
            .exclude(*field_exclude_list) \
            .order_by('+date')
        # convert to df
        quote_json = quote_qs.as_pymongo()
        self.quote_df = pd.DataFrame(quote_json)
        self.quote_df.set_index("date", inplace=True)

        # most_recent_factor_date = freshness_meta_helper.read_freshness_meta(code=self.stock_obj.code,
        #                                                                     meta_type='factor',
        #                                                                     name='fq_factor')
        # if most_recent_factor_date:
        #     quote_qs = StockDailyQuote.objects(code=self.stock.code,
        #                                        date__gt=most_recent_factor_date) \
        #         .exclude(*field_exclude_list) \
        #         .order_by('+date')
        # else:
        #     quote_qs = StockDailyQuote.objects(code=self.stock.code) \
        #         .exclude(*field_exclude_list) \
        #         .order_by('+date')
        # # convert to df
        # quote_json = quote_qs.as_pymongo()
        # self.quote_df = pd.DataFrame(quote_json)
        # self.quote_df.set_index("date", inplace=True)
        # self.latest_quote_date = self.quote_df.index[-1]

    def check_metadata(self, stock_code, factor_name):
        skip_flag = False
        if self.scenario.is_backtest:
            latest_quote_date = freshness_meta_helper.read_freshness_meta(code=stock_code,
                                                                          object_type=self.stock_obj.object_type,
                                                                          meta_type='quote',
                                                                          meta_name='daily_quote')
        else:
            latest_quote_date = self.scenario.current_datetime
        logger.info(f'Metadata for {self.stock_obj.code} - {self.stock_obj.name} - daily_quote : '
                    f'{latest_quote_date} ')
        latest_factor_date = freshness_meta_helper.read_freshness_meta(code=stock_code,
                                                                       object_type=self.stock_obj.object_type,
                                                                       meta_type='factor',
                                                                       meta_name=factor_name,
                                                                       backtest_name=self.backtest_name)
        logger.info(f'Metadata for {self.stock_obj.code} - {self.stock_obj.name} - {factor_name} : '
                    f'{latest_factor_date} ')
        if not latest_factor_date or latest_quote_date > latest_factor_date:
            self.counter_dict['TODO'] += 1
        else:
            self.counter_dict['SKIP'] += 1
            skip_flag = True
        return skip_flag

    # def run_processor(self, stock_obj):
    #     # if new stock object is different from previous, then update quote df
    #     if stock_obj != self.stock_obj:
    #         self.stock_obj = stock_obj
    #         self.read_quote_data(stock_obj)

    # def run_processors(self):
    #     logger.info(f'Running factor processors for {self.stock.code} - {self.stock.name}')
    #     for factor_name in self.factor_processor_exec_list:
    #         logger.info(f'Running factor processor {factor_name}')
    #         processor_object = processors.factor_processor_registry[factor_name]['processor_object']
    #         kwargs = {}
    #         if 'kwargs' in processors.factor_processor_registry[factor_name].keys():
    #             kwargs = processors.factor_processor_registry[factor_name]['kwargs']
    #         processor_instance = processor_object(self.stock, self.quote_df, self.latest_factor_date, **kwargs)
    #         process_handler_func = getattr(processor_instance, processors.factor_processor_registry[factor_name]['handler'])
    #         process_handler_func()


if __name__ == '__main__':
    pass
