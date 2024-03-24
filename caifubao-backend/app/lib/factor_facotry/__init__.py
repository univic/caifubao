import logging
import itertools
import pandas as pd
from app.model.stock import StockDailyQuote
from app.lib.factor_facotry import processors
from app.lib import GeneralWorker
from app.lib.factor_facotry.processors import factor_processor_registry
from app.utilities import freshness_meta_helper

logger = logging.getLogger(__name__)


class FactorFactory(GeneralWorker):
    def __init__(self, strategy_director, portfolio_manager, scenario):

        # get class name
        super().__init__(strategy_director, portfolio_manager, scenario)
        self.processor_registry = factor_processor_registry

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

    def exec_todo(self):
        prev_stock_code = None
        for todo_item in self.todo_list:
            # TODO: need prog bar here
            self.stock_obj = todo_item[0]
            # if previous stock code is different from current one, reload the quote data
            if prev_stock_code != self.stock_obj.code:
                self.prepare_input_df()
            prev_stock_code = self.stock_obj.code

            factor_name = todo_item[1]
            processor_dict = self.processor_registry[factor_name]
            self.run_processor(processor_dict)

        # logger.info(f'Factor generation complete, '
        #             f'{self.counter_dict["FINI"]} finished, '
        #             f'{self.counter_dict["SKIP"]} skipped.')

    def prepare_input_df(self):
        logger.info(f'Reading quote df for {self.stock_obj.code} - {self.stock_obj.name}')
        # field_exclude_list = ['volume', 'trade_amount']
        field_exclude_list = []

        # quote_qs = StockDailyQuote.objects(code=self.stock_obj.code,
        #                                    date__gt=self.current_day) \
        #     .exclude(*field_exclude_list) \
        #     .order_by('+date')
        quote_qs = StockDailyQuote.objects(code=self.stock_obj.code) \
            .exclude(*field_exclude_list) \
            .order_by('+date')
        # convert to df
        quote_json = quote_qs.as_pymongo()
        self.input_df = pd.DataFrame(quote_json)
        self.input_df.set_index("date", inplace=True)

    # def check_metadata(self, stock_code, factor_name):
    #     skip_flag = False
    #     if self.scenario.is_backtest:
    #         latest_quote_date = freshness_meta_helper.read_freshness_meta(code=stock_code,
    #                                                                       object_type=self.stock_obj.object_type,
    #                                                                       meta_type='quote',
    #                                                                       meta_name='daily_quote')
    #     else:
    #         latest_quote_date = self.scenario.current_datetime
    #     logger.info(f'Metadata for {self.stock_obj.code} - {self.stock_obj.name} - daily_quote : '
    #                 f'{latest_quote_date} ')
    #     latest_factor_date = freshness_meta_helper.read_freshness_meta(code=stock_code,
    #                                                                    object_type=self.stock_obj.object_type,
    #                                                                    meta_type='factor',
    #                                                                    meta_name=factor_name,
    #                                                                    backtest_name=self.backtest_name)
    #     logger.info(f'Metadata for {self.stock_obj.code} - {self.stock_obj.name} - {factor_name} : '
    #                 f'{latest_factor_date} ')
    #     if not latest_factor_date or latest_quote_date > latest_factor_date:
    #         self.counter_dict['TODO'] += 1
    #     else:
    #         self.counter_dict['SKIP'] += 1
    #         skip_flag = True
    #     return skip_flag

    # def run_processor(self, stock_obj):
    #     # if new stock object is different from previous, then update quote df
    #     if stock_obj != self.stock_obj:
    #         self.stock_obj = stock_obj
    #         self.prepare_input_df(stock_obj)

    # def run_processors(self):
    #     logger.info(f'Running factor processors for {self.stock.code} - {self.stock.name}')
    #     for processor_name in self.factor_processor_exec_list:
    #         logger.info(f'Running factor processor {processor_name}')
    #         processor_object = processors.factor_processor_registry[processor_name]['processor_object']
    #         kwargs = {}
    #         if 'kwargs' in processors.factor_processor_registry[processor_name].keys():
    #             kwargs = processors.factor_processor_registry[processor_name]['kwargs']
    #         processor_instance = processor_object(self.stock, self.quote_df, self.latest_factor_date, **kwargs)
    #         process_handler_func = getattr(processor_instance, processors.factor_processor_registry[processor_name]['handler'])
    #         process_handler_func()


if __name__ == '__main__':
    pass
