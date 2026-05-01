import logging
import itertools


import pandas as pd
from app.utilities.progress_bar import progress_bar
from app.model.stock import StockDailyQuote
from app.model.data_freshness import DataFreshnessMeta
from app.lib.factor_factory import processors
from app.lib import GeneralWorker
from app.lib.factor_factory.processors import factor_processor_registry
from app.utilities import freshness_meta_helper
from app.utilities.exceptions import PrerequisiteCheckException

logger = logging.getLogger(__name__)


class FactorFactory(GeneralWorker):
    def __init__(self, strategy_director, portfolio_manager, scenario):

        # get class name
        super().__init__(strategy_director, portfolio_manager, scenario)
        self.processor_registry = factor_processor_registry

    def before_run(self):
        self.backtest_name = self.scenario.backtest_name
        self.processor_instance = processors.factor_processor_registry

    def get_todo(self):
        logger.info(f"{self.module_name} - Calculating todo list")
        stock_list = self.strategy_director.get_stock_list()
        stock_code_list = [stock.code for stock in stock_list]
        factor_list: list = self.strategy_director.get_factor_list()
        # factor_dict = {factor: factor_processor_registry[factor] for factor in factor_list}

        # Obtain the Cartesian product of two lists
        self.todo_list = []
        todo_list = list(itertools.product(stock_list, factor_list))
        # get data freshness meta for all the stocks
        freshness_meta_list = list(
            DataFreshnessMeta.objects(
                code__in=stock_code_list, meta_type="factor", meta_name__in=factor_list
            )
        )
        self.freshness_meta_dict = {
            (meta.code, meta.meta_name): meta for meta in freshness_meta_list
        }
        # filter the items that is already up-to-date and inactive
        for todo_item in todo_list:
            stock_obj = todo_item[0]
            factor_name = todo_item[1]
            search_tuple = (stock_obj.code, factor_name)
            meta = self.freshness_meta_dict.get(search_tuple)
            todo_tuple = (stock_obj, factor_name, meta)
            todo_factor_registry = self.processor_registry[factor_name]
            scope_of_calc = getattr(todo_factor_registry, "scope_of_calc", None)
            # check execution scope, no need to calculate fq factors for stock index
            if not scope_of_calc or self.stock_obj.__class__ in scope_of_calc:
                # if already up-to-date, then skip
                if meta:
                    result_code = freshness_meta_helper.check_factor_freshness(
                        stock_obj=stock_obj, meta_obj=meta, scenario=self.scenario
                    )
                    self.counter_dict[result_code] += 1

                    if result_code == "UPD":
                        self.todo_list.append(todo_tuple)
                else:
                    self.todo_list.append(todo_tuple)
            else:
                self.counter_dict["SKIP"] += 1

                # if (search_tuple in freshness_dict and
                #         freshness_dict[search_tuple] == self.scenario.current_datetime_prev_complete_trading_day):
                #     self.counter_dict['SKIP'] += 1
                # else:
                #     self.todo_list.append(todo_tuple)
        msg_str = "FactorFactory todo item count: "
        for key, value in self.counter_dict.items():
            msg_str = msg_str + key + ":" + str(value) + ","
        logger.info(f"FactorFactory todo item count: {msg_str}")

    def exec_todo(self):
        prev_stock_code = None
        prev_factor_name = None
        prog_bar = progress_bar()
        for i, todo_item in enumerate(self.todo_list):
            # TODO: need prog bar here
            is_continue = True
            self.stock_obj = todo_item[0]
            # if previous stock code is different from current one, reload the quote data
            if (
                prev_stock_code != self.stock_obj.code
                or prev_factor_name == "FQ_FACTOR"
            ):
                self.prepare_input_df()
            search_tuple = (todo_item[0].code, todo_item[1])

            # do meta status check, if quote data no longer update, then set meta to NO_UPD
            self.meta_obj = self.freshness_meta_dict.get(search_tuple)
            if self.meta_obj:
                check_result = freshness_meta_helper.check_meta_status(
                    scenario=self.scenario,
                    quote_df=self.input_df,
                    meta_obj=self.meta_obj,
                )
                if check_result == "SKIP":
                    logger.info(
                        f"{self.module_name} - {self.stock_obj.code}-{self.stock_obj.name}-"
                        f"Skipping {todo_item[1]} meta status set to NO_UPD due to no new quote"
                    )
                    is_continue = False
            if is_continue:
                prev_stock_code = self.stock_obj.code
                prev_factor_name = todo_item[1]
                factor_name = todo_item[1]
                if not self.input_df.empty and factor_name in self.processor_registry:
                    processor_dict = self.processor_registry[factor_name]
                    msg = (
                        f"Running factor processor {factor_name} "
                        f"for {self.stock_obj.code}-{self.stock_obj.name}"
                    )
                    prog_bar(i, len(self.todo_list), msg)
                    try:
                        self.run_processor(processor_dict)
                    except PrerequisiteCheckException:
                        self.counter_dict["SKIP"] += 1
                        logger.info(
                            f"{self.stock_obj.code}-{self.stock_obj.name}-{factor_name} "
                            f"skipped due to prerequisite condition not met."
                        )
                elif self.input_df.empty:
                    logger.warning(
                        f"{self.stock_obj.code}-{self.stock_obj.name}-{self.processor_registry[factor_name]} "
                        f"aborted due to empty input_df."
                    )
                else:
                    logger.error(
                        f"{self.stock_obj.code}-{self.stock_obj.name}-{self.processor_registry[factor_name]} "
                        f"processor not found in registry"
                    )

            # except Exception as e:
            #     self.counter_dict['ERR'] += 1
            #     logger.error(f"{self.stock_obj.code}-{self.stock_obj.name}-{self.processor_registry[factor_name]} "
            #                  f"encountered exception {traceback.format_exception(e)}.")

        # logger.info(f'Factor generation complete, '
        #             f'{self.counter_dict["FINI"]} finished, '
        #             f'{self.counter_dict["SKIP"]} skipped.')

    def prepare_input_df(self):
        # logger.info(f'Reading quote df for {self.stock_obj.code} - {self.stock_obj.name}')
        # field_exclude_list = ['volume', 'trade_amount']
        field_exclude_list = []

        # quote_qs = StockDailyQuote.objects(code=self.stock_obj.code,
        #                                    date__gt=self.current_day) \
        #     .exclude(*field_exclude_list) \
        #     .order_by('+date')
        quote_qs = (
            StockDailyQuote.objects(code=self.stock_obj.code)
            .exclude(*field_exclude_list)
            .order_by("+date")
        )
        # convert to df
        quote_json = quote_qs.as_pymongo()
        self.input_df = pd.DataFrame(quote_json)
        if "date" in self.input_df.columns:
            self.input_df.set_index("date", inplace=True)
        else:
            logger.warning(
                f"{self.stock_obj.code}-{self.stock_obj.name}-"
                f"failed to set input dataframe index, input_df will be reset to empty"
            )
            self.input_df = pd.DataFrame()


if __name__ == "__main__":
    pass
