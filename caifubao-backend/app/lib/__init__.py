import logging
import traceback
from app.utilities import general_utils, trading_day_helper, freshness_meta_helper


logger = logging.getLogger(__name__)


class GeneralExecUnit(object):
    target_stock = None  # target stock
    process_type = None  # the type of the work, e.g. factor, signal
    processor = None     # which processor to use
    handler_func = None  # which function of the processor to call
    analyte = None          # item being processed by the processor
    args: list = None    # arguments when running the analysis
    kwargs: dict = None  # keyword arguments when running the analysis


class GeneralWorker(object):
    def __init__(self, strategy_director, portfolio_manager, scenario):
        self.module_name = self.__class__.__name__
        self.processor_registry = None
        self.strategy_director = strategy_director
        self.portfolio_manager = portfolio_manager
        self.scenario = scenario
        self.strategy = None
        self.current_day = None
        self.backtest_name: str = ""
        self.todo_list: list = []
        self.input_df = None
        self.counter_dict = {
            'FINI': 0,
            'SKIP': 0,
            'ERR': 0,
        }
        self.stock_obj = None
        self.processor_instance = None
        # self.exec_unit_list = []
        logger.info(f'{self.module_name} is initializing')

    def before_run(self):
        pass

    def get_todo(self):
        """
        get a list which contains what should be processed,
        :return:
        """
        pass

    def exec_todo(self):
        pass

    # def generate_exec_plan(self):
    #     """
    #     for each stock, iterate through all the processors and determine whether to exec compute/analysis
    #     if so, assemble an exec unit and append it to wait list
    #     :return:
    #     """
    #     for stock in self.stock_list:
    #         analyte_list = self.get_analyte_list()
    #         for item in analyte_list:
    #             go_exec = self.check_last_analysis_date(stock, item)
    #             if go_exec:
    #                 exec_unit = GeneralExecUnit()
    #                 exec_unit.target_stock = stock
    #                 exec_unit.process_type = self.module_name
    #                 exec_unit.analyte = item
    #                 exec_unit.processor = self.processor_registry.registry[item]['processor']
    #                 exec_unit.handler_func = self.processor_registry.registry[item]['handler_func']
    #                 self.exec_unit_list.append(exec_unit)
    #             else:
    #                 pass
    #
    #     # Check metadata and determine whether to run the processor
    #
    # def commit_tasks(self):
    #     logger.info(f'{self.module_name} - running exec units')
    #     for exec_unit in self.exec_unit_list:
    #         stock = exec_unit.target_stock
    #         processor = exec_unit.processor
    #         processor_name = general_utils.get_class_name(processor)
    #         logger.info(f'{self.module_name} - processor {exec_unit.processor} - target {stock.code}/{stock.name} - analyte {exec_unit.analyte}')
    #         if exec_unit.kwargs:
    #             kwargs = exec_unit.kwargs
    #             processor_instance = processor(exec_unit)
    #             process_handler_func = getattr(processor_instance, exec_unit.handler_func)
    #             exec_result_dict = process_handler_func()
    #             result_flag = exec_result_dict["flag"]
    #             self.counter_dict[result_flag] += 1
    #             logger.info(
    #                 f'{self.module_name} processor {processor_name} exec result: {result_flag} {exec_result_dict["msg"]}')

    def after_run(self):
        pass

    def run(self):
        self.before_run()
        self.get_todo()
        self.exec_todo()

    def get_processor_instance(self, processor_name):
        # logger.info(f'Looking for {processor_name} processor for {self.stock_obj.code} - {self.stock_obj.name}')
        processor_dict = self.processor_registry[processor_name]
        processor_object = processor_dict['processor_object']
        kwargs = {}
        if 'kwargs' in self.processor_registry[processor_name].keys():
            kwargs = self.processor_registry[processor_name]['kwargs']
        processor_instance = processor_object(stock_obj=self.stock_obj,
                                              scenario=self.scenario,
                                              input_df=self.input_df,
                                              processor_dict=processor_dict, **kwargs)
        return processor_instance

    def run_processor(self, processor_dict):
        processor_name = processor_dict["name"]
        self.processor_instance = self.get_processor_instance(processor_name)
        process_handler_func = getattr(self.processor_instance, processor_dict['handler'])
        logger.info(f'Doing factor analysis {processor_name} for {self.stock_obj.code} - {self.stock_obj.name}')
        process_handler_func()

        # self.before_run()
        # self.generate_exec_plan()
        # self.commit_tasks()
        # logger.info(f'{self.module_name} processors run finished, '
        #             f'{self.counter_dict["FINI"]} finished, '
        #             f'{self.counter_dict["SKIP"]} skipped.')

    # def check_last_analysis_date(self, target_stock, item_name):
    #     latest_quote_date = trading_day_helper.read_freshness_meta(target_stock, 'daily_quote')
    #     for processor_name in self.processor_list:
    #         exec_flag = True
    #         # if analysis had never happened, or analysis date is behind quote date, run the processor
    #         latest_analysis_date = freshness_meta_helper.read_freshness_meta(target_stock, name=processor_name)
    #         if latest_analysis_date and latest_quote_date <= self.latest_analysis_date:
    #             self.counter_dict['SKIP'] += 1
    #             exec_flag = False
    #             logger.info(f'{self.module_name} processor {processor_name} skipped')
    #         return exec_flag


class GeneralProcessor(object):
    """
    Base class for all the processors
    input: an exec unit
    output: a result dict, contains result code and msg
    """

    def __init__(self, stock_obj, scenario, processor_dict, input_df, *args, **kwargs):
        self.stock_obj = stock_obj
        # self.processor = exec_unit.processor
        # self.processor_name = general_utils.get_class_name(processor)
        # self.processor_type = exec_unit.processor_type
        # self.most_recent_processor_unit_date = None
        self.most_recent_existing_data_dt = None
        self.processor_dict: dict = processor_dict

        self.meta_type = None
        self.meta_name = None
        self.scenario = scenario
        self.backtest_name: str = ""
        self.input_df = input_df
        self.process_df = input_df

        # self.most_recent_existing_data_dt = None
        # self.output_data_start_dt = None

        self.output_df = None
        self.output_field_list: list = []
        self.db_document_object = None
        self.bulk_insert_list: list = []
        self.exec_result_dict: dict = {
            "code": "FINI",
            "msg": ""
        }

    def run(self):
        # prepare the input
        self.before_exec()
        self.determine_exec_range()
        if self.exec_result_dict["code"] != "SKIP":
            self.perform_calc()
            # get output_df that ready for db insert
            self.prepare_output()
            self.prepare_bulk_insert_list()
            self.perform_db_upsert()
            self.update_freshness_meta()
        else:
            logger.debug(f"{self.stock_obj.code}-{self.stock_obj.name} {self.meta_type}-{self.meta_name}"
                         f" skipped due to already up to date")
        self.after_exec()

    def before_exec(self):
        pass

    def exec(self):
        # Customizing here
        self.perform_calc()

    def perform_calc(self):
        pass

    def determine_exec_range(self):
        # if overall analysis is not enabled, check latest process date by backtest name
        backtest_name = None
        if not self.processor_dict['backtest_overall_anaylsis']:
            self.backtest_name = self.scenario.backtest_name
        self.most_recent_existing_data_dt = freshness_meta_helper.read_freshness_meta(code=self.stock_obj.code,
                                                                                      object_type=self.stock_obj.object_type,
                                                                                      meta_type=self.meta_type,
                                                                                      meta_name=self.processor_dict['name'],
                                                                                      backtest_name=self.backtest_name)

        # if no metadata was founded, do complete analysis
        if not self.most_recent_existing_data_dt:
            self.process_df = self.input_df

        # if metadata time is behind current time, do partial analysis
        elif self.most_recent_existing_data_dt < self.scenario.current_datetime:
            head_index = (self.input_df.index.get_loc(self.most_recent_existing_data_dt) -
                          self.processor_dict['partial_process_offset'])
            self.process_df = self.input_df.iloc[head_index:][:]

        elif self.most_recent_existing_data_dt == self.scenario.current_datetime:
            # if metadata has same datetime, skip
            self.set_exec_result_state('SKIP', 'skipped due to nothing to update')

        else:
            logger.error(f'Unidentified processing circumstance: most recent meta datetime greater than current time')

    def prepare_output(self):
        if self.most_recent_existing_data_dt:
            head_index = self.process_df.index.get_loc(self.most_recent_existing_data_dt)
            self.output_df = self.process_df.iloc[head_index:][:]
            logger.debug(f"{self.stock_obj.code}-{self.stock_obj.name} {self.meta_type}-{self.meta_name}"
                         f" sliced output df from {self.most_recent_existing_data_dt}")
        else:
            self.output_df = self.process_df

    def prepare_bulk_insert_list(self):
        pass

    def perform_db_upsert(self):
        try:
            # try bulk insert
            self.db_document_object.objects.insert(self.bulk_insert_list, load_bulk=False)
        # update database
        except KeyError as e:
            logger.warning(f"Key error when trying to bulk insert. traceback info: {traceback.format_exception(e)}")
        finally:
            pass

    def update_freshness_meta(self):
        latest_date = max(self.output_df.index)
        freshness_meta_helper.upsert_freshness_meta(code=self.stock_obj.code,
                                                    object_type=self.stock_obj.object_type,
                                                    meta_type=self.meta_type,
                                                    meta_name=self.meta_name,
                                                    dt=latest_date,
                                                    backtest_name=self.backtest_name)

    def after_exec(self):
        pass

    def set_exec_result_state(self, code, msg):
        self.exec_result_dict['code'] = code     # FINI | SKIP | FAIL
        self.exec_result_dict['msg'] = msg
