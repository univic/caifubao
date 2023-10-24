import logging
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
    def __init__(self, module_name, processor_registry):
        self.module_name = module_name
        self.processor_registry = processor_registry
        self.processor_list = []
        self.scenario = None
        self.strategy = None
        self.counter_dict = {
            'FINI': 0,
            'SKIP': 0,
            'ERR': 0,
        }
        self.stock_list = []
        self.exec_unit_list = []
        logger.info(f'{self.module_name} is initializing')

    def before_exec(self):
        pass

    def get_todo_list(self):
        """
        get a list which contains what should be processed,
        :return:
        """
        pass

    def generate_exec_plan(self):
        """
        for each stock, iterate through all the processors and determine whether to exec compute/analysis
        if so, assemble an exec unit and append it to wait list
        :return:
        """
        for stock in self.stock_list:
            analyte_list = self.get_analyte_list()
            for item in analyte_list:
                go_exec = self.check_last_analysis_date(stock, item)
                if go_exec:
                    exec_unit = GeneralExecUnit()
                    exec_unit.target_stock = stock
                    exec_unit.process_type = self.module_name
                    exec_unit.analyte = item
                    exec_unit.processor = self.processor_registry.registry[item]['processor']
                    exec_unit.handler_func = self.processor_registry.registry[item]['handler_func']
                    self.exec_unit_list.append(exec_unit)
                else:
                    pass

        # Check metadata and determine whether to run the processor

    def commit_tasks(self):
        logger.info(f'{self.module_name} - running exec units')
        for exec_unit in self.exec_unit_list:
            stock = exec_unit.target_stock
            processor = exec_unit.processor
            processor_name = general_utils.get_class_name(processor)
            logger.info(f'{self.module_name} - processor {exec_unit.processor} - target {stock.code}/{stock.name} - analyte {exec_unit.analyte}')
            if exec_unit.kwargs:
                kwargs = exec_unit.kwargs
                # TODO: TO BE DONE BELOW
                processor_instance = processor(exec_unit)
                process_handler_func = getattr(processor_instance, exec_unit.handler_func)
                exec_result_dict = process_handler_func()
                result_flag = exec_result_dict["flag"]
                self.counter_dict[result_flag] += 1
                logger.info(
                    f'{self.module_name} processor {processor_name} exec result: {result_flag} {exec_result_dict["msg"]}')

    def after_exec(self):
        pass

    def run(self):
        self.before_exec()
        self.generate_exec_plan()
        self.commit_tasks()
        logger.info(f'{self.module_name} processors run finished, '
                    f'{self.counter_dict["FINI"]} finished, '
                    f'{self.counter_dict["SKIP"]} skipped.')
        self.after_exec()

    def get_analyte_list(self):
        analyte_list = []
        return analyte_list

    def check_last_analysis_date(self, target_stock, item_name):
        latest_quote_date = trading_day_helper.read_freshness_meta(target_stock, 'daily_quote')
        for processor_name in self.processor_list:
            exec_flag = True
            # if analysis had never happened, or analysis date is behind quote date, run the processor
            latest_analysis_date = freshness_meta_helper.read_freshness_meta(target_stock, name=processor_name)
            if latest_analysis_date and latest_quote_date <= self.latest_analysis_date:
                self.counter_dict['SKIP'] += 1
                exec_flag = False
                logger.info(f'{self.module_name} processor {processor_name} skipped')
            return exec_flag


class GeneralProcessor(object):
    """
    Base class for all the processors
    input: an exec unit
    output: a result dict, contains result code and msg
    """

    def __init__(self, exec_unit, *args, **kwargs):
        # self.stock = exec_unit.stock
        # self.processor = exec_unit.processor
        # self.processor_name = general_utils.get_class_name(processor)
        # self.processor_type = exec_unit.processor_type
        # self.most_recent_processor_unit_date = None
        # # self.latest_process_date = latest_process_date
        self.data_df = None
        self.exec_result_dict: dict = {
            "flag": "FINI",
            "msg": ""
        }

    def run(self):
        self.before_exec()
        self.exec()
        self.perform_db_upsert()
        self.update_freshness_meta()
        self.after_exec()
        return self.exec_result_dict

    def before_exec(self):
        pass

    def exec(self):
        # Customizing here
        pass

    def after_exec(self):
        pass

    def get_source_data(self):
        pass

    def update_freshness_meta(self):
        latest_date = max(self.data_df.index)
        freshness_meta_helper.upsert_freshness_meta(self.stock, self.processor_name,
                                                    self.processor_type, latest_date)

    def perform_db_upsert(self):
        pass

