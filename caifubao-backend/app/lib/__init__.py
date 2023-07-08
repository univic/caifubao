import logging
from app.utilities import trading_day_helper, freshness_meta_helper


logger = logging.getLogger()


class GeneralExecUnit(object):
    target_stock = None  # target stock
    process_type = None  # the type of the work, e.g. factor, signal
    processor = None     # which processor to use
    compute_item = None          # item being processed by the processor


class GeneralWorker(object):
    def __init__(self, scenario, strategy, module_name, meta_type, processor_registry):
        self.module_name = module_name
        self.meta_type = meta_type
        self.processor_registry = processor_registry
        self.scenario = scenario
        self.strategy = strategy
        self.counter_dict = {
            'FINI': 0,
            'SKIP': 0,
            'ERR': 0,
        }
        self.stock_list = []
        self.exec_unit_list = []
        logger.info(f'Initializing {self.module_name}')

    def run(self):
        self.before_exec()
        self.exec()
        self.after_exec()

    def before_exec(self):
        pass

    def exec(self):
        self.generate_exec_plan()
        self.run_processors()
        logger.info(f'{self.module_name} processors run finished for {self.stock.name}, '
                    f'{self.counter_dict["FINI"]} finished, '
                    f'{self.counter_dict["SKIP"]} skipped.')

    def after_exec(self):
        pass

    def get_target_list(self):
        pass

    def get_compute_item_list(self):
        pass

    def check_last_analysis_date(self, target_stock, item_name):
        latest_quote_date = trading_day_helper.read_freshness_meta(target_stock, 'daily_quote')
        for processor_name in self.processor_list:
            exec_flag = True
            # if analysis has never happend, or analysis date is behind quote date, run the processor
            latest_analysis_date = freshness_meta_helper.read_freshness_meta(target_stock, name=processor_name)
            if latest_analysis_date and latest_quote_date <= self.latest_analysis_date:
                self.counter_dict['SKIP'] += 1
                exec_flag = False
                logger.info(f'{self.module_name} processor {processor_name} skipped')
            return exec_flag

    def generate_exec_plan(self):
        """
        for each stock, iterate through all the processors and determine whether to exec compute/analysis
        if so, assemble a exec unit and append it to wait list
        :return:
        """
        for stock in self.stock_list:
            compute_item_list = self.get_compute_item_list()
            for item in compute_item_list:
                go_exec = self.check_last_analysis_date(stock, item)
                if go_exec:
                    exec_unit = GeneralExecUnit()
                    self.exec_unit_list.append(exec_unit)
                else:
                    pass

        # Check meta data and determine whether to run the processor

    def go_exec(self):
        pass


    def run_processors(self):
        logger.info(f'Running {self.module_name} processors for {self.stock.code} - {self.stock.name}')
        for processor_name in self.processor_exec_list:
            logger.info(f'Running {self.module_name} processor {processor_name}')
            processor_object = self.processor_registry.registry[processor_name]['processor_object']
            if 'kwargs' in self.processor_registry.registry[processor_name].keys():
                kwargs = self.processor_registry.registry[processor_name]['kwargs']
                processor_instance = processor_object(self.stock, processor_name, self.latest_analysis_date, **kwargs)
                process_handler_func = getattr(processor_instance, self.processor_registry.registry[processor_name]['handler'])
                exec_result_dict = process_handler_func()
                result_flag = exec_result_dict["flag"]
                self.counter_dict[result_flag] += 1
                logger.info(f'{self.module_name} processor {processor_name} exec result: {result_flag} {exec_result_dict["msg"]}')


class GeneralProcessor(object):
    """
    Base class for all the processors
    """

    def __init__(self, stock, processor_name, latest_process_date, *args, **kwargs):
        self.stock = stock
        self.processor_name = processor_name
        self.processor_type = None
        self.most_recent_processor_unit_date = None
        self.latest_process_date = latest_process_date
        self.data_df = None
        self.exec_result_dict = {
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

    def update_freshness_meta(self):
        latest_date = max(self.data_df.index)
        freshness_meta_helper.upsert_freshness_meta(self.stock, self.processor_name,
                                                    self.processor_type, latest_date)

    def perform_db_upsert(self):
        pass

