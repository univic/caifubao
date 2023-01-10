import logging
from app.utilities import trading_day_helper, freshness_meta_helper


logger = logging.getLogger()


class GeneralFactory(object):
    def __init__(self, stock, module_name, meta_type, processor_registry, processor_name_list, quote_df=None):
        self.stock = stock
        self.module_name = module_name
        self.meta_type = meta_type
        self.quote_df = quote_df
        self.latest_quote_date = None
        self.latest_analysis_date = None
        self.processor_name_list = processor_name_list
        self.processor_registry = processor_registry
        self.processor_list = []
        self.processor_exec_list = []
        self.counter_dict = {
            'FINI': 0,
            'SKIP': 0,
            'ERR': 0,
        }
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

    def generate_exec_plan(self):
        # Check meta data and determine whether to run the processor
        self.latest_quote_date = trading_day_helper.read_freshness_meta(self.stock, 'daily_quote')
        for processor_name in self.processor_name_list:
            exec_flag = True
            # if analysis has never happend, or analysis date is behind quote date, run the processor
            self.latest_analysis_date = freshness_meta_helper.read_freshness_meta(self.stock, name=processor_name)
            if self.latest_analysis_date and self.latest_quote_date <= self.latest_analysis_date:
                self.counter_dict['SKIP'] += 1
                exec_flag = False
                logger.info(f'{self.module_name} processor {processor_name} skipped')
            if exec_flag:
                self.processor_exec_list.append(processor_name)

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

