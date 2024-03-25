import logging
import itertools
from app.lib import GeneralWorker
from app.lib.opportunity_seeker.processors import registry


logger = logging.getLogger(__name__)


class OpportunitySeeker(GeneralWorker):
    def __init__(self, strategy_director, portfolio_manager, scenario):
        super().__init__(strategy_director, portfolio_manager, scenario)
        meta_type = 'opportunity'
        self.processor_registry = registry

    def get_todo(self):
        stock_list = self.strategy_director.get_stock_list()
        opportunity_scanner_list = self.strategy_director.get_opportunity_scanner_list()
        self.todo_list = itertools.product(stock_list, opportunity_scanner_list)

    def exec_todo(self):
        for todo_item in self.todo_list:
            self.stock_obj = todo_item[0]
            opportunity_scanner_name = todo_item[1]
            processor_dict = self.processor_registry[opportunity_scanner_name]
            self.run_processor(processor_dict)