import logging
import itertools
from app.lib import GeneralWorker
from app.lib.signal_man.processors import registry


logger = logging.getLogger(__name__)


class SignalMan(GeneralWorker):
    def __init__(self, strategy_director, portfolio_manager, scenario):
        super().__init__(strategy_director, portfolio_manager, scenario)
        # get class name
        # self.module_name = self.__class__.__name__
        self.meta_type = 'signal'
        self.processor_registry = registry

    def get_todo(self):
        stock_list = self.strategy_director.get_stock_list()
        signal_list = self.strategy_director.get_signal_list()
        self.todo_list = itertools.product(stock_list, signal_list)

    def exec_todo(self):
        for todo_item in self.todo_list:
            self.stock_obj = todo_item[0]
            signal_name = todo_item[1]
            processor_dict = self.processor_registry[signal_name]
            self.run_processor(processor_dict)


if __name__ == "__main__":
    pass
