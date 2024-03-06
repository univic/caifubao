import logging
from app.lib import GeneralWorker
from app.lib.signal_man import processors


logger = logging.getLogger(__name__)


class SignalMan(GeneralWorker):
    def __init__(self, strategy_director, portfolio_manager, scenario):

        self.module_name = 'SignalMan'
        self.meta_type = 'signal_processor'
        self.processor_registry = processors.registry
        super().__init__(strategy_director, portfolio_manager, scenario)


if __name__ == "__main__":
    signal_man = SignalMan()
    pass
