import logging
from app.lib import GeneralFactory
from app.lib.signal_man import processors


logger = logging.getLogger()


class SignalMan(GeneralFactory):
    def __init__(self, stock, processor_name_list):
        module_name = 'SignalMan'
        meta_type = 'signal_processor'
        processor_registry = processors.registry
        super().__init__(stock, module_name, meta_type, processor_registry, processor_name_list)

