import logging
from app.lib import GeneralProcessor
from app.utilities import freshness_meta_helper


logger = logging.getLogger(__name__)


class SignalProcessor(GeneralProcessor):
    """
    Base class for all the factor scenario_processors
    """

    def __init__(self, stock_obj, scenario, processor_dict, input_df, *args, **kwargs):
        super().__init__(stock_obj, scenario, processor_dict, input_df, *args, **kwargs)
        self.meta_type = 'signal'

    def exec(self):
        self.perform_calc()

    def perform_calc(self):
        pass