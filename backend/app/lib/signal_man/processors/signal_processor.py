import logging
from app.lib import GeneralProcessor


logger = logging.getLogger(__name__)


class SignalProcessor(GeneralProcessor):
    """
    Base class for all the factor scenario_processors
    """

    def __init__(
        self, stock_obj, scenario, processor_dict, input_df, meta_obj, *args, **kwargs
    ):
        super().__init__(
            stock_obj, scenario, processor_dict, input_df, meta_obj, *args, **kwargs
        )
        self.meta_type = "signal"
