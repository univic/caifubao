from app.lib import GeneralProcessor
from app.utilities import freshness_meta_helper


class FactorProcessor(GeneralProcessor):
    """
    Base class for all the factor scenario_processors
    """

    def __init__(self, stock_obj, scenario, input_df, *args, **kwargs):
        super().__init__(stock_obj, scenario, input_df, *args, **kwargs)
        self.meta_type = 'factor'
