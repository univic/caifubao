from app.lib import GeneralProcessor
from app.utilities import freshness_meta_helper


class FactorProcessor(GeneralProcessor):
    """
    Base class for all the factor scenario_processors
    """

    def __init__(self, stock_obj, scenario, processor_dict, input_df, *args, **kwargs):
        super().__init__(stock_obj, scenario, processor_dict, input_df, *args, **kwargs)
        self.meta_type = 'factor'

    def determine_exec_mode(self):
        # if overall analysis is not enabled, check latest process date by backtest name
        backtest_name = None
        if not self.processor_dict['backtest_overall_anaylsis']:
            backtest_name = self.scenario.backtest_name
        self.latest_process_date = freshness_meta_helper.read_freshness_meta(stock_code=self.stock_obj.code,
                                                                             meta_type=self.meta_type,
                                                                             meta_name=self.processor_dict['name'],
                                                                             backtest_name=backtest_name)
