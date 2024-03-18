import logging
from app.lib import GeneralProcessor
from app.utilities import freshness_meta_helper


logger = logging.getLogger(__name__)


class FactorProcessor(GeneralProcessor):
    """
    Base class for all the factor scenario_processors
    """

    def __init__(self, stock_obj, scenario, processor_dict, input_df, *args, **kwargs):
        super().__init__(stock_obj, scenario, processor_dict, input_df, *args, **kwargs)
        self.meta_type = 'factor'

    def determine_exec_range(self):
        # if overall analysis is not enabled, check latest process date by backtest name
        backtest_name = None
        if not self.processor_dict['backtest_overall_anaylsis']:
            self.backtest_name = self.scenario.backtest_name
        self.most_recent_process_datetime = freshness_meta_helper.read_freshness_meta(code=self.stock_obj.code,
                                                                                      object_type=self.stock_obj.object_type,
                                                                                      meta_type=self.meta_type,
                                                                                      meta_name=self.processor_dict['name'],
                                                                                      backtest_name=self.backtest_name)

        # if no metadata was founded, do complete analysis
        if not self.most_recent_process_datetime:
            self.process_df = self.input_df

        # if metadata time is behind current time, do partial analysis
        elif self.most_recent_process_datetime < self.scenario.current_datetime:
            head_index = (self.input_df.index.get_loc(self.most_recent_process_datetime) -
                          self.processor_dict['partial_process_offset'])
            self.process_df = self.input_df.iloc[head_index:][:]

        elif self.most_recent_process_datetime == self.scenario.current_datetime:
            # if metadata has same datetime, skip
            self.set_exec_result_state('SKIP', 'skipped due to nothing to update')

        else:
            logger.error('Unidentified processing circumstance')


