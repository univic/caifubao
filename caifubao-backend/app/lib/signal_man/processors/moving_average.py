import logging
import pandas as pd
from app.utilities import freshness_meta_helper
from app.model.factor import FactorDataEntry
from app.lib.signal_man.processors import SignalProcessor

logger = logging.getLogger()


class MACrossSignalProcessor(SignalProcessor):
    def __init__(self, stock, signal_name, *args, **kwargs):
        super().__init__(stock, signal_name)
        self.backtest_overall_anaylsis = True
        self.pri_ma = kwargs['PRI_MA']
        self.ref_ma = kwargs['REF_MA']
        self.cross_type = kwargs['CROSS_TYPE']
        self.latest_analysis_date = None

    def read_factor_data(self):
        logger.info(f'Reading factor data for {self.stock.code} - {self.stock.name}')
        # queryset
        pri_ma_factor_qs = FactorDataEntry.objects(stock_code=self.stock.code, name=self.pri_ma)
        ref_ma_factor_qs = FactorDataEntry.objects(stock_code=self.stock.code, name=self.ref_ma)
        # convert queryset to json
        pri_ma_factor_query_json = pri_ma_factor_qs.as_pymongo()
        ref_ma_factor_query_json = ref_ma_factor_qs.as_pymongo()
        # convert json to df
        pri_ma_factor_df = pd.DataFrame(pri_ma_factor_query_json)
        ref_ma_factor_df = pd.DataFrame(ref_ma_factor_query_json)
        self.latest_analysis_date = pri_ma_factor_df.index[-1]

    def update_freshness_meta(self):
        freshness_meta_helper.upsert_freshness_meta(self.stock, self.signal_name,
                                                    'signal_analysis', self.latest_analysis_date)

