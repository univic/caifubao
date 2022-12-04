import logging
import pandas as pd
from app.utilities import freshness_meta_helper
from app.model.factor import FactorDataEntry
from app.model.signal import SpotSignalData
from app.lib.signal_man.processors.signal_processor import SignalProcessor

logger = logging.getLogger()


class MACrossSignalProcessor(SignalProcessor):
    def __init__(self, stock, signal_name, latest_signal_date, *args, **kwargs):
        super().__init__(stock, signal_name, latest_signal_date)
        self.backtest_overall_anaylsis = True
        self.pri_ma = kwargs['PRI_MA']
        self.ref_ma = kwargs['REF_MA']
        self.cross_type = kwargs['CROSS_TYPE']

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
        # set index
        pri_ma_factor_df.set_index("date", inplace=True)
        ref_ma_factor_df.set_index("date", inplace=True)
        # rename column
        pri_ma_factor_df.rename(columns={"value": self.pri_ma}, inplace=True)
        ref_ma_factor_df.rename(columns={"value": self.ref_ma}, inplace=True)
        # remove abundant columns
        pri_ma_factor_df.drop(['_id', 'name', 'stock_code'], axis=1, inplace=True)
        ref_ma_factor_df.drop(['_id', 'name', 'stock_code'], axis=1, inplace=True)

        self.factor_df = pd.merge(pri_ma_factor_df, ref_ma_factor_df, how="outer", left_index=True, right_index=True)

        self.latest_factor_date = self.factor_df.index[-1]

    def generate_signal(self, *args, **kwargs):
        self.factor_df['pri_above_ref'] = self.factor_df[self.pri_ma] > self.factor_df[self.ref_ma]
        self.factor_df['pri_cross_ref'] = self.factor_df['pri_above_ref'].diff()
        # drop NA lines, otherwise the and operation will fail
        self.factor_df.dropna(inplace=True)
        self.factor_df['pri_up_cross_ref'] = (self.factor_df['pri_above_ref'] & self.factor_df['pri_cross_ref'])

    def perform_db_upsert(self):
        bulk_insert_list = []
        if self.latest_signal_date:
            signal_df = self.factor_df[(self.factor_df['pri_up_cross_ref']), self.factor_df.index > self.latest_signal_date]
        else:
            signal_df = self.factor_df[(self.factor_df['pri_up_cross_ref'])]
        for i, row in signal_df.iterrows():
            signal_data = SpotSignalData()
            signal_data.name = self.signal_name
            signal_data.stock_code = self.stock.code
            signal_data.date = i
            bulk_insert_list.append(signal_data)
        SpotSignalData.objects.insert(bulk_insert_list, load_bulk=False)
        pass
