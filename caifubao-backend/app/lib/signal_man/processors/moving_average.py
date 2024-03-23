import logging
import pandas as pd
from app.utilities import freshness_meta_helper
from app.model.factor import FactorDataEntry
from app.model.signal import SpotSignalData
from app.lib.signal_man.processors.signal_processor import SignalProcessor

logger = logging.getLogger(__name__)


class MACrossSignalProcessor(SignalProcessor):
    def __init__(self, stock_obj, scenario, processor_dict, input_df, *args, **kwargs):
        super().__init__(stock_obj, scenario, processor_dict, input_df, *args, **kwargs)
        self.meta_name = processor_dict["name"]
        self.pri_ma = kwargs['PRI_MA']
        self.ref_ma = kwargs['REF_MA']
        self.cross_type = kwargs['CROSS_TYPE']

    def exec(self):
        self.read_factor_data()
        self.generate_signal()

    def read_factor_data(self):
        logger.info(f'Reading factor data for {self.stock_obj.code} - {self.stock_obj.name}')
        # queryset
        pri_ma_factor_qs = FactorDataEntry.objects(stock_code=self.stock_obj.code, name=self.pri_ma)
        ref_ma_factor_qs = FactorDataEntry.objects(stock_code=self.stock_obj.code, name=self.ref_ma)
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
        pri_ma_factor_df.drop(['_id', 'name', 'code'], axis=1, inplace=True)
        ref_ma_factor_df.drop(['_id', 'name', 'code'], axis=1, inplace=True)

        self.data_df = pd.merge(pri_ma_factor_df, ref_ma_factor_df, how="outer", left_index=True, right_index=True)

    def generate_signal(self, *args, **kwargs):
        self.data_df['pri_above_ref'] = self.data_df[self.pri_ma] > self.data_df[self.ref_ma]
        self.data_df['pri_cross_ref'] = self.data_df['pri_above_ref'].diff()
        # drop NA lines, otherwise the and operation will fail
        self.data_df.dropna(inplace=True)
        self.data_df['pri_up_cross_ref'] = (self.data_df['pri_above_ref'] & self.data_df['pri_cross_ref'])

    def perform_db_upsert(self):
        bulk_insert_list = []
        if self.most_recent_existing_data_dt:
            signal_df = self.data_df[(self.data_df['pri_up_cross_ref']), self.data_df.index > self.most_recent_existing_data_dt]
        else:
            signal_df = self.data_df[(self.data_df['pri_up_cross_ref'])]
        for i, row in signal_df.iterrows():
            signal_data = SpotSignalData()
            signal_data.name = self.most_recent_existing_data_dt
            signal_data.stock_code = self.stock.code
            signal_data.date = i
            bulk_insert_list.append(signal_data)
        SpotSignalData.objects.insert(bulk_insert_list, load_bulk=False)
        pass


class PriceMARelationProcessor(GeneralProcessor):
    """
    Generate price-MA relation signal, indicate whether current HFQ price is above/below specified MA line.
    """
    def __init__(self, stock, processor_name, latest_process_date, *args, **kwargs):
        super().__init__(stock, processor_name, latest_process_date, *args, **kwargs)