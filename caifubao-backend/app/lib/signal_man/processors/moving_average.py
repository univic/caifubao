import logging
import pandas as pd
from app.utilities import freshness_meta_helper
from app.model.factor import FactorDataEntry
from app.model.signal import SignalData
from app.lib.signal_man.processors.signal_processor import SignalProcessor

logger = logging.getLogger(__name__)


class MACrossSignalProcessor(SignalProcessor):
    def __init__(self, stock_obj, scenario, processor_dict, input_df, *args, **kwargs):
        super().__init__(stock_obj, scenario, processor_dict, input_df, *args, **kwargs)
        self.meta_name = processor_dict["name"]
        self.pri_ma = kwargs['PRI_MA']
        self.ref_ma = kwargs['REF_MA']
        self.cross_type = kwargs['CROSS_TYPE']
        self.db_document_object = SignalData

    def prepare_input(self):
        logger.info(f'Reading factor data for {self.stock_obj.code} - {self.stock_obj.name}')
        # queryset
        pri_ma_factor_qs = FactorDataEntry.objects(stock=self.stock_obj, name=self.pri_ma)
        ref_ma_factor_qs = FactorDataEntry.objects(stock=self.stock_obj, name=self.ref_ma)
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
        pri_ma_factor_df.drop(['_id'], axis=1, inplace=True)
        ref_ma_factor_df.drop(['_id'], axis=1, inplace=True)

        self.input_df = pd.merge(pri_ma_factor_df, ref_ma_factor_df, how="outer", left_index=True, right_index=True)
        # self.input_df.set_index("date", inplace=True)
        self.process_df = self.input_df

    def perform_calc(self, *args, **kwargs):
        self.process_df['pri_above_ref'] = self.process_df[self.pri_ma] > self.process_df[self.ref_ma]
        self.process_df['pri_cross_ref'] = self.process_df['pri_above_ref'].diff()
        # drop NA lines, otherwise the AND operation will fail
        self.process_df.dropna(inplace=True)
        self.process_df['pri_up_cross_ref'] = (self.process_df['pri_above_ref'] & self.process_df['pri_cross_ref'])
        self.process_df = self.process_df[(self.process_df['pri_up_cross_ref'])]

    def prepare_bulk_insert_list(self):
        for i, row in self.output_df.iterrows():
            signal_data = self.db_document_object()
            signal_data.name = self.processor_dict["name"]
            signal_data.stock = self.stock_obj
            signal_data.stock_name = self.stock_obj.name
            signal_data.stock_code = self.stock_obj.code
            signal_data.date = i
            self.bulk_insert_list.append(signal_data)


class PriceMARelationProcessor(SignalProcessor):
    """
    Generate price-MA relation signal, indicate whether current HFQ price is above/below specified MA line.
    """
    def __init__(self, stock, processor_name, latest_process_date, *args, **kwargs):
        super().__init__(stock, processor_name, latest_process_date, *args, **kwargs)