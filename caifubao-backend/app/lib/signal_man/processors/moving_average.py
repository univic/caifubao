import logging
import pandas as pd
from app.utilities import freshness_meta_helper
from app.model.stock import StockDailyQuote
from app.model.factor import FactorDataEntry
from app.model.signal import SpotSignalData, ContinuousSignalData
from app.lib.signal_man.processors.signal_processor import SignalProcessor

logger = logging.getLogger()


class MACrossSignalProcessor(SignalProcessor):
    def __init__(self, stock, signal_name, latest_process_date, *args, **kwargs):
        super().__init__(stock, signal_name, latest_process_date)
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

        self.data_df = pd.merge(pri_ma_factor_df, ref_ma_factor_df, how="outer", left_index=True, right_index=True)

    def generate_signal(self, *args, **kwargs):
        self.data_df['pri_above_ref'] = self.data_df[self.pri_ma] > self.data_df[self.ref_ma]
        self.data_df['pri_cross_ref'] = self.data_df['pri_above_ref'].diff()
        # drop NA lines, otherwise the and operation will fail
        self.data_df.dropna(inplace=True)
        self.data_df['pri_up_cross_ref'] = (self.data_df['pri_above_ref'] & self.data_df['pri_cross_ref'])
        if self.latest_process_date:
            self.signal_df = self.data_df[(self.data_df['pri_up_cross_ref']), self.data_df.index > self.latest_process_date]
        else:
            self.signal_df = self.data_df[(self.data_df['pri_up_cross_ref'])]
        for i, row in self.signal_df.iterrows():
            signal_data = SpotSignalData()
            signal_data.name = self.processor_name
            signal_data.stock_code = self.stock.code
            signal_data.date = i
            self.bulk_insert_list.append(signal_data)

    def perform_db_upsert(self):
        SpotSignalData.objects.insert(self.bulk_insert_list, load_bulk=False)


class PriceMARelationProcessor(SignalProcessor):
    """
    Generate price-MA relation signal, indicate whether current HFQ price is above/below specified MA line.
    """
    def __init__(self, stock, processor_name, latest_process_date, *args, **kwargs):
        super().__init__(stock, processor_name, latest_process_date, *args, **kwargs)
        self.backtest_overall_anaylsis = False
        self.pri_ma = kwargs['PRI_MA']
        self.relation_type = kwargs['RELATION_TYPE']

    def read_factor_data(self):
        logger.info(f'Reading factor data for {self.stock.code} - {self.stock.name}')
        # queryset
        pri_ma_factor_qs = FactorDataEntry.objects(stock_code=self.stock.code, name=self.pri_ma)
        fields = ['date', 'close_hfq']
        hfq_price_qs = StockDailyQuote.objects(code=self.stock.code).only(*fields)
        # convert queryset to json
        pri_ma_factor_query_json = pri_ma_factor_qs.as_pymongo()
        hfq_price_query_json = hfq_price_qs.as_pymongo()
        # convert json to df
        pri_ma_factor_df = pd.DataFrame(pri_ma_factor_query_json)
        hfq_price_factor_df = pd.DataFrame(hfq_price_query_json)
        # set index
        pri_ma_factor_df.set_index("date", inplace=True)
        hfq_price_factor_df.set_index("date", inplace=True)
        # rename column
        pri_ma_factor_df.rename(columns={"value": self.pri_ma}, inplace=True)
        # remove abundant columns
        pri_ma_factor_df.drop(['_id', 'name', 'stock_code'], axis=1, inplace=True)
        hfq_price_factor_df.drop(['_id'], axis=1, inplace=True)

        self.data_df = pd.merge(pri_ma_factor_df, hfq_price_factor_df, how="outer", left_index=True, right_index=True)

    def generate_signal(self):
        # calculate the intersection point of price and MA lines
        self.data_df['price_above_ma'] = self.data_df['close_hfq'] > self.data_df[self.pri_ma]
        self.data_df['price_above_ma'].replace({True: 1, False: 0})
        self.data_df['price_ma_cross'] = self.data_df['price_above_ma'].diff()
        self.signal_df = self.data_df[self.data_df['price_ma_cross'] != 0]
        if self.relation_type == 'ABOVE':
            start_flag = 1
            end_flag = -1
        else:
            start_flag = -1
            end_flag = 1
        signal_stack = []
        for i, row in self.signal_df.iterrows():
            if row['price_ma_cross'] == start_flag and not signal_stack:
                signal_data = ContinuousSignalData()
                signal_data.name = self.processor_name
                signal_data.stock_code = self.stock.code
                signal_data.date = i
                signal_stack.append(signal_data)
                self.bulk_insert_list.append(signal_data)
            elif row['price_ma_cross'] == end_flag and signal_stack:
                self.bulk_insert_list[-1].expire_date = i
                signal_stack.pop()
            else:
                pass

    def perform_db_upsert(self):
        ContinuousSignalData.objects.insert(self.bulk_insert_list, load_bulk=False)


if __name__ == '__main__':
    pass
