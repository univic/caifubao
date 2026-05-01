import logging
import pandas_ta as ta
from app.utilities.exceptions import PrerequisiteCheckException
from app.model.stock import IndividualStock
from app.model.factor import FactorDataEntry
from app.lib.factor_factory.processors.factor_processor import FactorProcessor

logger = logging.getLogger(__name__)


class MovingAverageFactorProcessor(FactorProcessor):
    def __init__(
        self, stock_obj, scenario, processor_dict, input_df, meta_obj, *args, **kwargs
    ):
        super().__init__(stock_obj, scenario, processor_dict, input_df, meta_obj)
        self.ma_days = kwargs["MA"]
        self.meta_name = f"MA_{self.ma_days}"
        self.factor_name = f"MA_{self.ma_days}"
        self.db_document_object = FactorDataEntry
        self.factor_object = FactorDataEntry

    def check_prerequisite(self):
        super().check_prerequisite()
        if len(self.input_df) <= self.ma_days:
            raise PrerequisiteCheckException

    def get_latest_entry_date(self):
        entry = (
            FactorDataEntry.objects(
                stock_code=self.stock_obj.code, name=self.factor_name
            )
            .order_by("-date")
            .first()
        )
        if entry:
            self.latest_entry_date = entry.date

    def perform_calc(self):
        if isinstance(self.stock_obj, IndividualStock):
            price_field = "close_hfq"
        else:
            price_field = "close"
        self.process_df[self.factor_name] = ta.sma(
            self.process_df[price_field], length=self.ma_days
        )
        self.process_df = self.process_df[self.process_df[self.factor_name].notna()]
        # if self.latest_factor_date:
        #     self.output_df = self.output_df[self.output_df.index > self.latest_factor_date]

    def prepare_bulk_insert_list(self):
        for i, row in self.output_df.iterrows():
            factor_data = self.db_document_object()
            factor_data.name = self.factor_name
            factor_data.stock = self.stock_obj
            factor_data.stock_name = self.stock_obj.name
            factor_data.stock_code = self.stock_obj.code
            factor_data.value = row[self.factor_name]
            factor_data.date = i
            self.bulk_insert_list.append(factor_data)

    # def perform_db_upsert(self):
    #     FactorDataEntry.objects.insert(self.bulk_insert_list, load_bulk=False)

    # def read_existing_factors(self):
    #     pass
