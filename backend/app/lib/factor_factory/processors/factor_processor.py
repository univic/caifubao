import logging
from app.lib import GeneralProcessor
from app.utilities import freshness_meta_helper
from app.utilities.logger_utils import event_logger


logger = logging.getLogger(__name__)


class FactorProcessor(GeneralProcessor):
    """
    Base class for all the factor scenario_processors
    """

    def __init__(
        self, stock_obj, scenario, processor_dict, input_df, meta_obj, *args, **kwargs
    ):
        super().__init__(
            stock_obj, scenario, processor_dict, input_df, meta_obj, *args, **kwargs
        )
        self.meta_type = "factor"
        self.entry_object = None

    def check_prerequisite(self):
        self.check_meta_alignment()

    def check_meta_alignment(self):
        self.get_latest_entry_date()
        meta_date = getattr(self.meta_obj, "freshness_datetime", None)
        if self.latest_entry_date and meta_date:
            # have both factor date and meta date, compare the alignment of the date
            if self.latest_entry_date != meta_date:
                logger.warning(
                    f"{self.stock_obj.code}-{self.stock_obj.name}-{self.meta_type}-{self.meta_name} "
                    f"meta data disagree, latest entry date {self.latest_entry_date}, meta date {meta_date}"
                )
                freshness_meta_helper.upsert_freshness_meta(
                    code=self.stock_obj.code,
                    object_type=self.stock_obj.object_type,
                    meta_type=self.meta_type,
                    meta_name=self.meta_name,
                    dt=self.latest_entry_date,
                    backtest_name=self.backtest_name,
                )
                logger.warning(
                    f"{self.stock_obj.code}-{self.stock_obj.name}-{self.meta_type}-{self.meta_name} "
                    f"meta date reset to latest factor date {self.latest_entry_date}"
                )
        elif self.latest_entry_date and not meta_date:
            event_logger.record_event(
                code=self.stock_obj.code,
                name=self.stock_obj.name,
                module_name="FactorProcessor",
                meta_type=self.meta_type,
                meta_name=self.meta_name,
                log_level="warning",
                message="meta data not found",
            )
            freshness_meta_helper.upsert_freshness_meta(
                code=self.stock_obj.code,
                object_type=self.stock_obj.object_type,
                meta_type=self.meta_type,
                meta_name=self.meta_name,
                dt=self.latest_entry_date,
                backtest_name=self.backtest_name,
            )
            event_logger.record_event(
                code=self.stock_obj.code,
                name=self.stock_obj.name,
                module_name="FactorProcessor",
                meta_type=self.meta_type,
                meta_name=self.meta_name,
                log_level="warning",
                message=f"rebuild meta data with date {self.latest_entry_date}",
            )
        elif not self.latest_entry_date and meta_date:
            logger.warning(
                f"{self.stock_obj.code}-{self.stock_obj.name}-{self.meta_type}-{self.meta_name} "
                f"meta data without underlying data."
            )
            freshness_meta_helper.remove_meta(
                code=self.stock_obj.code,
                meta_type=self.meta_type,
                meta_name=self.meta_name,
            )
            logger.warning(
                f"{self.stock_obj.code}-{self.stock_obj.name}-{self.meta_type}-{self.meta_name} "
                f"removed meta data with date {self.latest_entry_date}"
            )
        else:
            logger.debug(
                f"{self.stock_obj.code}-{self.stock_obj.name}-{self.meta_type}-{self.meta_name} "
                f"No meta and factor data was found."
            )

    def get_latest_entry_date(self):
        pass
