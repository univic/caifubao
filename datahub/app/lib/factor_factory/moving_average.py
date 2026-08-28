from __future__ import annotations

import datetime
import logging
from collections.abc import Iterable

import pandas as pd
from pymongo import UpdateOne

from app.lib.utilities import data_asset_status_helper
from app.lib.utilities import data_capability_helper
from app.model.data_asset_status import STATUS_OK, DataAssetStatus
from app.model.factor import StockFactorDaily
from app.model.stock import IndividualStock, StockDailyQuote


logger = logging.getLogger(__name__)


class MovingAverageFactorService:
    DEFAULT_WINDOWS = (10, 20, 30, 60, 120)

    def __init__(
        self,
        quote_model=StockDailyQuote,
        stock_model=IndividualStock,
        factor_model=StockFactorDaily,
        windows: Iterable[int] | None = None,
    ):
        self.quote_model = quote_model
        self.stock_model = stock_model
        self.factor_model = factor_model
        self.windows = tuple(windows or self.DEFAULT_WINDOWS)

    @staticmethod
    def factor_name(window: int) -> str:
        return f"MA_{window}"

    @staticmethod
    def factor_field(window: int) -> str:
        return f"ma_{window}"

    @classmethod
    def build_ma_frame(
        cls,
        input_df: pd.DataFrame,
        windows: Iterable[int] | None = None,
        price_field: str = "close_hfq",
    ) -> pd.DataFrame:
        if input_df.empty:
            return input_df.copy()

        if price_field not in input_df.columns:
            raise ValueError(f"Missing price field for MA calculation: {price_field}")

        process_df = input_df.copy()
        process_df[price_field] = pd.to_numeric(
            process_df[price_field], errors="coerce"
        )
        if process_df[price_field].isna().all():
            raise ValueError(f"No valid {price_field} values for MA calculation")

        for window in windows or cls.DEFAULT_WINDOWS:
            factor_name = cls.factor_name(window)
            process_df[factor_name] = (
                process_df[price_field].rolling(window=window).mean().round(4)
            )
        return process_df

    def _load_latest_factor_meta_dates(self, code: str) -> dict[str, object]:
        quote_status = data_asset_status_helper.read_asset_status(
            code,
            "individual_stock",
            "quote",
            "daily_quote",
        )
        quote_count = int(getattr(quote_status, "data_count", 0) or 0)
        factor_names = [self.factor_name(window) for window in self.windows]
        status_list = list(
            DataAssetStatus.objects(
                code=code,
                object_type="individual_stock",
                asset_type="factor",
                asset_name__in=factor_names,
                status=STATUS_OK,
            ).only("asset_name", "latest_data_date")
        )
        status_map = {item.asset_name: item.latest_data_date for item in status_list}
        return {
            self.factor_name(window): status_map.get(self.factor_name(window))
            for window in self.windows
            if quote_count >= window
        }

    def _get_incremental_anchor_date(self, code: str):
        meta_dates = self._load_latest_factor_meta_dates(code)
        if not meta_dates:
            return None
        if any(value is None for value in meta_dates.values()):
            return None
        return min(meta_dates.values())

    def _load_quote_df(self, code: str, anchor_date=None) -> pd.DataFrame:
        query = self.quote_model.objects(code=code).only("code", "date", "close_hfq")

        if anchor_date is None:
            quote_rows = list(query.order_by("+date").as_pymongo())
        else:
            lookback_rows = list(
                query.filter(date__lte=anchor_date)
                .order_by("-date")[: max(self.windows) - 1]
                .as_pymongo()
            )
            new_rows = list(
                query.filter(date__gt=anchor_date).order_by("+date").as_pymongo()
            )
            quote_rows = list(reversed(lookback_rows)) + new_rows

        quote_df = pd.DataFrame(quote_rows)
        if not quote_df.empty and "date" in quote_df.columns:
            quote_df.set_index("date", inplace=True)
        return quote_df

    def _build_bulk_operations(
        self, stock_obj, output_df: pd.DataFrame, output_date_gt=None
    ) -> list[UpdateOne]:
        bulk_operations: list[UpdateOne] = []
        if output_date_gt is not None:
            output_df = output_df[output_df.index > output_date_gt]

        for quote_date, row in output_df.iterrows():
            factor_values = {}
            for window in self.windows:
                factor_name = self.factor_name(window)
                value = row.get(factor_name)
                if pd.isna(value):
                    continue
                factor_values[self.factor_field(window)] = float(value)

            if not factor_values:
                continue

            bulk_operations.append(
                UpdateOne(
                    {"stock_code": stock_obj.code, "date": quote_date},
                    {
                        "$set": {
                            **factor_values,
                            "stock": stock_obj.to_dbref(),
                            "stock_name": stock_obj.name,
                            "stock_code": stock_obj.code,
                            "category": "stock",
                            "date": quote_date,
                        }
                    },
                    upsert=True,
                )
            )
        return bulk_operations

    def _refresh_factor_statuses(self, stock_obj) -> None:
        quote_status = data_asset_status_helper.read_asset_status(
            stock_obj.code,
            stock_obj.object_type,
            "quote",
            "daily_quote",
        )
        for window in self.windows:
            data_asset_status_helper.refresh_ma_factor_status(
                stock_obj=stock_obj,
                factor_model=self.factor_model,
                window=window,
                quote_status=quote_status,
                last_job_name="ma_factor_sync",
            )

    def refresh_market_statuses(self, codes: list[str]) -> None:
        """Refresh MA window statuses for many codes with bulk round trips.

        Replaces one read + (aggregate + upsert) per code and window with one
        status read, one aggregate per window, and one chunked bulk upsert.
        Records are built by the same pure builder as the single-code path,
        so the resulting documents are identical to per-stock
        refresh_ma_factor_status calls. Must run after the factor writes of
        the batch have committed: statuses are derived from persisted rows.
        """
        if not codes:
            return
        stock_list = list(
            self.stock_model.objects(code__in=codes).only(
                "code", "name", "object_type", "data_capabilities"
            )
        )
        object_type_by_code = {
            stock.code: getattr(stock, "object_type", "individual_stock")
            for stock in stock_list
        }
        quote_status_map = data_asset_status_helper.read_quote_status_map(codes)
        calculated_at = datetime.datetime.now()
        records = []
        for window in self.windows:
            stats_by_code = data_asset_status_helper.aggregate_stats_by_code(
                self.factor_model._get_collection(),
                match={
                    "stock_code": {"$in": codes},
                    self.factor_field(window): {"$exists": True, "$ne": None},
                },
                code_field="stock_code",
            )
            for code in codes:
                if code not in object_type_by_code:
                    continue
                records.append(
                    data_asset_status_helper.build_ma_status_record(
                        code=code,
                        object_type=object_type_by_code[code],
                        stats_row=stats_by_code.get(code),
                        quote_status=quote_status_map.get(code),
                        window=window,
                        last_job_name="ma_factor_sync",
                        calculated_at=calculated_at,
                    )
                )
        data_asset_status_helper.bulk_upsert_asset_status(records)

    def update_code(
        self, code: str, *, refresh_statuses: bool = True
    ) -> dict[str, int | str | None]:
        stock_obj = (
            self.stock_model.objects(code=code)
            .only("code", "name", "object_type", "data_capabilities")
            .first()
        )
        if not stock_obj:
            return {"code": "FAIL", "written_count": 0, "message": "stock not found"}
        if not data_capability_helper.stock_supports(stock_obj, "ma_factor"):
            return {
                "code": "SKIP",
                "written_count": 0,
                "message": "ma_factor is not supported for this stock",
            }

        anchor_date = self._get_incremental_anchor_date(code)
        quote_df = self._load_quote_df(code, anchor_date=anchor_date)
        if quote_df.empty:
            if refresh_statuses:
                self._refresh_factor_statuses(stock_obj)
            return {"code": "GOOD", "written_count": 0, "message": None}

        try:
            output_df = self.build_ma_frame(
                quote_df, windows=self.windows, price_field="close_hfq"
            )
        except ValueError as exc:
            logger.warning("MA factor skipped: code=%s reason=%s", code, exc)
            return {"code": "SKIP", "written_count": 0, "message": str(exc)}

        bulk_operations = self._build_bulk_operations(
            stock_obj, output_df, output_date_gt=anchor_date
        )
        if bulk_operations:
            self.factor_model._get_collection().bulk_write(
                bulk_operations, ordered=False
            )
        if refresh_statuses:
            self._refresh_factor_statuses(stock_obj)

        logger.info(
            "MA factors updated: code=%s windows=%s written=%s",
            stock_obj.code,
            self.windows,
            len(bulk_operations),
        )
        return {
            "code": "GOOD",
            "written_count": len(bulk_operations),
            "message": None,
        }

    def get_codes_requiring_update(self, market=None) -> list[str]:
        stock_query = self.stock_model.objects(active_status=0)
        if market is not None:
            stock_query = stock_query.filter(market=market)

        stock_list = [
            stock
            for stock in stock_query.only("code", "data_capabilities")
            if data_capability_helper.stock_supports(stock, "ma_factor")
        ]
        if not stock_list:
            return []

        code_list = [stock.code for stock in stock_list]
        factor_names = [self.factor_name(window) for window in self.windows]
        quote_status_list = list(
            DataAssetStatus.objects(
                code__in=code_list,
                object_type="individual_stock",
                asset_type="quote",
                asset_name="daily_quote",
                status=STATUS_OK,
            ).only("code", "latest_data_date", "data_count")
        )
        ma_status_list = list(
            DataAssetStatus.objects(
                code__in=code_list,
                object_type="individual_stock",
                asset_type="factor",
                asset_name__in=factor_names,
            ).only("code", "asset_name", "latest_data_date", "status")
        )

        quote_status_map = {item.code: item for item in quote_status_list}
        ma_status_map = {
            (item.code, item.asset_name): (item.latest_data_date, item.status)
            for item in ma_status_list
        }

        result: list[str] = []
        for code in code_list:
            quote_status = quote_status_map.get(code)
            if not quote_status or not quote_status.latest_data_date:
                continue
            if any(
                ma_status_map.get((code, factor_name), (None, None))
                != (quote_status.latest_data_date, STATUS_OK)
                for factor_name in factor_names
                if int(quote_status.data_count or 0)
                >= int(factor_name.removeprefix("MA_"))
            ):
                result.append(code)
        return result

    def update_market(
        self, market=None, selected_codes: list[str] | None = None
    ) -> dict[str, int]:
        codes = (
            list(selected_codes)
            if selected_codes is not None
            else self.get_codes_requiring_update(market=market)
        )
        written_total = 0
        skipped_count = 0
        failed_count = 0
        failed_codes: list[str] = []
        refreshed_codes: list[str] = []
        for code in codes:
            try:
                result = self.update_code(code, refresh_statuses=False)
            except Exception:
                failed_count += 1
                failed_codes.append(code)
                logger.exception("MA factor update failed: code=%s", code)
                continue
            if result.get("code") == "SKIP":
                skipped_count += 1
            elif result.get("code") == "GOOD":
                refreshed_codes.append(code)
            else:
                failed_count += 1
                failed_codes.append(code)
                continue
            written_total += int(result.get("written_count", 0))
        # One batched freshness refresh for every updated code, after all
        # factor writes of this run have committed (see refresh_market_statuses).
        self.refresh_market_statuses(refreshed_codes)
        return {
            "pulled_count": len(codes),
            "written_count": written_total,
            "skipped_count": skipped_count,
            "failed_count": failed_count,
            "failed_codes": failed_codes,
        }

    def backfill_code(self, code: str) -> dict[str, int | str | None]:
        return self.update_code(code)

    def backfill_all(self, market=None) -> dict[str, int]:
        stock_query = self.stock_model.objects()
        if market is not None:
            stock_query = stock_query.filter(market=market)
        stock_list = [
            stock
            for stock in stock_query.only("code", "data_capabilities")
            if data_capability_helper.stock_supports(stock, "ma_factor")
        ]
        code_list = [stock.code for stock in stock_list]
        written_total = 0
        skipped_count = 0
        failed_count = 0
        for code in code_list:
            try:
                result = self.update_code(code)
            except Exception:
                failed_count += 1
                logger.exception("MA factor backfill failed: code=%s", code)
                continue
            if result.get("code") == "SKIP":
                skipped_count += 1
            written_total += int(result.get("written_count", 0))
        return {
            "pulled_count": len(code_list),
            "written_count": written_total,
            "skipped_count": skipped_count,
            "failed_count": failed_count,
        }
