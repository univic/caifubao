from __future__ import annotations

import logging
import math

import pandas as pd
from pymongo import UpdateOne

from app.model.stock import IndividualStock, StockDailyQuote
from app.model.data_asset_status import STATUS_OK, DataAssetStatus
from app.lib.utilities import data_asset_status_helper
from app.lib.utilities import data_capability_helper


logger = logging.getLogger(__name__)


class FQFactorService:
    FACTOR_NAME = "FQ_FACTOR"

    def __init__(self, quote_model=StockDailyQuote, stock_model=IndividualStock):
        self.quote_model = quote_model
        self.stock_model = stock_model

    @staticmethod
    def _validate_quote_df(input_df: pd.DataFrame) -> None:
        required_columns = {"code", "open", "close", "high", "low", "previous_close"}
        missing_columns = required_columns - set(input_df.columns)
        if missing_columns:
            raise ValueError(
                f"Missing required columns for FQ factor calculation: {sorted(missing_columns)}"
            )
        invalid_rows = input_df[
            input_df["previous_close"].isna()
            | (input_df["previous_close"] <= 0)
            | input_df["close"].isna()
            | (input_df["close"] <= 0)
        ]
        if not invalid_rows.empty:
            raise ValueError(
                "Invalid quote rows found for FQ factor calculation: "
                f"{invalid_rows.index.tolist()[:5]}"
            )

    @classmethod
    def build_fq_factor_frame(
        cls,
        input_df: pd.DataFrame,
        adj_factor_df: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Attach real after-adjustment factors to quote rows.

        ``adj_factor_df`` is the tushare ``pro.adj_factor`` frame for the same
        code/date range (columns: trade_date, adj_factor). The real factor only
        changes on ex-dividend dates; it is constant otherwise. When a trading
        day is missing from the factor frame, the most recent known factor is
        carried forward (fallback anchor), so a factor is always available for
        every quote row.

        Adjusted prices are derived as ``close_hfq = close * fq_factor`` with
        open/high/low scaled by the same ratio as close.
        """
        if input_df.empty:
            return input_df.copy()

        cls._validate_quote_df(input_df)

        process_df = input_df.copy()
        if process_df.index.name != "date":
            process_df = process_df.set_index("date")

        if adj_factor_df is not None and not adj_factor_df.empty:
            factor_map = {}
            for _, row in adj_factor_df.iterrows():
                try:
                    ts = row["trade_date"]
                    if not isinstance(ts, str):
                        ts = str(int(ts))
                    factor_value = float(row["adj_factor"])
                    if not math.isfinite(factor_value) or factor_value <= 0:
                        continue
                    d = pd.Timestamp(ts)
                    factor_map[d] = factor_value
                except (KeyError, TypeError, ValueError):
                    continue
            if not factor_map:
                raise RuntimeError("adj_factor response contains no valid factor rows")

            factor_series = pd.Series(factor_map, dtype=float).sort_index()
            aligned_index = factor_series.index.union(process_df.index).sort_values()
            process_df["fq_factor"] = (
                factor_series.reindex(aligned_index)
                .ffill()
                .bfill()
                .reindex(process_df.index)
            )
        else:
            raise RuntimeError("adj_factor unavailable for quote rows")

        process_df["close_hfq"] = process_df["close"] * process_df["fq_factor"]
        scale = process_df["close_hfq"] / process_df["close"]
        process_df["open_hfq"] = process_df["open"] * scale
        process_df["high_hfq"] = process_df["high"] * scale
        process_df["low_hfq"] = process_df["low"] * scale

        for column in ["fq_factor", "close_hfq", "open_hfq", "high_hfq", "low_hfq"]:
            process_df[column] = process_df[column].round(4)

        return process_df

    def _load_quote_df(self, code: str, date_gt=None) -> pd.DataFrame:
        query = self.quote_model.objects(code=code).only(
            "code", "date", "open", "close", "high", "low", "previous_close"
        )
        if date_gt is not None:
            query = query.filter(date__gt=date_gt)
        quote_json = query.order_by("+date").as_pymongo()
        quote_df = pd.DataFrame(quote_json)
        if not quote_df.empty and "date" in quote_df.columns:
            quote_df.set_index("date", inplace=True)
        return quote_df

    @staticmethod
    def _build_bulk_operations(output_df: pd.DataFrame) -> list[UpdateOne]:
        bulk_operations: list[UpdateOne] = []
        for quote_date, row in output_df.iterrows():
            bulk_operations.append(
                UpdateOne(
                    {"code": row["code"], "date": quote_date},
                    {
                        "$set": {
                            "fq_factor": row["fq_factor"],
                            "close_hfq": row["close_hfq"],
                            "open_hfq": row["open_hfq"],
                            "high_hfq": row["high_hfq"],
                            "low_hfq": row["low_hfq"],
                        }
                    },
                    upsert=False,
                )
            )
        return bulk_operations

    def update_code(self, code: str) -> dict[str, int | str | None]:
        stock_obj = (
            self.stock_model.objects(code=code)
            .only("code", "name", "object_type", "data_capabilities")
            .first()
        )
        if not stock_obj:
            return {"code": "FAIL", "written_count": 0, "message": "stock not found"}
        if not data_capability_helper.stock_supports(stock_obj, "fq_factor"):
            return {
                "code": "SKIP",
                "written_count": 0,
                "message": "fq_factor is not supported for this stock",
            }

        quote_df = self._load_quote_df(code)
        if quote_df.empty:
            data_asset_status_helper.refresh_fq_factor_status(
                stock_obj=stock_obj,
                quote_model=self.quote_model,
                last_job_name="fq_factor_sync",
            )
            return {"code": "GOOD", "written_count": 0, "message": None}

        adj_factor_df = self._load_adj_factor_df(code, quote_df)
        if adj_factor_df is None:
            raise RuntimeError(f"adj_factor unavailable for {code}")

        output_df = self.build_fq_factor_frame(quote_df, adj_factor_df=adj_factor_df)

        bulk_operations = self._build_bulk_operations(output_df)
        if bulk_operations:
            self.quote_model._get_collection().bulk_write(
                bulk_operations, ordered=False
            )

        latest_output_date = output_df.index[-1]
        data_asset_status_helper.refresh_fq_factor_status(
            stock_obj=stock_obj,
            quote_model=self.quote_model,
            last_job_name="fq_factor_sync",
        )
        logger.info(
            "FQ factor updated: code=%s written=%s latest_date=%s",
            stock_obj.code,
            len(output_df),
            latest_output_date,
        )
        return {"code": "GOOD", "written_count": len(output_df), "message": None}

    def _load_adj_factor_df(self, code: str, quote_df: pd.DataFrame) -> pd.DataFrame:
        """Fetch real tushare adj_factor for the quote date span.

        Source errors and empty responses are raised so the runner records a
        failed code without overwriting existing FQ fields.
        """
        from app.lib.datahub.data_source.interface import tushare_interface

        ts_code = tushare_interface.to_tushare_ts_code(code)
        start = quote_df.index.min().strftime("%Y%m%d")
        end = quote_df.index.max().strftime("%Y%m%d")
        raw = tushare_interface.adj_factor(ts_code, start, end)
        if raw is None or raw.empty:
            raise RuntimeError(f"adj_factor returned no rows for {code}")
        return raw

    def get_codes_requiring_update(self, market=None) -> list[str]:
        stock_query = self.stock_model.objects(active_status=0)
        if market is not None:
            stock_query = stock_query.filter(market=market)

        stock_list = [
            stock
            for stock in stock_query.only("code", "data_capabilities")
            if data_capability_helper.stock_supports(stock, "fq_factor")
        ]
        if not stock_list:
            return []

        code_list = [stock.code for stock in stock_list]
        quote_status_list = list(
            DataAssetStatus.objects(
                code__in=code_list,
                object_type="individual_stock",
                asset_type="quote",
                asset_name="daily_quote",
                status=STATUS_OK,
            ).only("code", "latest_data_date")
        )
        factor_status_list = list(
            DataAssetStatus.objects(
                code__in=code_list,
                object_type="individual_stock",
                asset_type="factor",
                asset_name=self.FACTOR_NAME,
            ).only("code", "latest_data_date", "status")
        )
        quote_status_map = {
            item.code: item.latest_data_date for item in quote_status_list
        }
        factor_status_map = {
            item.code: (item.latest_data_date, item.status)
            for item in factor_status_list
        }

        result: list[str] = []
        for code in code_list:
            quote_dt = quote_status_map.get(code)
            factor_dt, factor_status = factor_status_map.get(code, (None, None))
            if quote_dt and (factor_status != STATUS_OK or quote_dt != factor_dt):
                result.append(code)
        return result

    def update_market(self, market=None) -> dict[str, int]:
        codes = self.get_codes_requiring_update(market=market)
        written_total = 0
        failed_count = 0
        for code in codes:
            try:
                result = self.update_code(code)
            except Exception:
                failed_count += 1
                logger.exception("FQ factor update failed: code=%s", code)
                continue
            written_total += int(result.get("written_count", 0))
        return {
            "pulled_count": len(codes),
            "written_count": written_total,
            "failed_count": failed_count,
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
            if data_capability_helper.stock_supports(stock, "fq_factor")
        ]
        code_list = [stock.code for stock in stock_list]
        written_total = 0
        failed_count = 0
        for code in code_list:
            try:
                result = self.update_code(code)
            except Exception:
                failed_count += 1
                logger.exception("FQ factor backfill failed: code=%s", code)
                continue
            written_total += int(result.get("written_count", 0))
        return {
            "pulled_count": len(code_list),
            "written_count": written_total,
            "failed_count": failed_count,
        }
