from __future__ import annotations

import datetime
import logging
import math
from collections import defaultdict

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

    @classmethod
    def build_market_snapshot_frame(
        cls,
        quote_df: pd.DataFrame,
        adj_factor_df: pd.DataFrame,
        target_date,
    ) -> pd.DataFrame:
        """Join one trading day's persisted quotes to its market factor snapshot."""
        if quote_df.empty:
            return quote_df.copy()
        if adj_factor_df is None or adj_factor_df.empty:
            raise RuntimeError("daily adj_factor snapshot is empty")

        process_df = quote_df.copy()
        if process_df.index.name == "date":
            process_df = process_df.reset_index()
        cls._validate_quote_df(process_df)

        target = pd.Timestamp(target_date).normalize()
        quote_dates = pd.to_datetime(process_df["date"], errors="coerce").dt.normalize()
        if quote_dates.isna().any() or not quote_dates.eq(target).all():
            raise RuntimeError("daily quote snapshot contains mismatched dates")
        if process_df["code"].duplicated().any():
            raise RuntimeError("daily quote snapshot contains duplicate codes")

        required_factor_columns = {"ts_code", "trade_date", "adj_factor"}
        missing_columns = required_factor_columns - set(adj_factor_df.columns)
        if missing_columns:
            raise RuntimeError(
                f"daily adj_factor snapshot missing columns: {sorted(missing_columns)}"
            )

        factor_df = adj_factor_df.copy()
        factor_dates = pd.to_datetime(
            factor_df["trade_date"], format="%Y%m%d", errors="coerce"
        ).dt.normalize()
        if factor_dates.isna().any() or not factor_dates.eq(target).all():
            raise RuntimeError("daily adj_factor snapshot contains mismatched dates")
        if factor_df["ts_code"].duplicated().any():
            raise RuntimeError("daily adj_factor snapshot contains duplicate codes")

        from app.lib.datahub.data_source.interface import tushare_interface

        factor_df["code"] = factor_df["ts_code"].map(
            tushare_interface.from_tushare_ts_code
        )
        factor_df["adj_factor"] = pd.to_numeric(
            factor_df["adj_factor"], errors="coerce"
        )
        valid_factor = factor_df["adj_factor"].map(
            lambda value: math.isfinite(value) and value > 0
        )
        factor_df = factor_df[valid_factor]

        quote_codes = set(process_df["code"])
        factor_codes = set(factor_df["code"])
        missing_codes = sorted(quote_codes - factor_codes)
        if missing_codes:
            raise RuntimeError(
                "daily adj_factor snapshot missing valid factors: "
                f"count={len(missing_codes)} examples={missing_codes[:10]}"
            )

        merged = process_df.merge(
            factor_df[["code", "adj_factor"]],
            on="code",
            how="left",
            validate="one_to_one",
        )
        merged["fq_factor"] = merged["adj_factor"]
        merged["close_hfq"] = merged["close"] * merged["fq_factor"]
        scale = merged["close_hfq"] / merged["close"]
        merged["open_hfq"] = merged["open"] * scale
        merged["high_hfq"] = merged["high"] * scale
        merged["low_hfq"] = merged["low"] * scale
        for column in ["fq_factor", "close_hfq", "open_hfq", "high_hfq", "low_hfq"]:
            merged[column] = merged[column].round(4)
        return merged.drop(columns=["adj_factor"]).set_index("date")

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

    @staticmethod
    def _is_next_trading_day(previous_date, target_date, market) -> bool:
        if (
            previous_date is None
            or target_date is None
            or not market
            or not getattr(market, "trade_calendar", None)
        ):
            return False
        previous = pd.Timestamp(previous_date).normalize()
        target = pd.Timestamp(target_date).normalize()
        calendar = sorted(
            pd.Timestamp(value).normalize()
            for value in market.trade_calendar
            if pd.Timestamp(value).normalize() <= target
        )
        try:
            target_position = calendar.index(target)
        except ValueError:
            return False
        return target_position > 0 and calendar[target_position - 1] == previous

    def _get_market_update_plan(
        self, market=None
    ) -> tuple[dict[str, object], list[str]]:
        stock_query = self.stock_model.objects(active_status=0)
        if market is not None:
            stock_query = stock_query.filter(market=market)

        stock_list = [
            stock
            for stock in stock_query.only("code", "data_capabilities")
            if data_capability_helper.stock_supports(stock, "fq_factor")
        ]
        if not stock_list:
            return {}, []

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

        snapshot_updates: dict[str, object] = {}
        historical_updates: list[str] = []
        for code in code_list:
            quote_dt = quote_status_map.get(code)
            factor_dt, factor_status = factor_status_map.get(code, (None, None))
            if quote_dt and (factor_status != STATUS_OK or quote_dt != factor_dt):
                if factor_dt and self._is_next_trading_day(factor_dt, quote_dt, market):
                    snapshot_updates[code] = quote_dt
                else:
                    historical_updates.append(code)
        return snapshot_updates, historical_updates

    def get_codes_requiring_update(self, market=None) -> list[str]:
        snapshot_updates, historical_updates = self._get_market_update_plan(
            market=market
        )
        return list(snapshot_updates) + historical_updates

    def _load_market_quote_snapshot(
        self, codes: list[str], target_date
    ) -> pd.DataFrame:
        quote_rows = (
            self.quote_model.objects(code__in=codes, date=target_date)
            .only("code", "date", "open", "close", "high", "low", "previous_close")
            .as_pymongo()
        )
        return pd.DataFrame(quote_rows)

    @staticmethod
    def _load_market_adj_factor_snapshot(target_date) -> pd.DataFrame:
        from app.lib.datahub.data_source.interface import tushare_interface

        trade_date = pd.Timestamp(target_date).strftime("%Y%m%d")
        return tushare_interface.adj_factor_by_trade_date(trade_date)

    def _refresh_market_snapshot_statuses(self, codes: list[str]) -> None:
        """Refresh FQ statuses for the snapshot batch with bulk round trips.

        Reads committed data only: the FQ aggregate runs after the snapshot
        bulk write has completed, and the quote statuses it compares against
        were committed by the earlier quote phase. Record construction uses
        the same pure builder as the single-code path, so statuses are
        identical to per-stock refresh_fq_factor_status calls.
        """
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
        fq_stats_by_code = data_asset_status_helper.aggregate_stats_by_code(
            self.quote_model._get_collection(),
            match={
                "code": {"$in": codes},
                "fq_factor": {"$exists": True, "$ne": None},
            },
            code_field="code",
        )
        calculated_at = datetime.datetime.now()
        records = [
            data_asset_status_helper.build_fq_status_record(
                code=code,
                object_type=object_type_by_code[code],
                stats_row=fq_stats_by_code.get(code),
                quote_status=quote_status_map.get(code),
                last_job_name="fq_factor_sync",
                calculated_at=calculated_at,
            )
            for code in codes
            if code in object_type_by_code
        ]
        data_asset_status_helper.bulk_upsert_asset_status(records)

    def update_codes_from_market_snapshots(
        self, code_dates: dict[str, object]
    ) -> dict[str, int]:
        grouped_codes: dict[object, list[str]] = defaultdict(list)
        for code, target_date in code_dates.items():
            grouped_codes[pd.Timestamp(target_date).to_pydatetime()].append(code)

        written_total = 0
        snapshot_count = 0
        factor_total = 0
        matched_total = 0
        ignored_extra_total = 0
        for target_date, codes in sorted(grouped_codes.items()):
            quote_df = self._load_market_quote_snapshot(codes, target_date)
            loaded_codes = set(quote_df["code"]) if "code" in quote_df else set()
            missing_quote_codes = sorted(set(codes) - loaded_codes)
            if missing_quote_codes:
                raise RuntimeError(
                    "daily quote snapshot missing planned codes: "
                    f"target_date={pd.Timestamp(target_date).date()} "
                    f"count={len(missing_quote_codes)} "
                    f"examples={missing_quote_codes[:10]}"
                )
            adj_factor_df = self._load_market_adj_factor_snapshot(target_date)
            snapshot_count += 1
            factor_total += len(adj_factor_df)
            output_df = self.build_market_snapshot_frame(
                quote_df, adj_factor_df, target_date
            )
            operations = self._build_bulk_operations(output_df)
            if operations:
                self.quote_model._get_collection().bulk_write(operations, ordered=False)
            written_codes = output_df["code"].tolist()
            self._refresh_market_snapshot_statuses(written_codes)
            written_total += len(output_df)
            matched_total += len(output_df)
            ignored_extra_total += len(adj_factor_df) - len(output_df)
            logger.info(
                "FQ market snapshot updated: target_date=%s quotes=%s factors=%s "
                "matched=%s ignored_extra=%s written=%s",
                pd.Timestamp(target_date).date(),
                len(quote_df),
                len(adj_factor_df),
                len(output_df),
                len(adj_factor_df) - len(output_df),
                len(output_df),
            )

        return {
            "pulled_count": len(code_dates),
            "written_count": written_total,
            "failed_count": 0,
            "snapshot_count": snapshot_count,
            "factor_count": factor_total,
            "matched_count": matched_total,
            "ignored_extra_count": ignored_extra_total,
        }

    def update_market(
        self, market=None, selected_codes: list[str] | None = None
    ) -> dict[str, object]:
        code_dates, historical_codes = self._get_market_update_plan(market=market)
        if selected_codes is not None:
            selected = set(selected_codes)
            code_dates = {
                code: target_date
                for code, target_date in code_dates.items()
                if code in selected
            }
            historical_codes = [code for code in historical_codes if code in selected]
        result = self.update_codes_from_market_snapshots(code_dates)
        failed_codes = []
        for code in historical_codes:
            try:
                update_result = self.update_code(code)
            except Exception:
                failed_codes.append(code)
                logger.exception("FQ historical repair failed: code=%s", code)
                continue
            result["written_count"] += int(update_result.get("written_count", 0))
        result["pulled_count"] += len(historical_codes)
        result["failed_count"] += len(failed_codes)
        result["failed_codes"] = failed_codes
        if failed_codes:
            raise RuntimeError(
                "FQ historical repair failed: "
                f"count={len(failed_codes)} examples={failed_codes[:10]}"
            )
        return result

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
