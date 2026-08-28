from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass
from typing import Any

import pandas as pd
from pymongo import UpdateOne

from app.lib.utilities import data_asset_status_helper
from app.lib.utilities import data_capability_helper
from app.model.data_asset_status import STATUS_OK, DataAssetStatus
from app.model.factor import StockFactorDaily
from app.model.signal import StockSignalDaily
from app.model.stock import IndividualStock, StockDailyQuote

logger = logging.getLogger(__name__)

SIGNAL_MA10_CROSS_MA20 = "MA10_CROSS_MA20"
SIGNAL_PRICE_ABOVE_MA60 = "PRICE_ABOVE_MA60"
SIGNAL_MA20_ABOVE_MA60 = "MA20_ABOVE_MA60"

SIGNAL_VERSION = "v1"
SIGNAL_ASSET_TYPE = "signal"


class SignalUpdateError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        failed_codes: list[str],
        written_count: int = 0,
    ):
        super().__init__(message)
        self.failed_codes = list(failed_codes)
        # Signals already persisted before the failure; the job-run record
        # keeps this so downstream scoring can proceed on the real data.
        self.written_count = int(written_count or 0)


@dataclass
class SignalConfig:
    name: str
    required_factors: list[str]
    direction: str = "BULLISH"
    signal_type: str = "MA_RELATION"
    reason_template: str = ""


class MovingAverageSignalService:
    """Generate MVP signals from wide MA factors."""

    def __init__(
        self,
        stock_model=IndividualStock,
        factor_model=StockFactorDaily,
        quote_model=StockDailyQuote,
        signal_model=StockSignalDaily,
        status_model=DataAssetStatus,
    ):
        self.stock_model = stock_model
        self.factor_model = factor_model
        self.quote_model = quote_model
        self.signal_model = signal_model
        self.status_model = status_model
        self.configs = {
            SIGNAL_MA10_CROSS_MA20: SignalConfig(
                name=SIGNAL_MA10_CROSS_MA20,
                required_factors=["ma_10", "ma_20"],
                signal_type="MA_CROSS",
                reason_template="MA10 上穿 MA20，短期均线重新转强。",
            ),
            SIGNAL_PRICE_ABOVE_MA60: SignalConfig(
                name=SIGNAL_PRICE_ABOVE_MA60,
                required_factors=["ma_60"],
                reason_template="收盘价站上 MA60，进入中期强势区间。",
            ),
            SIGNAL_MA20_ABOVE_MA60: SignalConfig(
                name=SIGNAL_MA20_ABOVE_MA60,
                required_factors=["ma_20", "ma_60"],
                reason_template="MA20 位于 MA60 之上，中期趋势向上。",
            ),
        }

    def build_signal_frame(
        self, config: SignalConfig, input_df: pd.DataFrame
    ) -> pd.DataFrame:
        if input_df.empty:
            return input_df.copy()

        missing_columns = [
            name for name in config.required_factors if name not in input_df
        ]
        if missing_columns:
            raise ValueError(
                f"Missing factor fields for {config.name}: {missing_columns}"
            )

        df = input_df.copy().sort_index()
        for col in config.required_factors:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        if config.name == SIGNAL_MA10_CROSS_MA20:
            df["previous_ma_10"] = df["ma_10"].shift(1)
            df["previous_ma_20"] = df["ma_20"].shift(1)
            valid = (
                df[["ma_10", "ma_20", "previous_ma_10", "previous_ma_20"]]
                .notna()
                .all(axis=1)
            )
            crossed = (df["previous_ma_10"] <= df["previous_ma_20"]) & (
                df["ma_10"] > df["ma_20"]
            )
            res = df[valid & crossed].copy()
            if not res.empty:
                res["strength"] = (
                    ((res["ma_10"] - res["ma_20"]) / res["ma_20"]) * 100
                ).round(4)
            return res

        elif config.name == SIGNAL_PRICE_ABOVE_MA60:
            if "close" not in df.columns:
                raise ValueError(f"Missing 'close' for {config.name}")
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            valid = df[["close", "ma_60"]].notna().all(axis=1)
            active = df["close"] > df["ma_60"]
            res = df[valid & active].copy()
            if not res.empty:
                res["strength"] = (
                    ((res["close"] - res["ma_60"]) / res["ma_60"]) * 100
                ).round(4)
            return res

        elif config.name == SIGNAL_MA20_ABOVE_MA60:
            valid = df[["ma_20", "ma_60"]].notna().all(axis=1)
            active = df["ma_20"] > df["ma_60"]
            res = df[valid & active].copy()
            if not res.empty:
                res["strength"] = (
                    ((res["ma_20"] - res["ma_60"]) / res["ma_60"]) * 100
                ).round(4)
            return res

        return pd.DataFrame()

    def _load_stock(self, code: str):
        return (
            self.stock_model.objects(code=code)
            .only("code", "name", "object_type", "data_capabilities")
            .first()
        )

    def _load_factor_df(
        self, code: str, anchor_date: datetime.datetime | None = None
    ) -> pd.DataFrame:
        start_date = None
        if anchor_date is not None:
            previous = (
                self.factor_model.objects(stock_code=code, date__lt=anchor_date)
                .only("date")
                .order_by("-date")
                .first()
            )
            start_date = getattr(previous, "date", anchor_date)

        factor_query = self.factor_model.objects(stock_code=code)
        quote_query = self.quote_model.objects(code=code)
        if start_date is not None:
            factor_query = factor_query.filter(date__gte=start_date)
            quote_query = quote_query.filter(date__gte=start_date)

        factor_rows = list(
            factor_query.only("date", "ma_10", "ma_20", "ma_60")
            .order_by("+date")
            .as_pymongo()
        )
        quote_rows = list(
            quote_query.only("date", "close").order_by("+date").as_pymongo()
        )

        f_df = pd.DataFrame(factor_rows)
        q_df = pd.DataFrame(quote_rows)

        if f_df.empty:
            return pd.DataFrame()

        f_df.set_index("date", inplace=True)
        if not q_df.empty:
            q_df.set_index("date", inplace=True)
            return pd.merge(f_df, q_df, left_index=True, right_index=True, how="left")
        return f_df

    def _load_signal_anchors(self, code: str) -> dict[str, datetime.datetime]:
        rows = self.status_model.objects(
            code=code,
            object_type="individual_stock",
            asset_type=SIGNAL_ASSET_TYPE,
            asset_name__in=list(self.configs),
            status=STATUS_OK,
        ).only("asset_name", "latest_data_date")
        return {
            row.asset_name: row.latest_data_date
            for row in rows
            if row.latest_data_date is not None
        }

    def _load_source_freshness(self, code: str) -> dict[str, Any]:
        status_rows = list(
            self.status_model.objects(
                code=code,
                object_type="individual_stock",
                asset_type="factor",
                asset_name__in=["MA_10", "MA_20", "MA_60"],
            ).only("asset_name", "latest_data_date", "status", "data_count")
        )
        return {
            row.asset_name: {
                "latest_data_date": row.latest_data_date,
                "status": row.status,
                "data_count": row.data_count,
            }
            for row in status_rows
        }

    def _build_bulk_operations(
        self,
        config: SignalConfig,
        stock_obj,
        signal_df: pd.DataFrame,
        source_freshness: dict[str, Any] | None = None,
        generated_at: datetime.datetime | None = None,
    ) -> list[UpdateOne]:
        operations: list[UpdateOne] = []
        source_freshness = source_freshness or {}
        generated_at = generated_at or datetime.datetime.now()
        for signal_date, row in signal_df.iterrows():
            snapshot = {f: float(row[f]) for f in config.required_factors if f in row}
            if "close" in row:
                snapshot["close"] = float(row["close"])
            if "previous_ma_10" in row:
                snapshot["previous_ma_10"] = float(row["previous_ma_10"])
            if "previous_ma_20" in row:
                snapshot["previous_ma_20"] = float(row["previous_ma_20"])

            operations.append(
                UpdateOne(
                    {
                        "stock_code": stock_obj.code,
                        "date": signal_date,
                        "signal_name": config.name,
                    },
                    {
                        "$set": {
                            "stock": stock_obj.to_dbref()
                            if hasattr(stock_obj, "to_dbref")
                            else None,
                            "stock_code": stock_obj.code,
                            "stock_name": getattr(stock_obj, "name", None),
                            "category": "stock",
                            "date": signal_date,
                            "signal_name": config.name,
                            "signal_version": SIGNAL_VERSION,
                            "direction": config.direction,
                            "signal_type": config.signal_type,
                            "strength": float(row.get("strength", 0)),
                            "reason": config.reason_template,
                            "factor_snapshot": snapshot,
                        },
                        "$setOnInsert": {
                            "source_freshness": source_freshness,
                            "generated_at": generated_at,
                        },
                    },
                    upsert=True,
                )
            )
        return operations

    def _refresh_signal_statuses(
        self,
        stock_obj,
        target_date: datetime.datetime,
        calculated_at: datetime.datetime,
    ) -> None:
        self._refresh_market_signal_statuses(
            {stock_obj.code: target_date},
            calculated_at=calculated_at,
            object_type_by_code={
                stock_obj.code: getattr(stock_obj, "object_type", "individual_stock")
            },
        )

    def _refresh_market_signal_statuses(
        self,
        code_targets: dict[str, datetime.datetime],
        *,
        calculated_at: datetime.datetime | None = None,
        object_type_by_code: dict[str, str] | None = None,
    ) -> None:
        if not code_targets:
            return
        calculated_at = calculated_at or datetime.datetime.now()
        codes = list(code_targets)
        if object_type_by_code is None:
            stocks = self.stock_model.objects(code__in=codes).only(
                "code", "object_type"
            )
            object_type_by_code = {
                stock.code: getattr(stock, "object_type", "individual_stock")
                for stock in stocks
            }
        rows = self.signal_model._get_collection().aggregate(
            [
                {
                    "$match": {
                        "stock_code": {"$in": codes},
                        "signal_name": {"$in": list(self.configs)},
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "stock_code": "$stock_code",
                            "signal_name": "$signal_name",
                        },
                        "data_count": {"$sum": 1},
                    }
                },
            ]
        )
        counts = {
            (row["_id"]["stock_code"], row["_id"]["signal_name"]): int(
                row["data_count"]
            )
            for row in rows
        }
        records = [
            {
                "code": code,
                "object_type": object_type_by_code.get(code, "individual_stock"),
                "asset_type": SIGNAL_ASSET_TYPE,
                "asset_name": signal_name,
                "latest_data_date": code_targets[code],
                "data_count": counts.get((code, signal_name), 0),
                "status": STATUS_OK,
                "status_reason": "signal calculation completed",
                "last_calculated_at": calculated_at,
                "last_success_at": calculated_at,
                "last_job_name": "ma_signal_sync",
                "error_message": None,
            }
            for code in codes
            for signal_name in self.configs
        ]
        data_asset_status_helper.bulk_upsert_asset_status(
            records, collection=self.status_model._get_collection()
        )

    def update_code(
        self, code: str, *, force: bool = False, refresh_statuses: bool = True
    ) -> dict[str, Any]:
        stock_obj = self._load_stock(code)
        if not stock_obj:
            return {"code": "FAIL", "written_count": 0, "message": "stock not found"}
        if not data_capability_helper.stock_supports(stock_obj, "ma_factor"):
            return {
                "code": "SKIP",
                "written_count": 0,
                "message": "ma_factor is not supported for this stock",
            }

        anchors = {} if force else self._load_signal_anchors(code)
        incremental_anchor = (
            min(anchors.values()) if len(anchors) == len(self.configs) else None
        )
        factor_df = self._load_factor_df(code, anchor_date=incremental_anchor)
        if factor_df.empty:
            if force:
                raise RuntimeError(
                    f"cannot rebuild signals without factor data: {code}"
                )
            return {"code": "GOOD", "written_count": 0, "message": "no factor data"}

        source_freshness = self._load_source_freshness(code)
        generated_at = datetime.datetime.now()
        target_date = factor_df.index.max()
        operations: list[UpdateOne] = []
        authoritative_keys: list[dict[str, Any]] = []

        for signal_name, config in self.configs.items():
            signal_df = self.build_signal_frame(config, factor_df)
            if not force:
                anchor = anchors.get(signal_name)
                if anchor is not None:
                    signal_df = signal_df[signal_df.index > anchor]
            if force:
                authoritative_keys.extend(
                    {"signal_name": config.name, "date": signal_date}
                    for signal_date in signal_df.index
                )
            operations.extend(
                self._build_bulk_operations(
                    config,
                    stock_obj,
                    signal_df,
                    source_freshness=source_freshness,
                    generated_at=generated_at,
                )
            )

        collection = self.signal_model._get_collection()
        if operations:
            collection.bulk_write(operations, ordered=force)
        if force:
            delete_filter: dict[str, Any] = {
                "stock_code": code,
                "signal_name": {"$in": list(self.configs)},
            }
            if authoritative_keys:
                delete_filter["$nor"] = authoritative_keys
            collection.delete_many(delete_filter)
        if refresh_statuses:
            self._refresh_signal_statuses(stock_obj, target_date, generated_at)

        written_count = len(operations)
        return {
            "code": "GOOD",
            "written_count": written_count,
            "message": None,
            "target_date": target_date,
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

        # Check freshness for all signals. For simplicity in MVP, if ANY signal needs update, we update all for the code.
        # We use MA_60 as the common denominator since it's used by the new signals.
        factor_status_rows = list(
            self.status_model.objects(
                code__in=code_list,
                object_type="individual_stock",
                asset_type="factor",
                asset_name__in=["MA_10", "MA_20", "MA_60"],
                status=STATUS_OK,
            ).only("code", "asset_name", "latest_data_date")
        )

        signal_status_rows = list(
            self.status_model.objects(
                code__in=code_list,
                object_type="individual_stock",
                asset_type=SIGNAL_ASSET_TYPE,
                asset_name__in=list(self.configs),
            ).only("code", "asset_name", "latest_data_date", "status")
        )

        factor_dates: dict[str, dict[str, datetime.datetime]] = {}
        for row in factor_status_rows:
            factor_dates.setdefault(row.code, {})[row.asset_name] = row.latest_data_date

        signal_dates = {
            (row.code, row.asset_name): (row.latest_data_date, row.status)
            for row in signal_status_rows
        }

        result: list[str] = []
        for code in code_list:
            ma_dates = factor_dates.get(code, {})
            # If we don't have basic MA data, skip
            if (
                not ma_dates.get("MA_10")
                or not ma_dates.get("MA_20")
                or not ma_dates.get("MA_60")
            ):
                continue

            # Use the oldest of the 3 MAs as the target date
            latest_required_date = min(
                ma_dates["MA_10"], ma_dates["MA_20"], ma_dates["MA_60"]
            )
            if any(
                signal_dates.get((code, signal_name))
                != (latest_required_date, STATUS_OK)
                for signal_name in self.configs
            ):
                result.append(code)

        return result

    def update_market(
        self, market=None, selected_codes: list[str] | None = None
    ) -> dict[str, Any]:
        codes = (
            list(selected_codes)
            if selected_codes is not None
            else self.get_codes_requiring_update(market=market)
        )
        written_total = 0
        skipped_count = 0
        failed_count = 0
        failed_codes: list[str] = []
        status_targets: dict[str, datetime.datetime] = {}
        for code in codes:
            try:
                result = self.update_code(code, refresh_statuses=False)
            except Exception:
                failed_count += 1
                failed_codes.append(code)
                logger.exception("Signal update failed: code=%s", code)
                continue
            if result.get("code") == "SKIP":
                skipped_count += 1
            elif result.get("code") != "GOOD":
                failed_count += 1
                failed_codes.append(code)
                continue
            if result.get("code") == "GOOD" and result.get("target_date") is not None:
                status_targets[code] = result["target_date"]
            written_total += int(result.get("written_count", 0))
        if failed_codes:
            raise SignalUpdateError(
                "signal market update failed for codes: " + ", ".join(failed_codes),
                failed_codes=failed_codes,
                written_count=written_total,
            )
        try:
            self._refresh_market_signal_statuses(status_targets)
        except Exception as exc:
            raise SignalUpdateError(
                "signal status update failed",
                failed_codes=list(status_targets),
                written_count=written_total,
            ) from exc
        return {
            "pulled_count": len(codes),
            "written_count": written_total,
            "skipped_count": skipped_count,
            "failed_count": failed_count,
            "failed_codes": failed_codes,
        }
