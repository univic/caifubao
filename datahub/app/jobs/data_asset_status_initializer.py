from __future__ import annotations

import argparse
import datetime
import json
import logging
from dataclasses import dataclass
from typing import Any

from app.lib.utilities import data_capability_helper
from app.model.data_asset_status import (
    STATUS_NO_DATA,
    STATUS_NOT_APPLICABLE,
    STATUS_OK,
    STATUS_STALE,
)


ASSET_TYPE_QUOTE = "quote"
ASSET_TYPE_FACTOR = "factor"
ASSET_DAILY_QUOTE = "daily_quote"
ASSET_FQ_FACTOR = "FQ_FACTOR"
JOB_NAME = "data_asset_status_init"
DEFAULT_BATCH_SIZE = 100
MA_WINDOWS = {
    "MA_10": 10,
    "MA_20": 20,
    "MA_30": 30,
    "MA_60": 60,
    "MA_120": 120,
}
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DataStats:
    first_data_date: datetime.datetime | None = None
    latest_data_date: datetime.datetime | None = None
    data_count: int = 0


def _coverage_rate(data_count: int, expected_count: int | None) -> float | None:
    if not expected_count:
        return None
    return round(data_count / expected_count * 100, 2)


def _date_is_before(
    left: datetime.datetime | None, right: datetime.datetime | None
) -> bool:
    if left is None or right is None:
        return False
    return left.date() < right.date()


def _record(
    *,
    stock: Any,
    asset_type: str,
    asset_name: str,
    stats: DataStats | None,
    status: str,
    status_reason: str | None,
    calculated_at: datetime.datetime,
    expected_count: int | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    stats = stats or DataStats()
    return {
        "code": stock.code,
        "object_type": getattr(stock, "object_type", "individual_stock"),
        "asset_type": asset_type,
        "asset_name": asset_name,
        "first_data_date": stats.first_data_date,
        "latest_data_date": stats.latest_data_date,
        "data_count": stats.data_count,
        "expected_count": expected_count,
        "coverage_rate": _coverage_rate(stats.data_count, expected_count),
        "status": status,
        "status_reason": status_reason,
        "last_calculated_at": calculated_at,
        "last_success_at": calculated_at if status == STATUS_OK else None,
        "last_job_name": JOB_NAME,
        "extra": extra or {},
    }


def _status_for_stats(
    stats: DataStats | None,
    *,
    quote_stats: DataStats | None = None,
) -> tuple[str, str | None]:
    if stats is None or stats.data_count <= 0:
        return STATUS_NO_DATA, "no_source_data"
    if quote_stats and _date_is_before(
        stats.latest_data_date, quote_stats.latest_data_date
    ):
        return STATUS_STALE, "behind_daily_quote"
    return STATUS_OK, None


def build_asset_status_records(
    stocks: list[Any],
    *,
    quote_stats_by_code: dict[str, DataStats],
    fq_stats_by_code: dict[str, DataStats],
    ma_stats_by_asset: dict[str, dict[str, DataStats]],
    calculated_at: datetime.datetime | None = None,
) -> list[dict[str, Any]]:
    calculated_at = calculated_at or datetime.datetime.now()
    records: list[dict[str, Any]] = []

    for stock in stocks:
        code = stock.code
        object_type = getattr(stock, "object_type", "individual_stock")
        quote_stats = quote_stats_by_code.get(code)
        quote_count = quote_stats.data_count if quote_stats else 0

        if object_type != "individual_stock" or data_capability_helper.stock_supports(
            stock, "daily_quote"
        ):
            quote_status, quote_reason = _status_for_stats(quote_stats)
            quote_expected_count = quote_count or None
        else:
            quote_status, quote_reason = STATUS_NOT_APPLICABLE, "capability_disabled"
            quote_expected_count = None

        records.append(
            _record(
                stock=stock,
                asset_type=ASSET_TYPE_QUOTE,
                asset_name=ASSET_DAILY_QUOTE,
                stats=quote_stats,
                status=quote_status,
                status_reason=quote_reason,
                expected_count=quote_expected_count,
                calculated_at=calculated_at,
            )
        )

        if object_type != "individual_stock":
            continue

        if data_capability_helper.stock_supports(stock, "fq_factor"):
            fq_status, fq_reason = _status_for_stats(
                fq_stats_by_code.get(code), quote_stats=quote_stats
            )
        else:
            fq_status, fq_reason = STATUS_NOT_APPLICABLE, "capability_disabled"

        records.append(
            _record(
                stock=stock,
                asset_type=ASSET_TYPE_FACTOR,
                asset_name=ASSET_FQ_FACTOR,
                stats=fq_stats_by_code.get(code),
                status=fq_status,
                status_reason=fq_reason,
                expected_count=quote_count or None,
                calculated_at=calculated_at,
            )
        )

        for asset_name, window in MA_WINDOWS.items():
            ma_stats = ma_stats_by_asset.get(asset_name, {}).get(code)
            expected_count = max(quote_count - window + 1, 0)

            if not data_capability_helper.stock_supports(stock, "ma_factor"):
                ma_status = STATUS_NOT_APPLICABLE
                ma_reason = "capability_disabled"
                expected_count = None
            elif quote_count < window:
                ma_status = STATUS_NOT_APPLICABLE
                ma_reason = "insufficient_quote_history"
            else:
                ma_status, ma_reason = _status_for_stats(
                    ma_stats, quote_stats=quote_stats
                )

            records.append(
                _record(
                    stock=stock,
                    asset_type=ASSET_TYPE_FACTOR,
                    asset_name=asset_name,
                    stats=ma_stats,
                    status=ma_status,
                    status_reason=ma_reason,
                    expected_count=expected_count or None,
                    calculated_at=calculated_at,
                    extra={"window": window},
                )
            )

    return records


def _chunks(values: list[Any], batch_size: int):
    for index in range(0, len(values), batch_size):
        yield values[index : index + batch_size]


def _merge_status_counts(
    target: dict[str, int], source: dict[str, int]
) -> dict[str, int]:
    for status, count in source.items():
        target[status] = target.get(status, 0) + count
    return target


class DataAssetStatusInitializer:
    def __init__(
        self,
        *,
        stock_model: Any,
        index_model: Any | None = None,
        quote_model: Any,
        factor_model: Any,
        status_model: Any,
    ):
        self.stock_model = stock_model
        self.index_model = index_model
        self.quote_model = quote_model
        self.factor_model = factor_model
        self.status_model = status_model

    def run(
        self,
        *,
        codes: list[str] | None = None,
        limit: int | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        stocks = self._load_stocks(codes=codes, limit=limit)
        result = {
            "dry_run": dry_run,
            "stock_count": sum(
                1
                for stock in stocks
                if getattr(stock, "object_type", "individual_stock")
                == "individual_stock"
            ),
            "index_count": sum(
                1
                for stock in stocks
                if getattr(stock, "object_type", None) == "stock_index"
            ),
            "asset_count": 0,
            "written_count": 0,
            "batch_size": batch_size,
            "batch_count": 0,
            "status_counts": {},
        }

        for batch_index, stock_batch in enumerate(_chunks(stocks, batch_size), start=1):
            batch_result = self._run_batch(stock_batch, dry_run=dry_run)
            result["batch_count"] = batch_index
            result["asset_count"] += batch_result["asset_count"]
            result["written_count"] += batch_result["written_count"]
            _merge_status_counts(result["status_counts"], batch_result["status_counts"])
            logger.info(
                "data_asset_status_init batch=%s stock_count=%s asset_count=%s "
                "written_count=%s status_counts=%s",
                batch_index,
                batch_result["stock_count"],
                batch_result["asset_count"],
                batch_result["written_count"],
                batch_result["status_counts"],
            )

        return result

    def _run_batch(self, stocks: list[Any], *, dry_run: bool) -> dict[str, Any]:
        stock_codes = [stock.code for stock in stocks]
        quote_stats_by_code = self._aggregate_quote_stats(stock_codes)
        fq_stats_by_code = self._aggregate_fq_stats(stock_codes)
        ma_stats_by_asset = {
            asset_name: self._aggregate_ma_stats(stock_codes, asset_name)
            for asset_name in MA_WINDOWS
        }
        records = build_asset_status_records(
            stocks,
            quote_stats_by_code=quote_stats_by_code,
            fq_stats_by_code=fq_stats_by_code,
            ma_stats_by_asset=ma_stats_by_asset,
        )

        if not dry_run:
            for record in records:
                self._upsert_record(record)

        status_counts: dict[str, int] = {}
        for record in records:
            status_counts[record["status"]] = status_counts.get(record["status"], 0) + 1

        return {
            "stock_count": len(stocks),
            "asset_count": len(records),
            "written_count": 0 if dry_run else len(records),
            "status_counts": status_counts,
        }

    def _load_stocks(self, *, codes: list[str] | None, limit: int | None) -> list[Any]:
        query = self.stock_model.objects(active_status=0)
        if codes:
            query = query.filter(code__in=codes)
        query = query.only("code", "object_type", "data_capabilities").order_by("code")
        stocks = list(query)
        if self.index_model is not None:
            index_query = self.index_model.objects()
            if codes:
                index_query = index_query.filter(code__in=codes)
            stocks.extend(
                list(index_query.only("code", "object_type").order_by("code"))
            )
            stocks.sort(key=lambda stock: stock.code)
        if limit is not None:
            return stocks[:limit]
        return stocks

    def _aggregate_quote_stats(self, codes: list[str]) -> dict[str, DataStats]:
        return self._aggregate_stats(
            self.quote_model._get_collection(),
            match={"code": {"$in": codes}},
            code_field="code",
        )

    def _aggregate_fq_stats(self, codes: list[str]) -> dict[str, DataStats]:
        return self._aggregate_stats(
            self.quote_model._get_collection(),
            match={
                "code": {"$in": codes},
                "fq_factor": {"$exists": True, "$ne": None},
            },
            code_field="code",
        )

    def _aggregate_ma_stats(
        self, codes: list[str], asset_name: str
    ) -> dict[str, DataStats]:
        field_name = asset_name.lower()
        return self._aggregate_stats(
            self.factor_model._get_collection(),
            match={
                "stock_code": {"$in": codes},
                field_name: {"$exists": True, "$ne": None},
            },
            code_field="stock_code",
        )

    def _aggregate_stats(
        self, collection: Any, *, match: dict[str, Any], code_field: str
    ) -> dict[str, DataStats]:
        if not match[code_field]["$in"]:
            return {}

        rows = collection.aggregate(
            [
                {"$match": match},
                {
                    "$group": {
                        "_id": f"${code_field}",
                        "first_data_date": {"$min": "$date"},
                        "latest_data_date": {"$max": "$date"},
                        "data_count": {"$sum": 1},
                    }
                },
            ]
        )
        return {
            row["_id"]: DataStats(
                first_data_date=row.get("first_data_date"),
                latest_data_date=row.get("latest_data_date"),
                data_count=int(row.get("data_count", 0)),
            )
            for row in rows
            if row.get("_id")
        }

    def _upsert_record(self, record: dict[str, Any]) -> None:
        query = {
            "code": record["code"],
            "object_type": record["object_type"],
            "asset_type": record["asset_type"],
            "asset_name": record["asset_name"],
        }
        updates = {f"set__{key}": value for key, value in record.items()}
        self.status_model.objects(**query).update_one(upsert=True, **updates)


def _load_default_initializer() -> DataAssetStatusInitializer:
    from app.lib.db_watcher.mongoengine_tool import mongo_watcher
    from app.model.data_asset_status import DataAssetStatus
    from app.model.factor import StockFactorDaily
    from app.model.stock import IndividualStock, StockDailyQuote, StockIndex

    mongo_watcher.get_db_connection()
    return DataAssetStatusInitializer(
        stock_model=IndividualStock,
        index_model=StockIndex,
        quote_model=StockDailyQuote,
        factor_model=StockFactorDaily,
        status_model=DataAssetStatus,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Initialize data_asset_status from quote and factor collections."
    )
    parser.add_argument(
        "--code",
        action="append",
        dest="codes",
        default=[],
        help="Specific stock code to initialize. Can be passed multiple times.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of active stocks to initialize.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute status records without writing data_asset_status.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Number of stocks to aggregate per batch. Defaults to {DEFAULT_BATCH_SIZE}.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.limit is not None and args.limit < 1:
        raise SystemExit("--limit must be greater than 0.")
    if args.batch_size < 1:
        raise SystemExit("--batch-size must be greater than 0.")

    initializer = _load_default_initializer()
    result = initializer.run(
        codes=args.codes,
        limit=args.limit,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, default=str, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
