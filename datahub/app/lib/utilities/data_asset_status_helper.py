from __future__ import annotations

import datetime
import logging
from typing import Any

from pymongo import UpdateOne

from app.lib.utilities.mongo_error_helper import is_duplicate_only_bulk_write_error
from app.model.data_asset_status import (
    STATUS_AHEAD,
    STATUS_NO_DATA,
    STATUS_NOT_APPLICABLE,
    STATUS_OK,
    STATUS_STALE,
    DataAssetStatus,
)


ASSET_TYPE_QUOTE = "quote"
ASSET_TYPE_FACTOR = "factor"
ASSET_DAILY_QUOTE = "daily_quote"
ASSET_FQ_FACTOR = "FQ_FACTOR"

STATUS_UPSERT_CHUNK_SIZE = 1000

logger = logging.getLogger(__name__)


def read_asset_status(code: str, object_type: str, asset_type: str, asset_name: str):
    return DataAssetStatus.objects(
        code=code,
        object_type=object_type,
        asset_type=asset_type,
        asset_name=asset_name,
    ).first()


def read_asset_latest_date(
    code: str, object_type: str, asset_type: str, asset_name: str
):
    status = read_asset_status(code, object_type, asset_type, asset_name)
    return status.latest_data_date if status else None


def read_quote_status_map(codes: list[str]) -> dict[str, Any]:
    """Read daily-quote asset statuses for many codes in one query.

    Returns a mapping of code -> DataAssetStatus document. Codes without a
    status document are simply absent from the mapping.
    """
    if not codes:
        return {}
    docs = DataAssetStatus.objects(
        code__in=codes,
        asset_type=ASSET_TYPE_QUOTE,
        asset_name=ASSET_DAILY_QUOTE,
    ).only("code", "object_type", "latest_data_date", "data_count")
    return {doc.code: doc for doc in docs}


def upsert_asset_status(
    *,
    code: str,
    object_type: str,
    asset_type: str,
    asset_name: str,
    latest_data_date=None,
    first_data_date=None,
    data_count: int = 0,
    expected_count: int | None = None,
    status: str,
    status_reason: str | None = None,
    last_job_name: str | None = None,
    error_message: str | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    calculated_at = datetime.datetime.now()
    record = {
        "code": code,
        "object_type": object_type,
        "asset_type": asset_type,
        "asset_name": asset_name,
        "first_data_date": first_data_date,
        "latest_data_date": latest_data_date,
        "data_count": data_count,
        "expected_count": expected_count,
        "coverage_rate": round(data_count / expected_count * 100, 2)
        if expected_count
        else None,
        "status": status,
        "status_reason": status_reason,
        "last_calculated_at": calculated_at,
        "last_job_name": last_job_name,
        "error_message": error_message,
        "extra": extra or {},
    }
    if status == STATUS_OK:
        record["last_success_at"] = calculated_at
    _upsert_record(record)


def _upsert_record(record: dict[str, Any]) -> None:
    query = {
        "code": record["code"],
        "object_type": record["object_type"],
        "asset_type": record["asset_type"],
        "asset_name": record["asset_name"],
    }
    updates = {f"set__{key}": value for key, value in record.items()}
    DataAssetStatus.objects(**query).update_one(upsert=True, **updates)


def aggregate_stats(collection: Any, *, match: dict[str, Any], code_field: str):
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
    return next(iter(rows), None)


def aggregate_stats_by_code(
    collection: Any, *, match: dict[str, Any], code_field: str
) -> dict[str, dict[str, Any]]:
    """Aggregate first/latest/count for many codes in one pipeline.

    Same grouping as aggregate_stats, but for every code matched by the
    ``$in`` filter at once. Returns raw grouped rows keyed by code.
    """
    if not match[code_field].get("$in"):
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
    return {row["_id"]: row for row in rows if row.get("_id")}


def _date_is_before(left, right) -> bool:
    if left is None or right is None:
        return False
    left_date = left.date() if hasattr(left, "date") else left
    right_date = right.date() if hasattr(right, "date") else right
    return left_date < right_date


def classify_quote_status(
    data_count,
    latest_data_date,
    expected_latest_date=None,
):
    if data_count <= 0 or latest_data_date is None:
        return STATUS_NO_DATA, "no_source_data"
    if expected_latest_date is None:
        return STATUS_OK, None
    if _date_is_before(latest_data_date, expected_latest_date):
        return STATUS_STALE, "behind_expected_quote_date"
    if _date_is_before(expected_latest_date, latest_data_date):
        return STATUS_AHEAD, "ahead_of_expected_quote_date"
    return STATUS_OK, None


def expected_quote_count(
    data_count,
    latest_data_date,
    expected_latest_date=None,
    trade_calendar=None,
):
    if (
        data_count <= 0
        or latest_data_date is None
        or expected_latest_date is None
        or not trade_calendar
        or not _date_is_before(latest_data_date, expected_latest_date)
    ):
        return data_count or None

    latest_date = (
        latest_data_date.date()
        if hasattr(latest_data_date, "date")
        else latest_data_date
    )
    expected_date = (
        expected_latest_date.date()
        if hasattr(expected_latest_date, "date")
        else expected_latest_date
    )
    missing_trading_days = sum(
        latest_date < (item.date() if hasattr(item, "date") else item) <= expected_date
        for item in trade_calendar
    )
    return data_count + missing_trading_days


def build_quote_status_record(
    *,
    code: str,
    object_type: str,
    stats_row: dict[str, Any] | None,
    expected_latest_date=None,
    trade_calendar=None,
    last_job_name: str | None = None,
    calculated_at: datetime.datetime | None = None,
) -> dict[str, Any]:
    """Build the daily-quote status record from one aggregate row.

    Pure function: same classification inputs and output fields as
    refresh_quote_status, usable for both single-code and batched refresh.
    """
    calculated_at = calculated_at or datetime.datetime.now()
    data_count = int(stats_row.get("data_count", 0)) if stats_row else 0
    latest_data_date = stats_row.get("latest_data_date") if stats_row else None
    first_data_date = stats_row.get("first_data_date") if stats_row else None
    expected_count = expected_quote_count(
        data_count,
        latest_data_date,
        expected_latest_date,
        trade_calendar,
    )
    status, status_reason = classify_quote_status(
        data_count,
        latest_data_date,
        expected_latest_date,
    )
    record = {
        "code": code,
        "object_type": object_type,
        "asset_type": ASSET_TYPE_QUOTE,
        "asset_name": ASSET_DAILY_QUOTE,
        "first_data_date": first_data_date,
        "latest_data_date": latest_data_date,
        "data_count": data_count,
        "expected_count": expected_count,
        "coverage_rate": round(data_count / expected_count * 100, 2)
        if expected_count
        else None,
        "status": status,
        "status_reason": status_reason,
        "last_calculated_at": calculated_at,
        "last_job_name": last_job_name,
        "error_message": None,
        "extra": {"expected_latest_date": expected_latest_date}
        if expected_latest_date
        else {},
    }
    if status == STATUS_OK:
        record["last_success_at"] = calculated_at
    return record


def build_fq_status_record(
    *,
    code: str,
    object_type: str,
    stats_row: dict[str, Any] | None,
    quote_status: Any | None = None,
    last_job_name: str | None = None,
    calculated_at: datetime.datetime | None = None,
) -> dict[str, Any]:
    """Build the FQ-factor status record from one aggregate row.

    Pure counterpart of refresh_fq_factor_status.
    """
    calculated_at = calculated_at or datetime.datetime.now()
    data_count = int(stats_row.get("data_count", 0)) if stats_row else 0
    latest_data_date = stats_row.get("latest_data_date") if stats_row else None
    expected_count = quote_status.data_count if quote_status else None
    status = STATUS_OK
    status_reason = None
    if data_count <= 0:
        status = STATUS_NO_DATA
        status_reason = "no_source_data"
    elif quote_status and _date_is_before(
        latest_data_date, quote_status.latest_data_date
    ):
        status = STATUS_STALE
        status_reason = "behind_daily_quote"

    record = {
        "code": code,
        "object_type": object_type,
        "asset_type": ASSET_TYPE_FACTOR,
        "asset_name": ASSET_FQ_FACTOR,
        "first_data_date": stats_row.get("first_data_date") if stats_row else None,
        "latest_data_date": latest_data_date,
        "data_count": data_count,
        "expected_count": expected_count,
        "coverage_rate": round(data_count / expected_count * 100, 2)
        if expected_count
        else None,
        "status": status,
        "status_reason": status_reason,
        "last_calculated_at": calculated_at,
        "last_job_name": last_job_name,
        "error_message": None,
        "extra": {},
    }
    if status == STATUS_OK:
        record["last_success_at"] = calculated_at
    return record


def build_ma_status_record(
    *,
    code: str,
    object_type: str,
    stats_row: dict[str, Any] | None,
    quote_status: Any | None = None,
    window: int,
    last_job_name: str | None = None,
    calculated_at: datetime.datetime | None = None,
) -> dict[str, Any]:
    """Build one MA-window status record from one aggregate row.

    Pure counterpart of refresh_ma_factor_status.
    """
    calculated_at = calculated_at or datetime.datetime.now()
    quote_count = int(getattr(quote_status, "data_count", 0) or 0)
    expected_count = max(quote_count - window + 1, 0)

    data_count = int(stats_row.get("data_count", 0)) if stats_row else 0
    latest_data_date = stats_row.get("latest_data_date") if stats_row else None
    status = STATUS_OK
    status_reason = None
    if quote_count < window:
        status = STATUS_NOT_APPLICABLE
        status_reason = "insufficient_quote_history"
    elif data_count <= 0:
        status = STATUS_NO_DATA
        status_reason = "no_source_data"
    elif quote_status and _date_is_before(
        latest_data_date, quote_status.latest_data_date
    ):
        status = STATUS_STALE
        status_reason = "behind_daily_quote"

    record = {
        "code": code,
        "object_type": object_type,
        "asset_type": ASSET_TYPE_FACTOR,
        "asset_name": f"MA_{window}",
        "first_data_date": stats_row.get("first_data_date") if stats_row else None,
        "latest_data_date": latest_data_date,
        "data_count": data_count,
        "expected_count": expected_count or None,
        "coverage_rate": round(data_count / expected_count * 100, 2)
        if expected_count
        else None,
        "status": status,
        "status_reason": status_reason,
        "last_calculated_at": calculated_at,
        "last_job_name": last_job_name,
        "error_message": None,
        "extra": {"window": window},
    }
    if status == STATUS_OK:
        record["last_success_at"] = calculated_at
    return record


def bulk_upsert_asset_status(
    records: list[dict[str, Any]],
    *,
    collection: Any | None = None,
    chunk_size: int = STATUS_UPSERT_CHUNK_SIZE,
) -> int:
    """Upsert many status records with chunked bulk_write.

    Each record must carry the four unique-key fields plus every field that
    should be written (mongo document field names). Records with duplicate
    keys are deduplicated (last one wins). Duplicate-key write errors are
    tolerated as idempotent no-ops; any other write error raises (fail-closed,
    matching the per-record update_one semantics).

    Returns the number of upserted + modified documents reported by MongoDB.
    """
    if not records:
        return 0
    if collection is None:
        collection = DataAssetStatus._get_collection()

    deduped: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for record in records:
        key = (
            record["code"],
            record["object_type"],
            record["asset_type"],
            record["asset_name"],
        )
        deduped[key] = record

    operations = [
        UpdateOne(
            {
                "code": record["code"],
                "object_type": record["object_type"],
                "asset_type": record["asset_type"],
                "asset_name": record["asset_name"],
            },
            {"$set": record},
            upsert=True,
        )
        for record in deduped.values()
    ]

    written = 0
    for start in range(0, len(operations), chunk_size):
        chunk = operations[start : start + chunk_size]
        try:
            result = collection.bulk_write(chunk, ordered=False)
            written += int(getattr(result, "upserted_count", 0) or 0) + int(
                getattr(result, "modified_count", 0) or 0
            )
        except Exception as error:  # noqa: BLE001
            # pymongo and mongoengine surface duplicate-key failures through
            # different exception shapes; is_duplicate_only_bulk_write_error
            # understands both. Anything else is fatal (fail-closed).
            if is_duplicate_only_bulk_write_error(error):
                logger.warning(
                    "data_asset_status bulk upsert hit duplicate-only write "
                    "errors; treating as idempotent no-op: %s",
                    error,
                )
                continue
            logger.error("data_asset_status bulk upsert failed: %s", error)
            raise
    return written


def refresh_quote_status(
    *,
    stock_obj: Any,
    quote_model: Any,
    last_job_name: str | None = None,
    expected_latest_date=None,
    trade_calendar=None,
) -> dict[str, Any]:
    row = aggregate_stats(
        quote_model._get_collection(),
        match={"code": stock_obj.code},
        code_field="code",
    )
    record = build_quote_status_record(
        code=stock_obj.code,
        object_type=getattr(stock_obj, "object_type", "individual_stock"),
        stats_row=row,
        expected_latest_date=expected_latest_date,
        trade_calendar=trade_calendar,
        last_job_name=last_job_name,
    )
    _upsert_record(record)
    return {
        "status": record["status"],
        "status_reason": record["status_reason"],
        "data_count": record["data_count"],
        "expected_count": record["expected_count"],
        "latest_data_date": record["latest_data_date"],
    }


def refresh_fq_factor_status(
    *,
    stock_obj: Any,
    quote_model: Any,
    last_job_name: str | None = None,
) -> None:
    quote_status = read_asset_status(
        stock_obj.code,
        getattr(stock_obj, "object_type", "individual_stock"),
        ASSET_TYPE_QUOTE,
        ASSET_DAILY_QUOTE,
    )
    row = aggregate_stats(
        quote_model._get_collection(),
        match={"code": stock_obj.code, "fq_factor": {"$exists": True, "$ne": None}},
        code_field="code",
    )
    record = build_fq_status_record(
        code=stock_obj.code,
        object_type=getattr(stock_obj, "object_type", "individual_stock"),
        stats_row=row,
        quote_status=quote_status,
        last_job_name=last_job_name,
    )
    _upsert_record(record)


def refresh_ma_factor_status(
    *,
    stock_obj: Any,
    factor_model: Any,
    window: int,
    quote_status: Any | None = None,
    last_job_name: str | None = None,
) -> None:
    object_type = getattr(stock_obj, "object_type", "individual_stock")
    quote_status = quote_status or read_asset_status(
        stock_obj.code, object_type, ASSET_TYPE_QUOTE, ASSET_DAILY_QUOTE
    )
    row = aggregate_stats(
        factor_model._get_collection(),
        match={
            "stock_code": stock_obj.code,
            f"ma_{window}": {"$exists": True, "$ne": None},
        },
        code_field="stock_code",
    )
    record = build_ma_status_record(
        code=stock_obj.code,
        object_type=object_type,
        stats_row=row,
        quote_status=quote_status,
        window=window,
        last_job_name=last_job_name,
    )
    _upsert_record(record)
