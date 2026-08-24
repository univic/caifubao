from __future__ import annotations

import datetime
from typing import Any

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
    coverage_rate = (
        round(data_count / expected_count * 100, 2) if expected_count else None
    )
    update_fields = {
        "set__first_data_date": first_data_date,
        "set__latest_data_date": latest_data_date,
        "set__data_count": data_count,
        "set__expected_count": expected_count,
        "set__coverage_rate": coverage_rate,
        "set__status": status,
        "set__status_reason": status_reason,
        "set__last_calculated_at": calculated_at,
        "set__last_job_name": last_job_name,
        "set__error_message": error_message,
        "set__extra": extra or {},
    }
    if status == STATUS_OK:
        update_fields["set__last_success_at"] = calculated_at

    DataAssetStatus.objects(
        code=code,
        object_type=object_type,
        asset_type=asset_type,
        asset_name=asset_name,
    ).update_one(
        upsert=True,
        set__code=code,
        set__object_type=object_type,
        set__asset_type=asset_type,
        set__asset_name=asset_name,
        **update_fields,
    )


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
    data_count = int(row.get("data_count", 0)) if row else 0
    latest_data_date = row.get("latest_data_date") if row else None
    first_data_date = row.get("first_data_date") if row else None
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
    upsert_asset_status(
        code=stock_obj.code,
        object_type=getattr(stock_obj, "object_type", "individual_stock"),
        asset_type=ASSET_TYPE_QUOTE,
        asset_name=ASSET_DAILY_QUOTE,
        first_data_date=first_data_date,
        latest_data_date=latest_data_date,
        data_count=data_count,
        expected_count=expected_count,
        status=status,
        status_reason=status_reason,
        last_job_name=last_job_name,
        extra={"expected_latest_date": expected_latest_date}
        if expected_latest_date
        else None,
    )
    return {
        "status": status,
        "status_reason": status_reason,
        "data_count": data_count,
        "expected_count": expected_count,
        "latest_data_date": latest_data_date,
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
    data_count = int(row.get("data_count", 0)) if row else 0
    latest_data_date = row.get("latest_data_date") if row else None
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

    upsert_asset_status(
        code=stock_obj.code,
        object_type=getattr(stock_obj, "object_type", "individual_stock"),
        asset_type=ASSET_TYPE_FACTOR,
        asset_name=ASSET_FQ_FACTOR,
        first_data_date=row.get("first_data_date") if row else None,
        latest_data_date=latest_data_date,
        data_count=data_count,
        expected_count=quote_status.data_count if quote_status else None,
        status=status,
        status_reason=status_reason,
        last_job_name=last_job_name,
    )


def refresh_ma_factor_status(
    *,
    stock_obj: Any,
    factor_model: Any,
    window: int,
    quote_status: Any | None = None,
    last_job_name: str | None = None,
) -> None:
    asset_name = f"MA_{window}"
    field_name = f"ma_{window}"
    object_type = getattr(stock_obj, "object_type", "individual_stock")
    quote_status = quote_status or read_asset_status(
        stock_obj.code, object_type, ASSET_TYPE_QUOTE, ASSET_DAILY_QUOTE
    )
    quote_count = int(getattr(quote_status, "data_count", 0) or 0)
    expected_count = max(quote_count - window + 1, 0)

    row = aggregate_stats(
        factor_model._get_collection(),
        match={
            "stock_code": stock_obj.code,
            field_name: {"$exists": True, "$ne": None},
        },
        code_field="stock_code",
    )
    data_count = int(row.get("data_count", 0)) if row else 0
    latest_data_date = row.get("latest_data_date") if row else None
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

    upsert_asset_status(
        code=stock_obj.code,
        object_type=object_type,
        asset_type=ASSET_TYPE_FACTOR,
        asset_name=asset_name,
        first_data_date=row.get("first_data_date") if row else None,
        latest_data_date=latest_data_date,
        data_count=data_count,
        expected_count=expected_count or None,
        status=status,
        status_reason=status_reason,
        last_job_name=last_job_name,
        extra={"window": window},
    )
