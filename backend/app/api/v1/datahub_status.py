# -*- coding: utf-8 -*-
# Datahub status API Blueprint - quick data asset / quote overview

import datetime
import threading
import time
from flask import Blueprint, jsonify

from app.model.data_asset_status import DataAssetStatus
from app.model.stock import FinanceMarket, IndividualStock, StockIndex
from app.utilities.trading_day_helper import (
    determine_most_recent_previous_complete_trading_day,
    determine_pervious_trading_day,
)

datahub_status_bp = Blueprint("datahub_status", __name__, url_prefix="/api/datahub")
STATUS_CACHE_TTL_SECONDS = 60
_status_cache_lock = threading.RLock()
_status_cache = {"expires_at": 0, "payload": None}


def _format_datetime(value):
    if value is None:
        return None
    return value.isoformat()


def _get_codes(stock_model):
    return list(stock_model.objects.scalar("code"))


def _date_from_dt(value):
    if value is None:
        return None
    # Keep this check resilient even when tests replace datetime.datetime.
    if hasattr(value, "date") and hasattr(value, "hour"):
        return value.date()
    return value


def _resolve_reference_dates(current_dt=None):
    current_dt = current_dt or datetime.datetime.now()
    market = FinanceMarket.objects(name="ChinaAStock").first()
    trade_calendar = market.trade_calendar if market else []
    latest_complete_trading_day = (
        determine_most_recent_previous_complete_trading_day(trade_calendar, current_dt)
        if trade_calendar
        else None
    )
    previous_complete_trading_day = None
    if trade_calendar and latest_complete_trading_day:
        try:
            previous_complete_trading_day = determine_pervious_trading_day(
                trade_calendar, latest_complete_trading_day
            )
        except (IndexError, ValueError):
            previous_complete_trading_day = None

    return {
        "latest_complete_trading_day": latest_complete_trading_day,
        "previous_complete_trading_day": previous_complete_trading_day,
    }


def _classify_asset_status(
    latest_data_date, latest_complete_trading_day, previous_complete_trading_day
):
    if not latest_data_date:
        return "no_data"

    asset_date = _date_from_dt(latest_data_date)
    latest_date = _date_from_dt(latest_complete_trading_day)
    previous_date = _date_from_dt(previous_complete_trading_day)

    if latest_date and asset_date >= latest_date:
        return "up_to_date"
    if previous_date and asset_date == previous_date:
        return "lag_1_day"
    return "expired"


def _get_latest_asset_status(object_type):
    status_qs = DataAssetStatus.objects(
        object_type=object_type, asset_type="quote", asset_name="daily_quote"
    )
    latest = status_qs.order_by("-latest_data_date").first()
    latest_date = latest.latest_data_date if latest else None
    return latest_date, status_qs.count()


def _get_quote_status_data_count(object_type):
    result = DataAssetStatus.objects(
        object_type=object_type, asset_type="quote", asset_name="daily_quote"
    ).aggregate(
        [
            {
                "$group": {
                    "_id": None,
                    "quote_count": {"$sum": "$data_count"},
                }
            }
        ]
    )
    row = next(iter(result), None)
    return row.get("quote_count", 0) if row else 0


def _build_category_status(stock_model, object_type, reference_dates):
    codes = _get_codes(stock_model)
    latest_asset_status_date, asset_status_records_count = _get_latest_asset_status(
        object_type
    )
    latest_quote_date = latest_asset_status_date
    latest_quote_count = _get_quote_status_data_count(object_type)
    latest_complete_trading_day = reference_dates["latest_complete_trading_day"]
    previous_complete_trading_day = reference_dates["previous_complete_trading_day"]

    status_qs = DataAssetStatus.objects(
        object_type=object_type, asset_type="quote", asset_name="daily_quote"
    ).only("code", "latest_data_date")
    status_map = {doc.code: doc for doc in status_qs}

    counts = {
        "up_to_date": 0,
        "lag_1_day": 0,
        "expired": 0,
        "no_data": 0,
    }
    for code in codes:
        status_doc = status_map.get(code)
        status = _classify_asset_status(
            status_doc.latest_data_date if status_doc else None,
            latest_complete_trading_day,
            previous_complete_trading_day,
        )
        counts[status] += 1

    return {
        "total_count": len(codes),
        "quote_records_count": latest_quote_count,
        "latest_quote_date": _format_datetime(latest_quote_date),
        "asset_status_records_count": asset_status_records_count,
        "latest_asset_status_date": _format_datetime(latest_asset_status_date),
        "freshness_records_count": asset_status_records_count,
        "latest_freshness_date": _format_datetime(latest_asset_status_date),
        "freshness_deprecated": True,
        "up_to_date_count": counts["up_to_date"],
        "lag_1_day_count": counts["lag_1_day"],
        "expired_count": counts["expired"],
        "no_data_count": counts["no_data"],
        "is_up_to_date": (
            latest_complete_trading_day is not None
            and counts["up_to_date"] == len(codes)
        ),
    }


def _build_status_payload():
    reference_dates = _resolve_reference_dates()
    return {
        "generated_at": datetime.datetime.now().isoformat(),
        "reference_dates": {
            "latest_complete_trading_day": _format_datetime(
                reference_dates["latest_complete_trading_day"]
            ),
            "previous_complete_trading_day": _format_datetime(
                reference_dates["previous_complete_trading_day"]
            ),
        },
        "index": _build_category_status(StockIndex, "stock_index", reference_dates),
        "stock": _build_category_status(
            IndividualStock, "individual_stock", reference_dates
        ),
    }


@datahub_status_bp.route("/status", methods=["GET"])
def get_datahub_status():
    """Return a lightweight data asset / latest quote summary."""
    now = time.monotonic()
    with _status_cache_lock:
        if _status_cache["payload"] and _status_cache["expires_at"] > now:
            return jsonify(_status_cache["payload"]), 200

        payload = _build_status_payload()
        _status_cache["payload"] = payload
        _status_cache["expires_at"] = time.monotonic() + STATUS_CACHE_TTL_SECONDS
        return jsonify(payload), 200
