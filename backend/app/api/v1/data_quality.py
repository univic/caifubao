# -*- coding: utf-8 -*-
# Data quality APIs backed by data_asset_status.

import datetime
import logging
import threading
import time
from zoneinfo import ZoneInfo

from flask import Blueprint, jsonify, request

from app.model.data_asset_status import (
    STATUS_NO_DATA,
    STATUS_NOT_APPLICABLE,
    STATUS_OK,
    STATUS_STALE,
    DataAssetStatus,
)
from app.model.stock import FinanceMarket, IndividualStock
from app.utilities import data_capability_helper
from app.utilities.trading_day_helper import (
    determine_most_recent_previous_complete_trading_day,
)

data_quality_bp = Blueprint("data_quality", __name__, url_prefix="/api/data-quality")
logger = logging.getLogger(__name__)

MA_FACTOR_NAMES = ("MA_10", "MA_20", "MA_30", "MA_60", "MA_120")
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200
APP_TIMEZONE = ZoneInfo("Asia/Shanghai")
ITEMS_CACHE_TTL_SECONDS = 300
STATUS_BLOCKED_BY_QUOTE = "BLOCKED_BY_QUOTE"
TARGET_META_TYPES = {
    "daily_quote": "quote",
    "FQ_FACTOR": "factor",
    **{factor_name: "factor" for factor_name in MA_FACTOR_NAMES},
}
_items_cache_lock = threading.RLock()
_items_cache = {}


def _now_in_app_timezone():
    current = datetime.datetime.now(APP_TIMEZONE)
    if current.tzinfo is None:
        return current.replace(tzinfo=APP_TIMEZONE)
    return current


def _format_datetime(value):
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _date_key(value):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    text = str(value)
    try:
        return datetime.date.fromisoformat(text[:10])
    except ValueError:
        return text


def _parse_int(value, default, minimum=0, maximum=MAX_PAGE_SIZE):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(min(parsed, maximum), minimum)


def _serialize_stock(stock):
    return {
        "code": stock.code,
        "name": stock.name,
        "object_type": getattr(stock, "object_type", "individual_stock"),
        "active_status": getattr(stock, "active_status", None),
    }


def _is_data_quality_supported_stock(stock):
    return data_capability_helper.supports_all(stock)


def _load_active_stocks(query_text=""):
    query = IndividualStock.objects(active_status=0)
    query_text = (query_text or "").strip()
    if query_text:
        normalized = query_text.lower()
        code_matches = list(query.filter(code__icontains=normalized).order_by("code"))
        name_matches = list(
            IndividualStock.objects(
                active_status=0, name__icontains=query_text
            ).order_by("code")
        )
        seen = set()
        result = []
        for stock in [*code_matches, *name_matches]:
            if stock.code in seen:
                continue
            seen.add(stock.code)
            if not _is_data_quality_supported_stock(stock):
                continue
            result.append(stock)
        return result
    return [
        stock
        for stock in query.order_by("code")
        if _is_data_quality_supported_stock(stock)
    ]


def _build_stock_scope():
    stocks = list(
        IndividualStock.objects(active_status=0).only("code", "data_capabilities")
    )
    excluded_unsupported = sum(
        1 for stock in stocks if not _is_data_quality_supported_stock(stock)
    )
    total_active = len(stocks)
    return {
        "total_active": total_active,
        "excluded_unsupported": excluded_unsupported,
        "effective_total": total_active - excluded_unsupported,
        "unsupported_markets": ["BSE"],
    }


def _is_target_asset(row):
    return TARGET_META_TYPES.get(row.asset_name) == row.asset_type


def _load_asset_status_map(codes):
    if not codes:
        return {}
    status_rows = DataAssetStatus.objects(
        code__in=codes,
        asset_name__in=tuple(TARGET_META_TYPES.keys()),
    ).only(
        "code",
        "object_type",
        "asset_type",
        "asset_name",
        "latest_data_date",
        "data_count",
        "status",
        "status_reason",
        "last_calculated_at",
    )

    status_map = {}
    for row in status_rows:
        if not _is_target_asset(row):
            continue
        status_map[(row.code, row.asset_name)] = row
    return status_map


def _log_elapsed(label, started_at, **fields):
    logger.info(
        "data_quality.%s elapsed=%.3fs %s",
        label,
        time.perf_counter() - started_at,
        " ".join(f"{key}={value}" for key, value in fields.items()),
    )


def _asset_latest_date(row):
    return getattr(row, "latest_data_date", None) if row else None


def _asset_status(row):
    return getattr(row, "status", None) if row else None


def _quality_status_from_asset(row):
    status = _asset_status(row)
    if status == STATUS_OK:
        return "OK"
    if status == STATUS_STALE:
        return "STALE"
    if status == STATUS_NOT_APPLICABLE:
        return STATUS_NOT_APPLICABLE
    if status == "AHEAD":
        return "AHEAD"
    if status == STATUS_NO_DATA or status is None:
        return "MISSING"
    return "MISSING"


def _resolve_expected_quote_date(current_dt=None):
    current_dt = current_dt or _now_in_app_timezone()
    if getattr(current_dt, "tzinfo", None) is not None:
        current_dt = current_dt.astimezone(APP_TIMEZONE).replace(tzinfo=None)
    market = FinanceMarket.objects(name="ChinaAStock").first()
    trade_calendar = market.trade_calendar if market else []
    if not trade_calendar:
        return None
    return determine_most_recent_previous_complete_trading_day(
        trade_calendar, current_dt
    )


def _quote_status_from_asset(row, expected_quote_date=None):
    status = _quality_status_from_asset(row)
    if status != "OK" or expected_quote_date is None:
        return status

    quote_date = _date_key(_asset_latest_date(row))
    expected_date = _date_key(expected_quote_date)
    if quote_date is None or expected_date is None:
        return status
    if quote_date < expected_date:
        return "STALE"
    if quote_date > expected_date:
        return "AHEAD"
    return status


def _quality_status_from_factor_statuses(factor_statuses):
    effective_statuses = [
        status for status in factor_statuses if status != STATUS_NOT_APPLICABLE
    ]
    if any(value == "AHEAD" for value in effective_statuses):
        return "AHEAD"
    if any(value == "MISSING" for value in effective_statuses):
        return "MISSING"
    if any(value == "STALE" for value in effective_statuses):
        return "STALE"
    return "OK"


def _is_quote_blocked(quote_status):
    return quote_status != "OK"


def _apply_quote_block_to_factor_status(status, quote_blocked):
    if status == STATUS_NOT_APPLICABLE or not quote_blocked:
        return status
    return STATUS_BLOCKED_BY_QUOTE


def _build_quality_item(stock, asset_status_map, expected_quote_date=None):
    quote_status = asset_status_map.get((stock.code, "daily_quote"))
    fq_status_row = asset_status_map.get((stock.code, "FQ_FACTOR"))
    quote_dt = _asset_latest_date(quote_status)
    fq_dt = _asset_latest_date(fq_status_row)
    quote_asset_status = _quote_status_from_asset(
        quote_status, expected_quote_date=expected_quote_date
    )
    quote_blocked = _is_quote_blocked(quote_asset_status)

    issues = []
    ma_dates = {}
    ma_statuses = {}

    if quote_asset_status == "MISSING":
        issues.append("quote missing")
    elif quote_asset_status != "OK":
        issues.append(f"quote {quote_asset_status.lower()}")

    fq_status = _apply_quote_block_to_factor_status(
        _quality_status_from_asset(fq_status_row), quote_blocked
    )
    if fq_status not in ("OK", STATUS_NOT_APPLICABLE, STATUS_BLOCKED_BY_QUOTE):
        issues.append(f"FQ_FACTOR {fq_status.lower()}")

    for factor_name in MA_FACTOR_NAMES:
        ma_status_row = asset_status_map.get((stock.code, factor_name))
        ma_dt = _asset_latest_date(ma_status_row)
        ma_dates[factor_name] = _format_datetime(ma_dt)
        ma_status = _apply_quote_block_to_factor_status(
            _quality_status_from_asset(ma_status_row), quote_blocked
        )
        ma_statuses[factor_name] = ma_status
        if ma_status not in ("OK", STATUS_NOT_APPLICABLE, STATUS_BLOCKED_BY_QUOTE):
            issues.append(f"{factor_name} {ma_status.lower()}")

    if quote_asset_status != "OK":
        status = quote_asset_status
    else:
        status = _quality_status_from_factor_statuses(
            [fq_status, *ma_statuses.values()]
        )

    return {
        **_serialize_stock(stock),
        "quote_date": _format_datetime(quote_dt),
        "quote_status": quote_asset_status,
        "fq_factor_date": _format_datetime(fq_dt),
        "fq_factor_status": fq_status,
        "ma_dates": ma_dates,
        "ma_statuses": ma_statuses,
        "status": status,
        "issues": issues,
    }


def _status_matches(item_status, requested_status):
    if requested_status in ("", "all"):
        return True
    if requested_status == "abnormal":
        return item_status != "OK"
    return item_status == requested_status.upper()


def _count_status(items):
    counts = {"OK": 0, "STALE": 0, "MISSING": 0, "AHEAD": 0}
    for item in items:
        counts[item["status"]] = counts.get(item["status"], 0) + 1
    return counts


def _build_factor_coverage(items, field):
    total = 0
    ok = missing = stale = ahead = 0
    blocked = 0
    for item in items:
        if item["quote_status"] != "OK":
            blocked += 1
            continue

        total += 1
        if field == "fq_factor_date":
            statuses = [item["fq_factor_status"]]
        else:
            statuses = [
                status
                for status in item["ma_statuses"].values()
                if status != STATUS_NOT_APPLICABLE
            ]

        if all(status == "OK" for status in statuses):
            ok += 1
        elif any(status == "AHEAD" for status in statuses):
            ahead += 1
        elif any(status == "MISSING" for status in statuses):
            missing += 1
        elif any(status == STATUS_BLOCKED_BY_QUOTE for status in statuses):
            blocked += 1
        else:
            stale += 1

    return {
        "total": total,
        "ok": ok,
        "missing": missing,
        "stale": stale,
        "ahead": ahead,
        "blocked": blocked,
        "ok_rate": round((ok / total * 100) if total else 0, 2),
    }


def _build_quote_coverage(items):
    total = len(items)
    ok = missing = stale = ahead = 0
    for item in items:
        quote_status = item["quote_status"]
        if quote_status == "OK":
            ok += 1
        elif quote_status == "AHEAD":
            ahead += 1
        elif quote_status == "STALE":
            stale += 1
        else:
            missing += 1
    return {
        "total": total,
        "ok": ok,
        "missing": missing,
        "stale": stale,
        "ahead": ahead,
        "blocked": 0,
        "ok_rate": round((ok / total * 100) if total else 0, 2),
    }


def _build_items(query_text=""):
    started_at = time.perf_counter()
    stocks = _load_active_stocks(query_text)
    _log_elapsed(
        "load_active_stocks", started_at, query=repr(query_text), count=len(stocks)
    )

    stage_started_at = time.perf_counter()
    codes = [stock.code for stock in stocks]
    asset_status_map = _load_asset_status_map(codes)
    _log_elapsed(
        "load_asset_status_map",
        stage_started_at,
        query=repr(query_text),
        count=len(asset_status_map),
    )

    stage_started_at = time.perf_counter()
    expected_quote_date = _resolve_expected_quote_date()
    items = [
        _build_quality_item(stock, asset_status_map, expected_quote_date)
        for stock in stocks
    ]
    sorted_items = sorted(
        items, key=lambda item: (item["status"] == "OK", item["code"])
    )
    _log_elapsed(
        "build_items", stage_started_at, query=repr(query_text), count=len(items)
    )
    _log_elapsed(
        "build_items_total", started_at, query=repr(query_text), count=len(items)
    )
    return sorted_items


def _get_cached_items(query_text=""):
    cache_key = (query_text or "").strip()
    now = time.monotonic()
    with _items_cache_lock:
        cached = _items_cache.get(cache_key)
        if cached and cached["expires_at"] > now:
            logger.info(
                "data_quality.items_cache hit query=%r count=%s",
                cache_key,
                len(cached["items"]),
            )
            return cached["items"]

        logger.info("data_quality.items_cache miss query=%r", cache_key)
        items = _build_items(cache_key)
        _items_cache[cache_key] = {
            "items": items,
            "expires_at": time.monotonic() + ITEMS_CACHE_TTL_SECONDS,
        }
        return items


def _clear_items_cache():
    with _items_cache_lock:
        _items_cache.clear()


def _build_summary():
    items = _get_cached_items()
    status_counts = _count_status(items)
    total = len(items)
    ok_rate = round((status_counts.get("OK", 0) / total * 100) if total else 0, 2)
    if ok_rate >= 98:
        status = "OK"
    elif ok_rate >= 90:
        status = "WARN"
    else:
        status = "ERROR"

    latest_quote_date = max(
        (_date_key(item["quote_date"]) for item in items if item["quote_date"]),
        default=None,
    )
    return {
        "status": status,
        "generated_at": _now_in_app_timezone().isoformat(),
        "latest_quote_date": _format_datetime(latest_quote_date),
        "expected_quote_date": _format_datetime(_resolve_expected_quote_date()),
        "scope": _build_stock_scope(),
        "coverage": {
            "overall": {
                "total": total,
                "ok": status_counts.get("OK", 0),
                "missing": status_counts.get("MISSING", 0),
                "stale": status_counts.get("STALE", 0),
                "ahead": status_counts.get("AHEAD", 0),
                "blocked": 0,
                "ok_rate": ok_rate,
            },
            "quote": _build_quote_coverage(items),
            "fq_factor": _build_factor_coverage(items, "fq_factor_date"),
            "ma_factor": _build_factor_coverage(items, "ma_dates"),
        },
    }


@data_quality_bp.route("/summary", methods=["GET"])
def get_data_quality_summary():
    return jsonify(_build_summary()), 200


@data_quality_bp.route("/items", methods=["GET"])
def get_data_quality_items():
    query_text = request.args.get("q", "")
    requested_status = (request.args.get("status") or "abnormal").lower()
    limit = _parse_int(request.args.get("limit"), DEFAULT_PAGE_SIZE, 1, MAX_PAGE_SIZE)
    offset = _parse_int(request.args.get("offset"), 0, 0, 100000)

    items = [
        item
        for item in _get_cached_items(query_text)
        if _status_matches(item["status"], requested_status)
    ]
    return jsonify(
        {
            "total": len(items),
            "limit": limit,
            "offset": offset,
            "items": items[offset : offset + limit],
        }
    ), 200
