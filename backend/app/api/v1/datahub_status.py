# -*- coding: utf-8 -*-
# Datahub status API Blueprint - quick data asset / quote overview

import datetime
import threading
import time
from flask import Blueprint, jsonify
from mongoengine import get_db

from app.model.data_asset_status import DataAssetStatus
from app.model.stock import FinanceMarket, IndividualStock, StockIndex
from app.utilities import data_capability_helper
from app.utilities.trading_day_helper import (
    determine_most_recent_previous_complete_trading_day,
    determine_pervious_trading_day,
)

datahub_status_bp = Blueprint("datahub_status", __name__, url_prefix="/api/datahub")
STATUS_CACHE_TTL_SECONDS = 60
_status_cache_lock = threading.RLock()
_status_cache = {"expires_at": 0, "payload": None}

# ── pipeline job definitions for structured status reporting ──────────
# Each entry: (job_family, job_name, label, is_critical)
# is_critical=True means failure here blocks downstream and degrades freshness.
_PIPELINE_JOBS = [
    ("quote_daily", "datahub_quote_index_daily", "指数行情拉取", True),
    ("quote_daily", "datahub_quote_stock_daily", "个股行情拉取", True),
    ("signal_daily", "datahub_signal_daily", "信号生成", True),
    ("scoring_daily", "datahub_scoring_daily", "评分计算", True),
]

# ── freshness grade constants ─────────────────────────────────────────
GRADE_FRESH = "FRESH"
GRADE_STALE = "STALE"
GRADE_EXPIRED = "EXPIRED"
GRADE_ERROR = "ERROR"
GRADE_NO_DATA = "NO_DATA"


def _format_datetime(value):
    if value is None:
        return None
    return value.isoformat()


def _get_codes(stock_model):
    if stock_model == IndividualStock:
        # 只返回支持 daily_quote 的活跃股票（排除北交所等无行情能力的股票）
        stocks = stock_model.objects(active_status=0).only("code", "data_capabilities")
        return [
            s.code
            for s in stocks
            if data_capability_helper.stock_supports(s, "daily_quote")
        ]
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


def _get_latest_asset_status(object_type, codes=None):
    status_qs = DataAssetStatus.objects(
        object_type=object_type, asset_type="quote", asset_name="daily_quote"
    )
    if codes is not None:
        status_qs = status_qs.filter(code__in=codes)
    latest = status_qs.order_by("-latest_data_date").first()
    latest_date = latest.latest_data_date if latest else None
    return latest_date, status_qs.count()


def _get_quote_status_data_count(object_type, codes=None):
    pipeline = [
        {
            "$match": {
                "object_type": object_type,
                "asset_type": "quote",
                "asset_name": "daily_quote",
            }
        },
    ]
    if codes is not None:
        pipeline[0]["$match"]["code"] = {"$in": codes}
    pipeline.append({"$group": {"_id": None, "quote_count": {"$sum": "$data_count"}}})
    result = DataAssetStatus.objects.aggregate(*pipeline)
    row = next(iter(result), None)
    return row.get("quote_count", 0) if row else 0


def _build_category_status(stock_model, object_type, reference_dates):
    codes = _get_codes(stock_model)
    latest_asset_status_date, asset_status_records_count = _get_latest_asset_status(
        object_type, codes=codes
    )
    latest_quote_date = latest_asset_status_date
    latest_quote_count = _get_quote_status_data_count(object_type, codes=codes)
    latest_complete_trading_day = reference_dates["latest_complete_trading_day"]
    previous_complete_trading_day = reference_dates["previous_complete_trading_day"]

    status_qs = DataAssetStatus.objects(
        object_type=object_type, asset_type="quote", asset_name="daily_quote"
    ).only("code", "latest_data_date")
    if codes is not None:
        status_qs = status_qs.filter(code__in=codes)
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


def _get_pipeline_status(
    trading_day_start: datetime.datetime,
) -> dict:
    """Return structured pipeline status for all key job families.

    For each job family, queries the latest job run within the trading day
    window (any status), exposing FAILED / SKIPPED / RUNNING / NONE in
    addition to the legacy SUCCESS boolean so downstream reports can
    distinguish root causes from symptoms.
    """
    if trading_day_start is None:
        return {
            "jobs": {},
            "overall_healthy": False,
            "summary": "NO_TRADING_DAY",
            "signal_run_today": False,
            "scoring_run_today": False,
        }

    trading_day_end = trading_day_start + datetime.timedelta(days=1)
    db = get_db()

    jobs = {}
    critical_failed = 0
    critical_success = 0
    critical_total = 0

    for family, name, label, is_critical in _PIPELINE_JOBS:
        runs = list(
            db.datahub_job_runs.find(
                {
                    "job_family": family,
                    "job_name": name,
                    "started_at": {"$gte": trading_day_start, "$lt": trading_day_end},
                }
            )
            .sort("started_at", -1)
            .limit(1)
        )

        latest = runs[0] if runs else None
        entry = {"label": label}

        if latest is not None:
            status = latest.get("status", "UNKNOWN")
            entry["status"] = status
            entry["started_at"] = _format_datetime(latest.get("started_at"))
            entry["completed_at"] = _format_datetime(latest.get("completed_at"))
            entry["error_message"] = latest.get("error_message")
            summary = latest.get("summary") or {}
            entry["skipped_reason"] = summary.get("reason")
            entry["dependency_job_family"] = summary.get("dependency_job_family")
            entry["pulled_total"] = latest.get("pulled_total", 0)
            entry["written_total"] = latest.get("written_total", 0)
            entry["failed_phase"] = latest.get("failed_phase")
        else:
            entry["status"] = "NONE"
            entry["started_at"] = None
            entry["completed_at"] = None
            entry["error_message"] = None
            entry["skipped_reason"] = None
            entry["dependency_job_family"] = None
            entry["pulled_total"] = 0
            entry["written_total"] = 0
            entry["failed_phase"] = None

        jobs[family] = entry

        if is_critical:
            critical_total += 1
            if latest is not None and latest.get("status") == "SUCCESS":
                critical_success += 1
            elif latest is not None and latest.get("status") == "FAILED":
                critical_failed += 1

    # Build summary string for quick consumption
    if critical_total > 0:
        if critical_success == critical_total:
            summary = "ALL_JOBS_SUCCESS"
            overall_healthy = True
        elif critical_failed > 0:
            summary = "CRITICAL_FAILURE"
            overall_healthy = False
        elif critical_success > 0:
            summary = "PARTIAL_SUCCESS"
            overall_healthy = False
        else:
            summary = "NO_JOBS_RUN"
            overall_healthy = False
    else:
        summary = "NO_CRITICAL_JOBS"
        overall_healthy = True

    # Legacy boolean fields for backward compatibility
    signal_run = jobs.get("signal_daily", {}).get("status") == "SUCCESS"
    scoring_run = jobs.get("scoring_daily", {}).get("status") == "SUCCESS"

    return {
        "jobs": jobs,
        "overall_healthy": overall_healthy,
        "summary": summary,
        "signal_run_today": signal_run,
        "scoring_run_today": scoring_run,
    }


def _compute_freshness_grade(
    stock_category: dict,
    pipeline_status: dict,
    reference_dates: dict,
) -> dict:
    """Compute an overall freshness grade for the data pipeline.

    Combines pipeline health with data coverage/staleness to produce a
    single actionable grade: FRESH / STALE / EXPIRED / ERROR / NO_DATA.
    """
    latest_trading_day = reference_dates.get("latest_complete_trading_day")
    if latest_trading_day is None:
        return {"grade": GRADE_NO_DATA, "reason": "无法确定最新交易日"}

    overall_healthy = pipeline_status.get("overall_healthy", False)
    total = stock_category.get("total_count", 0)
    up_to_date = stock_category.get("up_to_date_count", 0)
    no_data = stock_category.get("no_data_count", 0)

    # Determine staleness from the stock category's latest quote date
    latest_quote_date_str = stock_category.get("latest_quote_date")
    trading_days_behind = 0
    if latest_quote_date_str is not None:
        try:
            quote_date = datetime.datetime.fromisoformat(latest_quote_date_str).date()
        except (ValueError, TypeError):
            quote_date = None
    else:
        quote_date = None

    # Count trading days behind using reference dates
    if quote_date is not None:
        latest_date = _date_from_dt(latest_trading_day)
        # Simple date difference as approximation (actual trading day diff
        # requires calendar data — upstream consumers can compute that)
        if latest_date is not None:
            cal_days_behind = (latest_date - quote_date).days
            trading_days_behind = max(cal_days_behind, 0)

    # Grade rules (ordered by priority)
    if total == 0 or no_data == total:
        return {
            "grade": GRADE_NO_DATA,
            "reason": "无可用行情数据",
            "details": {
                "trading_days_behind": None,
                "quote_date": latest_quote_date_str,
                "trading_day": _format_datetime(latest_trading_day),
                "up_to_date_ratio": 0,
            },
        }

    # Pipeline completely failed with stale data
    if not overall_healthy:
        # Check if any jobs have error messages
        failed_jobs = [
            v["label"]
            for v in pipeline_status.get("jobs", {}).values()
            if v.get("status") == "FAILED"
        ]
        skipped_jobs = [
            v["label"]
            for v in pipeline_status.get("jobs", {}).values()
            if v.get("status") in ("SKIPPED", "NONE")
        ]

        reasons = []
        if failed_jobs:
            reasons.append(f"流水线失败: {', '.join(failed_jobs)}")
        if skipped_jobs:
            reasons.append(f"未运行: {', '.join(skipped_jobs)}")

        return {
            "grade": GRADE_ERROR if failed_jobs else GRADE_STALE,
            "reason": "; ".join(reasons) if reasons else "流水线状态异常",
            "details": {
                "trading_days_behind": trading_days_behind,
                "quote_date": latest_quote_date_str,
                "trading_day": _format_datetime(latest_trading_day),
                "up_to_date_ratio": round(up_to_date / total, 4) if total > 0 else 0,
            },
        }

    # Pipeline healthy — grade by data staleness
    up_to_date_ratio = round(up_to_date / total, 4) if total > 0 else 0

    if trading_days_behind == 0 and up_to_date_ratio >= 0.95:
        grade = GRADE_FRESH
        reason = "数据新鲜，流水线正常"
    elif trading_days_behind <= 2:
        grade = GRADE_STALE
        reason = f"数据滞后约{trading_days_behind}个自然日"
    else:
        grade = GRADE_EXPIRED
        reason = f"数据过期，滞后约{trading_days_behind}个自然日"

    return {
        "grade": grade,
        "reason": reason,
        "details": {
            "trading_days_behind": trading_days_behind,
            "quote_date": latest_quote_date_str,
            "trading_day": _format_datetime(latest_trading_day),
            "up_to_date_ratio": up_to_date_ratio,
        },
    }


def _build_status_payload():
    reference_dates = _resolve_reference_dates()
    latest_trading_day = reference_dates["latest_complete_trading_day"]
    today_start = None
    if latest_trading_day is not None:
        if isinstance(latest_trading_day, datetime.datetime):
            today_start = latest_trading_day.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        else:
            today_start = datetime.datetime.combine(
                latest_trading_day, datetime.time.min
            )

    pipeline_status = _get_pipeline_status(today_start)

    stock_category = _build_category_status(
        IndividualStock, "individual_stock", reference_dates
    )
    freshness = _compute_freshness_grade(
        stock_category, pipeline_status, reference_dates
    )

    return {
        "generated_at": datetime.datetime.now().isoformat(),
        "reference_dates": {
            "latest_complete_trading_day": _format_datetime(latest_trading_day),
            "previous_complete_trading_day": _format_datetime(
                reference_dates["previous_complete_trading_day"]
            ),
        },
        "index": _build_category_status(StockIndex, "stock_index", reference_dates),
        "stock": stock_category,
        "pipeline": pipeline_status,
        "freshness": freshness,
        # Legacy top-level boolean fields (backward compatible)
        "signal_run_today": pipeline_status["signal_run_today"],
        "scoring_run_today": pipeline_status["scoring_run_today"],
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
