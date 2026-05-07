# -*- coding: utf-8 -*-
# Market overview API Blueprint

import datetime
import logging
import threading
import time

from flask import Blueprint, jsonify, request
from mongoengine import Q

from app.model.data_asset_status import DataAssetStatus
from app.model.stock import IndividualStock, StockIndex, StockDailyQuote
try:
    from app.model.scoring import StockScorePrediction
except ImportError:  # pragma: no cover - compatibility with older dev images
    from app.model.scoring import StockDailyScore as StockScorePrediction

logger = logging.getLogger(__name__)

market_bp = Blueprint("market", __name__, url_prefix="/api/market")

MAJOR_INDEXES = [
    ("sh000001", "上证指数"),
    ("sz399001", "深证成指"),
    ("sz399006", "创业板指"),
    ("sh000688", "科创50"),
]

DAILY_QUOTE_STATUS_FILTER = {
    "asset_type": "quote",
    "asset_name": "daily_quote",
}
OVERVIEW_CACHE_TTL_SECONDS = 60
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200
_overview_cache_lock = threading.RLock()
_overview_cache = {"expires_at": 0, "payload": None}


def _format_datetime(value):
    if value is None:
        return None
    return value.isoformat()


def _to_number(value, default=0.0):
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_horizon(value, default=5):
    try:
        horizon = int(value)
    except (TypeError, ValueError):
        horizon = default
    return horizon if horizon in {5, 20, 60} else default


def _parse_int(value, default, minimum=1, maximum=MAX_PAGE_SIZE):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(min(parsed, maximum), minimum)


def _latest_quote_date_for_object_type(object_type):
    latest = (
        DataAssetStatus.objects(
            object_type=object_type,
            **DAILY_QUOTE_STATUS_FILTER,
        )
        .only("latest_data_date")
        .order_by("-latest_data_date")
        .first()
    )
    return latest.latest_data_date if latest else None


def _latest_quote_date_for_code(code, object_type=None):
    query = {
        "code": code,
        **DAILY_QUOTE_STATUS_FILTER,
    }
    if object_type:
        query["object_type"] = object_type

    status = DataAssetStatus.objects(**query).only("latest_data_date").first()
    if status and status.latest_data_date:
        return status.latest_data_date

    latest_quote = (
        StockDailyQuote.objects(code=code).only("date").order_by("-date").first()
    )
    return latest_quote.date if latest_quote else None


def _serialize_score_summary(score_doc):
    if score_doc is None:
        return {
            "score": 0.0,
            "rank": None,
            "percentile": None,
            "recommendation": "NONE",
            "status": None,
            "verification": {},
            "model_version": None,
        }
    return {
        "score": getattr(score_doc, "score", 0.0),
        "rank": getattr(score_doc, "rank", None),
        "percentile": getattr(score_doc, "percentile", None),
        "recommendation": getattr(score_doc, "recommendation", "NONE"),
        "status": getattr(score_doc, "status", None),
        "verification": getattr(score_doc, "verification", {}) or {},
        "model_version": getattr(score_doc, "model_version", None),
    }


def _asset_query(asset_type, query_text=""):
    if asset_type == "stock":
        query = IndividualStock.objects(active_status=0)
    else:
        query = StockIndex.objects()

    query_text = (query_text or "").strip()
    if query_text:
        query = query.filter(
            Q(code__icontains=query_text.lower()) | Q(name__icontains=query_text)
        )
    return query


def _serialize_index(code, fallback_name):
    stock = StockIndex.objects(code=code).only("code", "name").first()
    latest_date = _latest_quote_date_for_code(code, "stock_index")
    latest_quote = None
    if latest_date:
        latest_quote = (
            StockDailyQuote.objects(code=code, date=latest_date)
            .only(
                "date",
                "open",
                "close",
                "previous_close",
                "high",
                "low",
            )
            .first()
        )

    if not latest_quote:
        latest_quote = (
            StockDailyQuote.objects(code=code)
            .only(
                "date",
                "open",
                "close",
                "previous_close",
                "high",
                "low",
            )
            .order_by("-date")
            .first()
        )

    if not latest_quote:
        return {
            "code": code,
            "name": stock.name if stock else fallback_name,
            "price": 0.0,
            "change": 0.0,
            "changePct": 0.0,
            "sparkline": [],
        }

    # 优先根据本地 Datahub 已有的历史数据进行计算
    # 查找上一个交易日的记录
    prev_quote = (
        StockDailyQuote.objects(code=code, date__lt=latest_quote.date)
        .only("close")
        .order_by("-date")
        .first()
    )

    if prev_quote:
        previous_close = _to_number(prev_quote.close)
    else:
        # 兜底方案：使用当前记录中存储的昨收字段
        previous_close = _to_number(
            latest_quote.previous_close, _to_number(latest_quote.close)
        )

    current_price = _to_number(latest_quote.close)
    change = current_price - previous_close
    change_pct = (change / previous_close * 100) if previous_close else 0.0

    # 获取近期走势用于展示折线图 (最近 10 个交易日)
    history = (
        StockDailyQuote.objects(code=code).only("close").order_by("-date").limit(10)
    )
    # 反转回正向时间序
    sparkline = [float(h.close) for h in reversed(list(history))]

    return {
        "code": code,
        "name": stock.name if stock else fallback_name,
        "price": current_price,
        "change": round(change, 2),
        "changePct": round(change_pct, 2),
        "sparkline": sparkline,
    }


def _build_major_indices():
    return [_serialize_index(code, name) for code, name in MAJOR_INDEXES]


def _build_market_breadth():
    stock_codes = list(IndividualStock.objects(active_status=0).scalar("code"))
    if not stock_codes:
        return {
            "advances": 0,
            "declines": 0,
            "limitUp": 0,
            "limitDown": 0,
        }

    latest_date = _latest_quote_date_for_object_type("individual_stock")
    if not latest_date:
        return {
            "advances": 0,
            "declines": 0,
            "limitUp": 0,
            "limitDown": 0,
        }

    quotes = (
        StockDailyQuote.objects(code__in=stock_codes, date=latest_date)
        .only("close", "previous_close", "change_rate", "trade_status")
        .no_dereference()
    )

    advances = declines = limit_up = limit_down = 0
    for quote in quotes:
        if quote.trade_status == 0:
            continue

        previous_close = quote.previous_close or quote.close or 0
        change_rate = quote.change_rate
        if change_rate is None and previous_close:
            change_rate = ((quote.close or 0) - previous_close) / previous_close * 100

        if change_rate is None:
            continue

        if change_rate > 0:
            advances += 1
        elif change_rate < 0:
            declines += 1

        if change_rate >= 9.5:
            limit_up += 1
        if change_rate <= -9.5:
            limit_down += 1

    return {
        "advances": advances,
        "declines": declines,
        "limitUp": limit_up,
        "limitDown": limit_down,
    }


def _build_top_movers(limit=5):
    stock_map = {
        stock.code: stock.name
        for stock in IndividualStock.objects(active_status=0).only("code", "name")
    }

    latest_date = _latest_quote_date_for_object_type("individual_stock")
    if not latest_date:
        return [], []

    quotes = (
        StockDailyQuote.objects(code__in=list(stock_map.keys()), date=latest_date)
        .only("code", "close", "change_rate")
        .no_dereference()
    )

    movers = []
    for quote in quotes:
        change_pct = quote.change_rate
        if change_pct is None:
            continue
        movers.append(
            {
                "code": quote.code,
                "name": stock_map.get(quote.code, quote.code),
                "price": _to_number(quote.close),
                "changePct": _to_number(change_pct),
            }
        )

    gainers = sorted(movers, key=lambda item: item["changePct"], reverse=True)[:limit]
    losers = sorted(movers, key=lambda item: item["changePct"])[:limit]
    return gainers, losers


def _build_payload():
    try:
        gainers, losers = _build_top_movers()
    except Exception as exc:  # pragma: no cover - defensive fallback for live data
        logger.exception("Failed to build top movers: %s", exc)
        gainers, losers = [], []

    return {
        "generated_at": datetime.datetime.now().isoformat(),
        "indices": _build_major_indices(),
        "breadth": _build_market_breadth(),
        "sectors": [],
        "top_gainers": gainers,
        "top_losers": losers,
        "capital_flow": {
            "northbound": 0,
            "main": 0,
            "retail": 0,
        },
    }


@market_bp.route("/overview", methods=["GET"])
def get_market_overview():
    """Return a lightweight market overview for the dashboard."""
    now = time.monotonic()
    with _overview_cache_lock:
        if _overview_cache["payload"] and _overview_cache["expires_at"] > now:
            return jsonify(_overview_cache["payload"]), 200

        payload = _build_payload()
        _overview_cache["payload"] = payload
        _overview_cache["expires_at"] = time.monotonic() + OVERVIEW_CACHE_TTL_SECONDS
        return jsonify(payload), 200


def _resolve_market_target_date(asset_type, date_str):
    if date_str:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d")

    object_type = "individual_stock" if asset_type == "stock" else "stock_index"
    latest_date = _latest_quote_date_for_object_type(object_type)
    return latest_date or datetime.datetime.now()


def _serialize_market_item(
    code, name, quote, score_map, primary_horizon, display_rank
):
    horizon_scores = {
        str(horizon): _serialize_score_summary(score_map.get((code, horizon)))
        for horizon in (5, 20, 60)
    }
    primary_score = horizon_scores[str(primary_horizon)]

    verification = primary_score["verification"] or {}
    return {
        "code": code,
        "name": name,
        "ohlcv": {
            "open": _to_number(quote.open) if quote else None,
            "high": _to_number(quote.high) if quote else None,
            "low": _to_number(quote.low) if quote else None,
            "close": _to_number(quote.close) if quote else None,
            "volume": _to_number(quote.volume) if quote else None,
            "change_rate": _to_number(quote.change_rate) if quote else None,
        },
        "evaluation": {
            "primary_horizon": primary_horizon,
            "score": primary_score["score"],
            "rank": primary_score["rank"] or display_rank,
            "display_rank": display_rank,
            "percentile": primary_score["percentile"],
            "recommendation": primary_score["recommendation"],
            "basis": {"signals": [], "trend": []},
            "status": primary_score["status"],
            "verification": verification,
            "model_version": primary_score["model_version"],
            "profit_percentage_t5": verification.get("profit_percentage_t5"),
            "max_profit_percentage": verification.get("max_profit_percentage"),
            "is_effective": verification.get("is_effective"),
            "scores": horizon_scores,
        },
    }


@market_bp.route("/comprehensive", methods=["GET"])
def get_comprehensive_data():
    """Return comprehensive OHLCV and Scoring data."""
    asset_type = request.args.get("type", "stock")
    date_str = request.args.get("date")
    primary_horizon = _parse_horizon(request.args.get("horizon"), default=5)
    page = _parse_int(request.args.get("page"), 1, minimum=1, maximum=100000)
    per_page = _parse_int(
        request.args.get("per_page"),
        DEFAULT_PAGE_SIZE,
        minimum=1,
        maximum=MAX_PAGE_SIZE,
    )
    query_text = request.args.get("q", "")

    try:
        target_date = _resolve_market_target_date(asset_type, date_str)
    except ValueError:
        return jsonify({"success": False, "message": "Invalid date format"}), 400

    # Normalize to midnight for comparison
    target_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    offset = (page - 1) * per_page

    assets_query = _asset_query(asset_type, query_text).only("code", "name")
    total = assets_query.count()
    all_matching_codes = None
    score_query = StockScorePrediction.objects(
        date=target_date, horizon=primary_horizon
    )
    if query_text:
        all_matching_assets = list(assets_query)
        all_matching_codes = [asset.code for asset in all_matching_assets]
        asset_name_map = {asset.code: asset.name for asset in all_matching_assets}
        score_query = score_query.filter(stock_code__in=all_matching_codes)
    else:
        asset_name_map = {}

    primary_scores = list(
        score_query.order_by("-score", "stock_code").skip(offset).limit(per_page)
    )
    if primary_scores:
        page_codes = [score.stock_code for score in primary_scores]
        if not asset_name_map:
            page_assets = list(
                _asset_query(asset_type)
                .filter(code__in=page_codes)
                .only("code", "name")
            )
            asset_name_map = {asset.code: asset.name for asset in page_assets}
    else:
        page_assets = list(assets_query.order_by("code").skip(offset).limit(per_page))
        page_codes = [asset.code for asset in page_assets]
        asset_name_map.update({asset.code: asset.name for asset in page_assets})

    quotes = (
        StockDailyQuote.objects(code__in=page_codes, date=target_date)
        .only("code", "open", "high", "low", "close", "volume", "change_rate")
        .no_dereference()
    )
    quote_map = {q.code: q for q in quotes}

    scores = StockScorePrediction.objects(stock_code__in=page_codes, date=target_date)
    score_map = {(s.stock_code, getattr(s, "horizon", 5)): s for s in scores}

    items = []
    for index, code in enumerate(page_codes, start=offset + 1):
        items.append(
            _serialize_market_item(
                code,
                asset_name_map.get(code, code),
                quote_map.get(code),
                score_map,
                primary_horizon,
                index,
            )
        )

    return (
        jsonify(
            {
                "success": True,
                "date": target_date.strftime("%Y-%m-%d"),
                "total": total,
                "page": page,
                "per_page": per_page,
                "items": items,
            }
        ),
        200,
    )
