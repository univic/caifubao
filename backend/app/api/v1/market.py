# -*- coding: utf-8 -*-
# Market overview API Blueprint

import datetime
import logging

from flask import Blueprint, jsonify, request

from app.model.stock import IndividualStock, StockIndex, StockDailyQuote
from app.model.scoring import StockDailyScore

logger = logging.getLogger(__name__)

market_bp = Blueprint("market", __name__, url_prefix="/api/market")

MAJOR_INDEXES = [
    ("sh000001", "上证指数"),
    ("sz399001", "深证成指"),
    ("sz399006", "创业板指"),
    ("sh000688", "科创50"),
]


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


def _serialize_index(code, fallback_name):
    stock = StockIndex.objects(code=code).only("code", "name").first()
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

    pipeline = [
        {"$match": {"code": {"$in": stock_codes}}},
        {"$sort": {"code": 1, "date": -1}},
        {
            "$group": {
                "_id": "$code",
                "close": {"$first": "$close"},
                "previous_close": {"$first": "$previous_close"},
                "change_rate": {"$first": "$change_rate"},
                "trade_status": {"$first": "$trade_status"},
            }
        },
    ]

    advances = declines = limit_up = limit_down = 0
    for row in StockDailyQuote.objects.aggregate(pipeline):
        if row.get("trade_status") == 0:
            continue

        previous_close = row.get("previous_close") or row.get("close") or 0
        change_rate = row.get("change_rate")
        if change_rate is None and previous_close:
            change_rate = (
                ((row.get("close") or 0) - previous_close) / previous_close * 100
            )

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

    pipeline = [
        {"$match": {"code": {"$in": list(stock_map.keys())}}},
        {"$sort": {"code": 1, "date": -1}},
        {
            "$group": {
                "_id": "$code",
                "close": {"$first": "$close"},
                "change_rate": {"$first": "$change_rate"},
            }
        },
    ]

    movers = []
    for row in StockDailyQuote.objects.aggregate(pipeline):
        change_pct = row.get("change_rate")
        if change_pct is None:
            continue
        code = row.get("_id")
        movers.append(
            {
                "code": code,
                "name": stock_map.get(code, code),
                "price": _to_number(row.get("close")),
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
    return jsonify(_build_payload()), 200


@market_bp.route("/comprehensive", methods=["GET"])
def get_comprehensive_data():
    """Return comprehensive OHLCV and Scoring data."""
    asset_type = request.args.get("type", "stock")
    date_str = request.args.get("date")

    if date_str:
        try:
            target_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return jsonify({"success": False, "message": "Invalid date format"}), 400
    else:
        # Auto-detect latest date from quotes
        latest_quote = StockDailyQuote.objects.order_by("-date").first()
        target_date = latest_quote.date if latest_quote else datetime.datetime.now()

    # Normalize to midnight for comparison
    target_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)

    # 1. Fetch Assets
    if asset_type == "stock":
        assets = IndividualStock.objects(active_status=0).only("code", "name")
    else:
        assets = StockIndex.objects().only("code", "name")

    asset_map = {a.code: a.name for a in assets}
    codes = list(asset_map.keys())

    # 2. Fetch Quotes for this date
    quotes = StockDailyQuote.objects(code={"$in": codes}, date=target_date)
    quote_map = {q.code: q for q in quotes}

    # 3. Fetch Scores for this date
    scores = StockDailyScore.objects(stock_code={"$in": codes}, date=target_date)
    score_map = {s.stock_code: s for s in scores}

    # 4. Merge and Calculate Ranks
    items = []
    for code in codes:
        q = quote_map.get(code)
        s = score_map.get(code)

        items.append(
            {
                "code": code,
                "name": asset_map.get(code),
                "ohlcv": {
                    "open": _to_number(q.open) if q else None,
                    "high": _to_number(q.high) if q else None,
                    "low": _to_number(q.low) if q else None,
                    "close": _to_number(q.close) if q else None,
                    "volume": _to_number(q.volume) if q else None,
                    "change_rate": _to_number(q.change_rate) if q else None,
                },
                "evaluation": {
                    "score": s.score if s else 0.0,  # Default to 0 for ranking
                    "recommendation": s.recommendation if s else "NONE",
                    "basis": s.scoring_basis if s else {},
                    "status": s.status if s else None,
                    "profit_percentage_t5": s.profit_percentage_t5 if s else None,
                    "max_profit_percentage": s.max_profit_percentage if s else None,
                    "is_effective": s.is_effective if s else None,
                },
            }
        )

    # Sort by score descending to assign rank
    items.sort(key=lambda x: x["evaluation"]["score"], reverse=True)
    for i, item in enumerate(items):
        item["evaluation"]["rank"] = i + 1

    return (
        jsonify(
            {
                "success": True,
                "date": target_date.strftime("%Y-%m-%d"),
                "total": len(items),
                "items": items,
            }
        ),
        200,
    )
