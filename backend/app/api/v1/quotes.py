# -*- coding: utf-8 -*-
# Quote APIs for stock search and history views

from datetime import datetime

from flask import Blueprint, jsonify, request

from app.model.data_asset_status import DataAssetStatus
from app.model.stock import BasicStock, StockDailyQuote

quotes_bp = Blueprint("quotes", __name__, url_prefix="/api/quotes")

DEFAULT_HISTORY_LIMIT = 120
MAX_SEARCH_LIMIT = 20


def _parse_int(value, default, minimum=1, maximum=500):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(min(parsed, maximum), minimum)


def _normalize_symbol(symbol):
    raw = (symbol or "").strip().lower()
    if not raw:
        return ""

    if raw.startswith(("sh", "sz", "bj")) and raw[2:].isdigit():
        return raw

    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return raw

    if digits.startswith(("6", "9")) or digits.startswith(("688", "689")):
        return f"sh{digits}"
    if digits.startswith(("4", "8")) or digits.startswith("92"):
        return f"bj{digits}"
    return f"sz{digits}"


def _parse_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip().replace("Z", "+00:00")
    if not text:
        return None
    if len(text) == 10:
        try:
            return datetime.fromisoformat(f"{text}T00:00:00")
        except ValueError:
            return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _format_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _serialize_stock(stock):
    market = getattr(stock, "market", None)
    exchange = getattr(stock, "exchange", None)
    return {
        "code": stock.code,
        "name": stock.name,
        "object_type": getattr(stock, "object_type", None),
        "market_name": market.name if market else None,
        "market_code": market.code if market else None,
        "exchange_name": exchange.name if exchange else None,
        "exchange_code": exchange.code if exchange else None,
        "active_status": getattr(stock, "active_status", None),
        "watch_level": getattr(stock, "watch_level", None),
        "pre_name": list(getattr(stock, "pre_name", []) or []),
        "total_equity": getattr(stock, "total_equity", None),
        "outstanding_share": getattr(stock, "outstanding_share", None),
    }


def _serialize_quote(quote):
    if not quote:
        return None
    previous_close = quote.previous_close
    change_amount = quote.change_amount
    change_rate = quote.change_rate
    if change_amount is None and quote.close is not None and previous_close is not None:
        change_amount = quote.close - previous_close
    if (
        change_rate is None
        and previous_close not in (None, 0)
        and change_amount is not None
    ):
        change_rate = (change_amount / previous_close) * 100
    return {
        "date": _format_datetime(quote.date),
        "open": quote.open,
        "close": quote.close,
        "previous_close": previous_close,
        "high": quote.high,
        "low": quote.low,
        "volume": quote.volume,
        "trade_amount": quote.trade_amount,
        "change_amount": change_amount,
        "change_rate": change_rate,
        "turnover_rate": quote.turnover_rate,
    }


def _find_stock(symbol):
    normalized = _normalize_symbol(symbol)
    candidates = []
    if normalized:
        candidates.append(normalized)
    raw = (symbol or "").strip().lower()
    if raw and raw not in candidates:
        candidates.append(raw)

    for candidate in candidates:
        stock = BasicStock.objects(code=candidate).first()
        if stock:
            return stock
    return None


def _get_latest_freshness(stock_code, object_type):
    asset_status = (
        DataAssetStatus.objects(
            code=stock_code,
            object_type=object_type,
            asset_type="quote",
            asset_name="daily_quote",
        )
        .order_by("-latest_data_date")
        .first()
    )
    if not asset_status:
        return None
    return {
        "freshness_datetime": _format_datetime(asset_status.latest_data_date),
        "calculated_at": _format_datetime(asset_status.last_calculated_at),
        "status": asset_status.status,
        "deprecated": True,
        "source": "data_asset_status",
    }


def _get_daily_quotes(stock_code, start=None, end=None, limit=DEFAULT_HISTORY_LIMIT):
    qs = StockDailyQuote.objects(code=stock_code)
    if start:
        qs = qs.filter(date__gte=start)
    if end:
        qs = qs.filter(date__lte=end)

    if start or end:
        rows = list(qs.order_by("date")[:limit]) if limit else list(qs.order_by("date"))
    else:
        rows = (
            list(qs.order_by("-date")[:limit]) if limit else list(qs.order_by("-date"))
        )
        rows.reverse()
    return rows


def _build_search_items(query, limit):
    normalized = _normalize_symbol(query)
    seen = set()
    items = []

    def add_stock(stock):
        if not stock or stock.code in seen:
            return
        seen.add(stock.code)
        items.append(_serialize_stock(stock))

    if normalized:
        add_stock(BasicStock.objects(code=normalized).first())

    for stock in BasicStock.objects(code__icontains=query).order_by("code"):
        add_stock(stock)
        if len(items) >= limit:
            return items

    for stock in BasicStock.objects(name__icontains=query).order_by("code"):
        add_stock(stock)
        if len(items) >= limit:
            return items

    return items[:limit]


@quotes_bp.route("/search", methods=["GET"])
def search_quotes():
    query = (request.args.get("q") or request.args.get("keyword") or "").strip()
    limit = _parse_int(
        request.args.get("limit"), default=10, minimum=1, maximum=MAX_SEARCH_LIMIT
    )
    if not query:
        return jsonify({"query": query, "total": 0, "items": []}), 200

    items = _build_search_items(query, limit)
    return jsonify({"query": query, "total": len(items), "items": items}), 200


@quotes_bp.route("/<symbol>", methods=["GET"])
def get_quote_detail(symbol):
    stock = _find_stock(symbol)
    if not stock:
        return jsonify({"message": "Stock not found"}), 404

    latest_quote = StockDailyQuote.objects(code=stock.code).order_by("-date").first()
    object_type = getattr(stock, "object_type", "individual_stock")
    return jsonify(
        {
            "symbol": symbol,
            "normalized_symbol": stock.code,
            "stock": _serialize_stock(stock),
            "freshness": _get_latest_freshness(stock.code, object_type),
            "latest_quote": _serialize_quote(latest_quote),
        }
    ), 200


@quotes_bp.route("/<symbol>/daily", methods=["GET"])
def get_quote_daily(symbol):
    stock = _find_stock(symbol)
    if not stock:
        return jsonify({"message": "Stock not found"}), 404

    limit = _parse_int(
        request.args.get("limit"), default=DEFAULT_HISTORY_LIMIT, minimum=1, maximum=365
    )
    start = _parse_datetime(request.args.get("start"))
    end = _parse_datetime(request.args.get("end"))
    quotes = _get_daily_quotes(stock.code, start=start, end=end, limit=limit)

    return jsonify(
        {
            "symbol": symbol,
            "normalized_symbol": stock.code,
            "count": len(quotes),
            "quotes": [_serialize_quote(quote) for quote in quotes],
        }
    ), 200
