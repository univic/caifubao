# -*- coding: utf-8 -*-
# Index Overview API - Stock indices OHLCV data

from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required
from pymongo.errors import PyMongoError

from app.model.stock import StockIndex, StockDailyQuote

indices_bp = Blueprint("indices", __name__, url_prefix="/api/v1/indices")

# Main indices to show in overview
MAIN_INDICES = [
    "000001",  # 上证指数
    "399001",  # 深证成指
    "399006",  # 创业板指
    "000688",  # 科创50
    "000300",  # 沪深300
    "000016",  # 上证50
    "000905",  # 中证500
    "000852",  # 中证1000
]


def get_latest_quote(code: str) -> StockDailyQuote | None:
    """Get the latest daily quote for a given index code"""
    return StockDailyQuote.objects(code=code).order_by("-date").first()


def get_latest_quote_pair(
    code: str,
) -> tuple[StockDailyQuote | None, StockDailyQuote | None]:
    """Get the latest and previous daily quotes for a given index code."""
    quotes = list(StockDailyQuote.objects(code=code).order_by("-date").limit(2))
    latest = quotes[0] if quotes else None
    previous = quotes[1] if len(quotes) > 1 else None
    return latest, previous


def build_quote_metrics(latest_quote, previous_quote=None):
    """Build consistent quote-derived metrics for index responses."""
    close_price = latest_quote.close if latest_quote else None
    previous_close = (
        previous_quote.close
        if previous_quote and previous_quote.close is not None
        else close_price
    )

    if close_price is not None and previous_close not in (None, 0):
        change_amount = close_price - previous_close
        change_rate = change_amount / previous_close * 100
    else:
        change_amount = 0
        change_rate = 0

    return {
        "close": close_price,
        "previousClose": previous_close,
        "changeAmount": change_amount,
        "changeRate": change_rate,
    }


def build_quote_metrics_from_docs(latest_quote_doc, previous_quote_doc=None):
    """Build consistent quote-derived metrics for Mongo dict documents."""
    close_price = latest_quote_doc.get("close") if latest_quote_doc else None
    previous_close = (
        previous_quote_doc.get("close")
        if previous_quote_doc and previous_quote_doc.get("close") is not None
        else close_price
    )

    if close_price is not None and previous_close not in (None, 0):
        change_amount = close_price - previous_close
        change_rate = change_amount / previous_close * 100
    else:
        change_amount = 0
        change_rate = 0

    return {
        "close": close_price,
        "previousClose": previous_close,
        "changeAmount": change_amount,
        "changeRate": change_rate,
    }


def get_latest_index_quote_snapshots(index_codes, sort_by, order, page, page_size):
    """Fetch latest index quote snapshots with DB-side sorting and pagination."""
    sort_field_map = {
        "change_rate": "calculated_change_rate",
        "close": "close",
        "volume": "volume",
        "date": "date",
    }
    sort_key = sort_field_map[sort_by]
    sort_direction = 1 if order == "asc" else -1

    pipeline = [
        {"$match": {"code": {"$in": list(index_codes)}}},
        {"$sort": {"code": 1, "date": -1}},
        {
            "$group": {
                "_id": "$code",
                "latest_quote": {
                    "$first": {
                        "code": "$code",
                        "open": "$open",
                        "high": "$high",
                        "low": "$low",
                        "close": "$close",
                        "volume": "$volume",
                        "date": "$date",
                    }
                },
                "closes": {"$push": "$close"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "code": "$_id",
                "open": "$latest_quote.open",
                "high": "$latest_quote.high",
                "low": "$latest_quote.low",
                "close": "$latest_quote.close",
                "volume": "$latest_quote.volume",
                "date": "$latest_quote.date",
                "previous_close": {
                    "$ifNull": [
                        {"$arrayElemAt": ["$closes", 1]},
                        "$latest_quote.close",
                    ]
                },
            }
        },
        {
            "$addFields": {
                "change_amount": {
                    "$cond": [
                        {
                            "$and": [
                                {"$ne": ["$close", None]},
                                {"$ne": ["$previous_close", None]},
                                {"$ne": ["$previous_close", 0]},
                            ]
                        },
                        {"$subtract": ["$close", "$previous_close"]},
                        0,
                    ]
                }
            }
        },
        {
            "$addFields": {
                "calculated_change_rate": {
                    "$cond": [
                        {
                            "$and": [
                                {"$ne": ["$close", None]},
                                {"$ne": ["$previous_close", None]},
                                {"$ne": ["$previous_close", 0]},
                            ]
                        },
                        {
                            "$multiply": [
                                {
                                    "$divide": [
                                        {"$subtract": ["$close", "$previous_close"]},
                                        "$previous_close",
                                    ]
                                },
                                100,
                            ]
                        },
                        0,
                    ]
                }
            }
        },
        {"$sort": {sort_key: sort_direction, "code": 1}},
        {"$skip": (page - 1) * page_size},
        {"$limit": page_size},
    ]
    return list(StockDailyQuote.objects.aggregate(pipeline))


def get_index_overview_data():
    """Get overview data for main indices"""
    result = []
    for code in MAIN_INDICES:
        index = StockIndex.objects(code=code).first()
        if not index:
            continue
        latest_quote, previous_quote = get_latest_quote_pair(code)
        if not latest_quote:
            continue
        metrics = build_quote_metrics(latest_quote, previous_quote)
        result.append(
            {
                "code": code,
                "name": index.name,
                "price": metrics["close"],
                "previousClose": metrics["previousClose"],
                "change": metrics["changeAmount"],
                "changePct": metrics["changeRate"],
                "open": latest_quote.open,
                "high": latest_quote.high,
                "low": latest_quote.low,
                "volume": latest_quote.volume,
                "tradeDate": latest_quote.date.isoformat()
                if latest_quote.date
                else None,
            }
        )
    return result


@indices_bp.route("/overview", methods=["GET"])
@jwt_required()
def get_overview():
    """Get overview of main stock indices"""
    try:
        data = get_index_overview_data()
        return jsonify({"data": data}), 200
    except Exception as e:
        return jsonify({"message": f"Failed to fetch index overview: {str(e)}"}), 500


@indices_bp.route("", methods=["GET"])
@jwt_required()
def get_indices_list():
    """Get paginated list of all stock indices with OHLCV data"""
    page = request.args.get("page", 1, type=int)
    page_size = request.args.get("page_size", 100, type=int)
    sort_by = request.args.get("sort_by", "change_rate")
    order = request.args.get("order", "desc")

    # Validate pagination
    page = max(1, page)
    page_size = min(max(1, page_size), 500)

    # Validate sort field
    valid_sort_fields = {
        "change_rate",
        "close",
        "volume",
        "date",
        "code",
        "name",
    }
    if sort_by not in valid_sort_fields:
        sort_by = "change_rate"

    # Determine sort order
    sort_prefix = "-" if order == "desc" else ""

    # Fields that are in StockDailyQuote (not StockIndex)
    quote_sort_fields = {
        "change_rate",
        "close",
        "volume",
        "date",
    }
    # Get total count of StockIndex documents
    total = StockIndex.objects().count()

    if sort_by in quote_sort_fields:
        # For quote fields: let Mongo aggregate each index's latest two quotes,
        # derive the latest snapshot, then sort and paginate in the database.
        all_index_codes = [idx.code for idx in StockIndex.objects().only("code")]

        try:
            paginated_quotes = get_latest_index_quote_snapshots(
                all_index_codes,
                sort_by=sort_by,
                order=order,
                page=page,
                page_size=page_size,
            )
        except PyMongoError:
            paginated_quotes = []
        paginated_codes = [q["code"] for q in paginated_quotes]

        # Fetch StockIndex info for paginated codes
        indices = {
            idx.code: idx for idx in StockIndex.objects(code__in=paginated_codes)
        }

        items = []
        for quote in paginated_quotes:
            idx = indices.get(quote["code"])
            if not idx:
                continue
            close_price = quote.get("close")
            items.append(
                {
                    "code": idx.code,
                    "name": idx.name,
                    "close": close_price,
                    "previousClose": quote.get("previous_close"),
                    "open": quote.get("open"),
                    "high": quote.get("high"),
                    "low": quote.get("low"),
                    "changeRate": quote["calculated_change_rate"],
                    "changeAmount": quote["change_amount"],
                    "volume": quote.get("volume"),
                    "tradeDate": quote.get("date").isoformat()
                    if quote.get("date")
                    else None,
                }
            )
    else:
        # For index fields (code, name): sort directly on StockIndex
        sort_field = f"{sort_prefix}{sort_by}"

        indices = (
            StockIndex.objects()
            .order_by(sort_field)
            .skip((page - 1) * page_size)
            .limit(page_size)
        )

        items = []
        for idx in indices:
            latest_quote, previous_quote = get_latest_quote_pair(idx.code)
            if latest_quote:
                metrics = build_quote_metrics(latest_quote, previous_quote)
                items.append(
                    {
                        "code": idx.code,
                        "name": idx.name,
                        "close": metrics["close"],
                        "previousClose": metrics["previousClose"],
                        "open": latest_quote.open,
                        "high": latest_quote.high,
                        "low": latest_quote.low,
                        "changeRate": metrics["changeRate"],
                        "changeAmount": metrics["changeAmount"],
                        "volume": latest_quote.volume,
                        "tradeDate": latest_quote.date.isoformat()
                        if latest_quote.date
                        else None,
                    }
                )
            else:
                items.append(
                    {
                        "code": idx.code,
                        "name": idx.name,
                        "close": None,
                        "previousClose": None,
                        "open": None,
                        "high": None,
                        "low": None,
                        "changeRate": 0,
                        "changeAmount": 0,
                        "volume": None,
                        "tradeDate": None,
                    }
                )

    return jsonify(
        {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
        }
    ), 200
