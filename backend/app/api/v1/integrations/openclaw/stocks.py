# -*- coding: utf-8 -*-
# Author : Gemini CLI
# Date: 2026-04-16

from flask import request
from mongoengine.queryset.visitor import Q

from app.api.v1.integrations.openclaw.utils import wrap_response
from app.lib.auth_decorators import service_token_required
from app.model.stock import BasicStock, FinanceMarket, StockExchange

from . import openclaw_bp


def _serialize_stock_claw(stock):
    market = getattr(stock, "market", None)
    exchange = getattr(stock, "exchange", None)

    # Extract data capabilities
    caps = {}
    if hasattr(stock, "data_capabilities") and stock.data_capabilities:
        caps = {
            "daily_quote": stock.data_capabilities.daily_quote,
            "fq_factor": stock.data_capabilities.fq_factor,
            "ma_factor": stock.data_capabilities.ma_factor,
        }

    return {
        "code": stock.code,
        "name": stock.name,
        "object_type": getattr(stock, "object_type", "individual_stock"),
        "active_status": getattr(stock, "active_status", 0),
        "exchange": {
            "code": exchange.code if exchange else None,
            "name": exchange.name if exchange else None,
        },
        "market": {
            "code": market.code if market else None,
            "name": market.name if market else None,
        },
        "data_capabilities": caps,
        "watch_level": getattr(stock, "watch_level", None),
    }


@openclaw_bp.route("/stocks", methods=["GET"])
@service_token_required(scope="openclaw:data-read")
def list_stocks():
    """
    List stocks for OpenClaw.
    Supports filtering by active_status, exchange, market, and keyword.
    """
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 100))
    active_status = request.args.get("active_status")
    exchange_code = request.args.get("exchange")
    market_code = request.args.get("market")
    keyword = request.args.get("keyword")

    query = Q()
    if active_status is not None:
        query &= Q(active_status=int(active_status))

    if exchange_code:
        exchange = StockExchange.objects(code=exchange_code).first()
        if exchange:
            query &= Q(exchange=exchange)

    if market_code:
        market = FinanceMarket.objects(code=market_code).first()
        if market:
            query &= Q(market=market)

    if keyword:
        query &= Q(code__icontains=keyword) | Q(name__icontains=keyword)

    stocks_qs = BasicStock.objects(query).order_by("code")
    total = stocks_qs.count()

    items = stocks_qs.skip((page - 1) * per_page).limit(per_page)

    return wrap_response(
        data={
            "items": [_serialize_stock_claw(s) for s in items],
            "total": total,
            "page": page,
            "per_page": per_page,
        }
    )
