# -*- coding: utf-8 -*-
# Author : Gemini CLI
# Date: 2026-04-16

from flask import request

from app.api.v1.integrations.openclaw.utils import _get_latest_date, wrap_response
from app.api.v1.quotes import _format_datetime, _normalize_symbol, _parse_datetime
from app.lib.auth_decorators import service_token_required
from app.model.stock import StockDailyQuote

from . import openclaw_bp


def _serialize_quote_claw(quote):
    return {
        "code": quote.code,
        "date": _format_datetime(quote.date),
        "open": quote.open,
        "close": quote.close,
        "high": quote.high,
        "low": quote.low,
        "volume": quote.volume,
        "trade_amount": quote.trade_amount,
        "turnover_rate": quote.turnover_rate,
        "trade_status": getattr(quote, "trade_status", 1),
        "is_st": getattr(quote, "isST", 0),
    }


@openclaw_bp.route("/quotes/daily", methods=["GET"])
@service_token_required(scope="openclaw:data-read")
def get_daily_quotes():
    """
    Get daily quotes for OpenClaw.
    Supports filtering by symbols (comma separated), start_date, and end_date.
    """
    symbols_raw = request.args.get("symbols", "")
    start_date = _parse_datetime(request.args.get("start_date"))
    end_date = _parse_datetime(request.args.get("end_date"))
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 100))

    query = {}
    if symbols_raw:
        codes = [
            _normalize_symbol(s.strip()) for s in symbols_raw.split(",") if s.strip()
        ]
        if codes:
            query["code__in"] = codes

    if start_date:
        query["date__gte"] = start_date
    if end_date:
        query["date__lte"] = end_date

    quotes_qs = StockDailyQuote.objects(**query).order_by("-date", "code")
    total = quotes_qs.count()
    items = quotes_qs.skip((page - 1) * per_page).limit(per_page)

    data_as_of = (
        _get_latest_date(StockDailyQuote, filter_kwargs=query) if query else None
    )

    return wrap_response(
        data={
            "items": [_serialize_quote_claw(q) for q in items],
            "total": total,
            "page": page,
            "per_page": per_page,
        },
        data_as_of=data_as_of,
    )
