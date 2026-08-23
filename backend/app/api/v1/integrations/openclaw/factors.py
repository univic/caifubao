# -*- coding: utf-8 -*-
# Author : Gemini CLI
# Date: 2026-04-16

from flask import request

from app.api.v1.integrations.openclaw.utils import _get_latest_date, wrap_response
from app.api.v1.quotes import _format_datetime, _normalize_symbol, _parse_datetime
from app.lib.auth_decorators import service_token_required
from app.model.factor import StockFactorDaily
from app.model.stock import StockDailyQuote

from . import openclaw_bp


def _serialize_factor_claw(quote, factor=None):
    data = {
        "code": quote.code,
        "date": _format_datetime(quote.date),
        # Adjusted prices (hfq)
        "fq_factor": quote.fq_factor,
        "open_hfq": quote.open_hfq,
        "close_hfq": quote.close_hfq,
        "high_hfq": quote.high_hfq,
        "low_hfq": quote.low_hfq,
        # Original prices for reference
        "open": quote.open,
        "close": quote.close,
    }

    if factor:
        data.update(
            {
                "ma_10": factor.ma_10,
                "ma_20": factor.ma_20,
                "ma_30": factor.ma_30,
                "ma_60": factor.ma_60,
                "ma_120": factor.ma_120,
            }
        )
    else:
        data.update(
            {
                "ma_10": None,
                "ma_20": None,
                "ma_30": None,
                "ma_60": None,
                "ma_120": None,
            }
        )

    return data


@openclaw_bp.route("/factors/daily", methods=["GET"])
@service_token_required(scope="openclaw:data-read")
def get_daily_factors():
    """
    Get daily factors and adjusted prices for OpenClaw.
    Merges data from StockDailyQuote and StockFactorDaily.
    """
    symbols_raw = request.args.get("symbols", "")
    start_date = _parse_datetime(request.args.get("start_date"))
    end_date = _parse_datetime(request.args.get("end_date"))
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 50))

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
    quotes = list(quotes_qs.skip((page - 1) * per_page).limit(per_page))

    # Batch fetch factors
    if quotes:
        codes = list(set(q.code for q in quotes))
        dates = list(set(q.date for q in quotes))
        factors = StockFactorDaily.objects(stock_code__in=codes, date__in=dates)
        factor_map = {(f.stock_code, f.date): f for f in factors}
    else:
        factor_map = {}

    items = []
    for q in quotes:
        f = factor_map.get((q.code, q.date))
        items.append(_serialize_factor_claw(q, f))

    data_as_of = (
        _get_latest_date(StockDailyQuote, filter_kwargs=query) if query else None
    )

    return wrap_response(
        data={
            "items": items,
            "total": total,
            "page": page,
            "per_page": per_page,
        },
        data_as_of=data_as_of,
    )
