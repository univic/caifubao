# -*- coding: utf-8 -*-
# Author : Gemini CLI
# Date: 2026-04-16

from flask import request

from app.api.v1.integrations.openclaw.utils import wrap_response
from app.api.v1.quotes import _format_datetime, _parse_datetime
from app.lib.auth_decorators import service_token_required
from app.model.signal import StockSignalDaily

from . import openclaw_bp


def _serialize_signal_claw(sig):
    return {
        "stock_code": sig.stock_code,
        "stock_name": sig.stock_name,
        "date": _format_datetime(sig.date),
        "signal_name": sig.signal_name,
        "signal_version": sig.signal_version,
        "direction": sig.direction,
        "signal_type": sig.signal_type,
        "strength": sig.strength,
        "reason": sig.reason,
        "factor_snapshot": sig.factor_snapshot,
        "price_snapshot": sig.price_snapshot,
        "generated_at": _format_datetime(sig.generated_at),
    }


@openclaw_bp.route("/signals", methods=["GET"])
@service_token_required(scope="openclaw:data-read")
def get_signals():
    """
    Get signals for OpenClaw.
    Supports filtering by date, signal_name, and direction.
    """
    date = _parse_datetime(request.args.get("date"))
    signal_name = request.args.get("signal_name")
    direction = request.args.get("direction")
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 100))

    query = {}
    if date:
        query["date"] = date
    if signal_name:
        query["signal_name"] = signal_name
    if direction:
        query["direction"] = direction

    signals_qs = StockSignalDaily.objects(**query).order_by("-date", "stock_code")
    total = signals_qs.count()
    items = signals_qs.skip((page - 1) * per_page).limit(per_page)

    return wrap_response(
        data={
            "items": [_serialize_signal_claw(s) for s in items],
            "total": total,
            "page": page,
            "per_page": per_page,
        }
    )
