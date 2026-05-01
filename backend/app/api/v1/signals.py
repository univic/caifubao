# -*- coding: utf-8 -*-
# Signal APIs for the MVP signal list page.

from datetime import datetime
from typing import Any

from flask import Blueprint, jsonify, request

from app.model.signal import StockSignalDaily

signals_bp = Blueprint("signals", __name__, url_prefix="/api/signals")

DEFAULT_LIMIT = 50
MAX_LIMIT = 200


def _parse_int(value, default, minimum=0, maximum=MAX_LIMIT):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(min(parsed, maximum), minimum)


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


def _serialize_value(value: Any):
    if isinstance(value, datetime):
        return _format_datetime(value)
    if isinstance(value, dict):
        return {key: _serialize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    return value


def _serialize_signal(row):
    return {
        "stock_code": row.stock_code,
        "stock_name": row.stock_name,
        "category": getattr(row, "category", None),
        "date": _format_datetime(row.date),
        "signal_name": row.signal_name,
        "signal_version": getattr(row, "signal_version", None),
        "direction": row.direction,
        "signal_type": row.signal_type,
        "strength": row.strength,
        "reason": row.reason,
        "price_snapshot": _serialize_value(row.price_snapshot or {}),
        "factor_snapshot": _serialize_value(row.factor_snapshot or {}),
        "source_freshness": _serialize_value(row.source_freshness or {}),
        "generated_at": _format_datetime(row.generated_at),
    }


def _get_latest_signal_date(signal_name=None, direction=None):
    query = StockSignalDaily.objects()
    if signal_name:
        query = query.filter(signal_name=signal_name)
    if direction:
        query = query.filter(direction=direction)
    latest = query.order_by("-date").only("date").first()
    return latest.date if latest else None


def _build_signal_query(date=None, signal_name=None, direction=None):
    query = StockSignalDaily.objects()
    if date:
        query = query.filter(date=date)
    if signal_name:
        query = query.filter(signal_name=signal_name)
    if direction:
        query = query.filter(direction=direction)
    return query


@signals_bp.route("", methods=["GET"])
def list_signals():
    signal_name = (request.args.get("signal_name") or "").strip() or None
    direction = (request.args.get("direction") or "").strip() or None
    limit = _parse_int(
        request.args.get("limit"), DEFAULT_LIMIT, minimum=1, maximum=MAX_LIMIT
    )
    offset = _parse_int(request.args.get("offset"), 0, minimum=0, maximum=10000)

    requested_date = _parse_datetime(request.args.get("date"))
    effective_date = requested_date or _get_latest_signal_date(
        signal_name=signal_name,
        direction=direction,
    )
    if effective_date is None:
        return jsonify(
            {
                "date": None,
                "requested_date": None,
                "total": 0,
                "limit": limit,
                "offset": offset,
                "items": [],
            }
        ), 200

    query = _build_signal_query(
        date=effective_date,
        signal_name=signal_name,
        direction=direction,
    )
    total = query.count()
    rows = list(query.order_by("-strength", "stock_code").skip(offset).limit(limit))
    return jsonify(
        {
            "date": _format_datetime(effective_date),
            "requested_date": _format_datetime(requested_date),
            "total": total,
            "limit": limit,
            "offset": offset,
            "items": [_serialize_signal(row) for row in rows],
        }
    ), 200
