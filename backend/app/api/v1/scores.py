# -*- coding: utf-8 -*-
# Read APIs for horizon-specific stock score predictions.

from datetime import datetime
from typing import Any

from flask import Blueprint, jsonify, request

from app.model.scoring import StockScorePrediction

scores_bp = Blueprint("scores", __name__, url_prefix="/api/scores")

SUPPORTED_HORIZONS = {5, 20, 60}
DEFAULT_LIMIT = 50
MAX_LIMIT = 200


def _parse_int(value, default, minimum=0, maximum=MAX_LIMIT):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(min(parsed, maximum), minimum)


def _parse_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value.replace(hour=0, minute=0, second=0, microsecond=0)
    text = str(value).strip().replace("Z", "+00:00")
    if not text:
        return None
    if len(text) == 10:
        try:
            return datetime.fromisoformat(f"{text}T00:00:00")
        except ValueError:
            return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed.replace(hour=0, minute=0, second=0, microsecond=0)


def _parse_horizon(value, default=5):
    horizon = _parse_int(value, default, minimum=1, maximum=999)
    return horizon if horizon in SUPPORTED_HORIZONS else None


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


def _serialize_prediction(row, include_details=False):
    payload = {
        "stock_code": row.stock_code,
        "stock_name": row.stock_name,
        "date": _format_datetime(row.date),
        "horizon": row.horizon,
        "score": row.score,
        "rank": row.rank,
        "percentile": row.percentile,
        "recommendation": row.recommendation,
        "base_price": row.base_price,
        "target_date": _format_datetime(row.target_date),
        "status": row.status,
        "verification": _serialize_value(row.verification or {}),
        "model_version": row.model_version,
        "generated_at": _format_datetime(row.generated_at),
        "updated_at": _format_datetime(row.updated_at),
    }
    if include_details:
        payload["explanation"] = _serialize_value(row.explanation or {})
        payload["input_snapshot"] = _serialize_value(row.input_snapshot or {})
    return payload


def _latest_score_date(horizon=None, model_version=None):
    query = StockScorePrediction.objects()
    if horizon:
        query = query.filter(horizon=horizon)
    if model_version:
        query = query.filter(model_version=model_version)
    latest = query.order_by("-date").only("date").first()
    return latest.date if latest else None


@scores_bp.route("", methods=["GET"])
def list_scores():
    horizon = _parse_horizon(request.args.get("horizon"), default=5)
    if horizon is None:
        return jsonify({"success": False, "message": "Unsupported horizon"}), 400

    requested_date = _parse_datetime(request.args.get("date"))
    model_version = (request.args.get("model_version") or "").strip() or None
    effective_date = requested_date or _latest_score_date(
        horizon=horizon,
        model_version=model_version,
    )
    limit = _parse_int(
        request.args.get("limit"), DEFAULT_LIMIT, minimum=1, maximum=MAX_LIMIT
    )
    offset = _parse_int(request.args.get("offset"), 0, minimum=0, maximum=10000)
    min_score = _parse_float(request.args.get("min_score"))
    recommendation = (request.args.get("recommendation") or "").strip() or None
    status = (request.args.get("status") or "").strip() or None

    if effective_date is None:
        return jsonify(
            {
                "date": None,
                "requested_date": _format_datetime(requested_date),
                "horizon": horizon,
                "total": 0,
                "limit": limit,
                "offset": offset,
                "items": [],
            }
        ), 200

    query = StockScorePrediction.objects(date=effective_date, horizon=horizon)
    if model_version:
        query = query.filter(model_version=model_version)
    if min_score is not None:
        query = query.filter(score__gte=min_score)
    if recommendation:
        query = query.filter(recommendation=recommendation)
    if status:
        query = query.filter(status=status)

    total = query.count()
    rows = list(query.order_by("-score", "stock_code").skip(offset).limit(limit))
    return jsonify(
        {
            "date": _format_datetime(effective_date),
            "requested_date": _format_datetime(requested_date),
            "horizon": horizon,
            "total": total,
            "limit": limit,
            "offset": offset,
            "items": [_serialize_prediction(row) for row in rows],
        }
    ), 200


@scores_bp.route("/<stock_code>", methods=["GET"])
def get_stock_score_history(stock_code):
    horizon = _parse_horizon(request.args.get("horizon"), default=5)
    if horizon is None:
        return jsonify({"success": False, "message": "Unsupported horizon"}), 400

    start_date = _parse_datetime(request.args.get("from"))
    end_date = _parse_datetime(request.args.get("to"))
    model_version = (request.args.get("model_version") or "").strip() or None
    limit = _parse_int(
        request.args.get("limit"), DEFAULT_LIMIT, minimum=1, maximum=MAX_LIMIT
    )
    offset = _parse_int(request.args.get("offset"), 0, minimum=0, maximum=10000)

    query = StockScorePrediction.objects(stock_code=stock_code, horizon=horizon)
    if start_date:
        query = query.filter(date__gte=start_date)
    if end_date:
        query = query.filter(date__lte=end_date)
    if model_version:
        query = query.filter(model_version=model_version)

    total = query.count()
    rows = list(query.order_by("-date").skip(offset).limit(limit))
    return jsonify(
        {
            "stock_code": stock_code,
            "horizon": horizon,
            "total": total,
            "limit": limit,
            "offset": offset,
            "items": [_serialize_prediction(row) for row in rows],
        }
    ), 200


@scores_bp.route("/<stock_code>/<date>/explanation", methods=["GET"])
def get_score_explanation(stock_code, date):
    horizon = _parse_horizon(request.args.get("horizon"), default=5)
    target_date = _parse_datetime(date)
    if horizon is None:
        return jsonify({"success": False, "message": "Unsupported horizon"}), 400
    if target_date is None:
        return jsonify({"success": False, "message": "Invalid date format"}), 400

    model_version = (request.args.get("model_version") or "").strip() or None
    query = StockScorePrediction.objects(
        stock_code=stock_code,
        date=target_date,
        horizon=horizon,
    )
    if model_version:
        query = query.filter(model_version=model_version)

    row = query.order_by("-generated_at").first()
    if row is None:
        return jsonify({"success": False, "message": "Score not found"}), 404

    return jsonify(_serialize_prediction(row, include_details=True)), 200
