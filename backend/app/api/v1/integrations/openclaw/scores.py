# -*- coding: utf-8 -*-
# Author : Gemini CLI
# Date: 2026-05-17
"""OpenClaw score-prediction endpoints — read-only access to Score5/20/60 data."""

from flask import request

from app.api.v1.integrations.openclaw import openclaw_bp
from app.api.v1.integrations.openclaw.utils import _get_latest_date, wrap_response
from app.api.v1.quotes import _format_datetime, _parse_datetime
from app.lib.auth_decorators import service_token_required
from app.model.scoring import StockScorePrediction


def _parse_horizon(value):
    """Parse horizon parameter, returning int or None for invalid."""
    try:
        horizon = int(value)
    except (TypeError, ValueError):
        return None
    if horizon in {5, 20, 60}:
        return horizon
    return None


def _serialize_score_claw(score_doc):
    """Serialize a StockScorePrediction for the OpenClaw response."""
    verification = score_doc.verification or {}
    return {
        "stock_code": score_doc.stock_code,
        "stock_name": score_doc.stock_name,
        "date": _format_datetime(score_doc.date),
        "horizon": score_doc.horizon,
        "score": score_doc.score,
        "rank": score_doc.rank,
        "percentile": score_doc.percentile,
        "recommendation": score_doc.recommendation,
        "status": score_doc.status,
        "base_price": score_doc.base_price,
        "target_date": _format_datetime(score_doc.target_date),
        "model_version": score_doc.model_version,
        "explanation": score_doc.explanation or {},
        "input_snapshot": score_doc.input_snapshot or {},
        "verification": {
            "hit_target_close": verification.get("hit_target_close"),
            "hit_target_intra": verification.get("hit_target_intra"),
            "return_at_target": verification.get("return_at_target"),
            "max_return": verification.get("max_return"),
            "min_return": verification.get("min_return"),
            "max_drawdown": verification.get("max_drawdown"),
            "days_to_max_return": verification.get("days_to_max_return"),
            "quote_count": verification.get("quote_count"),
        }
        if verification
        else None,
    }


@openclaw_bp.route("/scores", methods=["GET"])
@service_token_required(scope=["openclaw:score-read", "openclaw:data-read"])
def get_scores():
    """
    Get score predictions for OpenClaw.
    Query params:
        date          - YYYY-MM-DD filter by evaluation date
        horizon       - 5, 20, or 60
        stock_code    - filter by stock code
        model_version - filter by model version
        status        - filter by verification status
        page          - page number (default 1)
        per_page      - items per page (default 100, max 500)
    """
    date = _parse_datetime(request.args.get("date"))
    horizon = _parse_horizon(request.args.get("horizon"))
    stock_code = (request.args.get("stock_code") or "").strip() or None
    model_version = (request.args.get("model_version") or "").strip() or None
    status = (request.args.get("status") or "").strip() or None
    page = max(1, int(request.args.get("page", 1) or 1))
    per_page = min(max(1, int(request.args.get("per_page", 100) or 100)), 500)

    query = {}
    if date:
        query["date"] = date
    if horizon:
        query["horizon"] = horizon
    if stock_code:
        query["stock_code"] = stock_code
    if model_version:
        query["model_version"] = model_version
    if status:
        query["status"] = status

    # Count total before pagination
    qs = StockScorePrediction.objects(**query).order_by("-date", "-score")
    total = qs.count()

    items = qs.skip((page - 1) * per_page).limit(per_page)

    data_as_of = (
        _get_latest_date(StockScorePrediction, filter_kwargs=query) if query else None
    )

    return wrap_response(
        data={
            "items": [_serialize_score_claw(s) for s in items],
            "total": total,
            "page": page,
            "per_page": per_page,
        },
        data_as_of=data_as_of,
    )
