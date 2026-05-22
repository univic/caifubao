# -*- coding: utf-8 -*-
# Author : Gemini CLI
# Date: 2026-04-17

from flask import request
from app.api.v1.integrations.openclaw import openclaw_bp
from app.api.v1.integrations.openclaw.utils import _get_latest_date, wrap_response
from app.lib.auth_decorators import service_token_required
from app.model.scoring import StockScorePrediction
from app.api.v1.quotes import _parse_datetime, _format_datetime


def _parse_horizon(value, default=5):
    try:
        horizon = int(value)
    except (TypeError, ValueError):
        return None
    if horizon in {5, 20, 60}:
        return horizon
    return default if value in (None, "") else None


def _serialize_score_claw(score_doc):
    return {
        "stock_code": score_doc.stock_code,
        "stock_name": score_doc.stock_name,
        "date": _format_datetime(score_doc.date),
        "horizon": score_doc.horizon,
        "score": score_doc.score,
        "rank": score_doc.rank,
        "percentile": score_doc.percentile,
        "recommendation": score_doc.recommendation,
        "explanation": score_doc.explanation or {},
        "input_snapshot": score_doc.input_snapshot or {},
        "status": score_doc.status,
        "base_price": score_doc.base_price,
        "target_date": _format_datetime(score_doc.target_date),
        "verification": score_doc.verification or None,
        "model_version": score_doc.model_version,
    }


@openclaw_bp.route("/recommendations/daily", methods=["GET"])
@service_token_required(scope="openclaw:data-read")
def get_daily_recommendations():
    """
    Get top recommendations for a specific date.
    """
    date = _parse_datetime(request.args.get("date"))
    horizon = _parse_horizon(request.args.get("horizon"), default=5)
    min_score = float(request.args.get("min_score", 60.0))
    limit = int(request.args.get("limit", 20))
    model_version = (request.args.get("model_version") or "").strip() or None

    if horizon is None:
        return wrap_response(
            data={"message": "Unsupported horizon. Use 5, 20, or 60."},
            success=False,
        ), 400

    query = {"horizon": horizon}
    if date:
        query["date"] = date
    if model_version:
        query["model_version"] = model_version

    query["score__gte"] = min_score

    scores = StockScorePrediction.objects(**query).order_by("-score").limit(limit)

    data_as_of = _get_latest_date(StockScorePrediction, filter_kwargs=query)

    return wrap_response(
        data={
            "items": [_serialize_score_claw(s) for s in scores],
            "total": scores.count(),
            "horizon": horizon,
        },
        data_as_of=data_as_of,
    )


@openclaw_bp.route("/recommendations/performance", methods=["GET"])
@service_token_required(scope="openclaw:data-read")
def get_scoring_performance():
    """
    Get statistics on scoring effectiveness.
    """
    horizon = _parse_horizon(request.args.get("horizon"), default=5)
    if horizon is None:
        return wrap_response(
            data={"message": "Unsupported horizon. Use 5, 20, or 60."},
            success=False,
        ), 400
    query = {"status": "VERIFIED"}
    query["horizon"] = horizon
    model_version = (request.args.get("model_version") or "").strip() or None
    if model_version:
        query["model_version"] = model_version

    verified_scores = StockScorePrediction.objects(**query)
    total = verified_scores.count()

    if total == 0:
        return wrap_response(data={"message": "No verified data yet"})

    effective = sum(
        1
        for item in verified_scores
        if (item.verification or {}).get("hit_target_close")
    )

    # Calculate average profit for top recommendations (score >= 60)
    top_verified = verified_scores.filter(score__gte=60)
    top_total = top_verified.count()
    max_returns = [
        (item.verification or {}).get("max_return")
        for item in top_verified
        if (item.verification or {}).get("max_return") is not None
    ]
    avg_max_profit = 0.0
    if top_total > 0:
        avg_max_profit = sum(max_returns) / len(max_returns) if max_returns else 0.0

    return wrap_response(
        data={
            "total_verified": total,
            "effective_predictions": effective,
            "accuracy_rate": round(effective / total, 4) if total > 0 else 0,
            "top_recommendations_count": top_total,
            "avg_max_profit_top": round(avg_max_profit, 4) if avg_max_profit else 0,
        },
        data_as_of=_get_latest_date(StockScorePrediction, filter_kwargs=query),
    )
