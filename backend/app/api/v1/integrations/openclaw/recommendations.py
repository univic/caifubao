# -*- coding: utf-8 -*-
# Author : Gemini CLI
# Date: 2026-04-17

from flask import request
from app.api.v1.integrations.openclaw import openclaw_bp
from app.api.v1.integrations.openclaw.utils import wrap_response
from app.lib.auth_decorators import service_token_required
from app.model.scoring import StockDailyScore
from app.api.v1.quotes import _parse_datetime, _format_datetime


def _serialize_score_claw(score_doc):
    return {
        "stock_code": score_doc.stock_code,
        "stock_name": score_doc.stock_name,
        "date": _format_datetime(score_doc.date),
        "score": score_doc.score,
        "recommendation": score_doc.recommendation,
        "scoring_basis": score_doc.scoring_basis,
        "status": score_doc.status,
        "verification": {
            "base_price": score_doc.base_price,
            "target_date": _format_datetime(score_doc.target_date),
            "actual_price_t5": score_doc.actual_price_t5,
            "profit_percentage_t5": score_doc.profit_percentage_t5,
            "max_profit_percentage": score_doc.max_profit_percentage,
            "is_effective": score_doc.is_effective,
        }
        if score_doc.status == "VERIFIED"
        else None,
    }


@openclaw_bp.route("/recommendations/daily", methods=["GET"])
@service_token_required(scope="openclaw:data-read")
def get_daily_recommendations():
    """
    Get top recommendations for a specific date.
    """
    date = _parse_datetime(request.args.get("date"))
    min_score = float(request.args.get("min_score", 60.0))
    limit = int(request.args.get("limit", 20))

    query = {}
    if date:
        query["date"] = date

    query["score__gte"] = min_score

    scores = StockDailyScore.objects(**query).order_by("-score").limit(limit)

    return wrap_response(
        data={
            "items": [_serialize_score_claw(s) for s in scores],
            "total": scores.count(),
        }
    )


@openclaw_bp.route("/recommendations/performance", methods=["GET"])
@service_token_required(scope="openclaw:data-read")
def get_scoring_performance():
    """
    Get statistics on scoring effectiveness.
    """
    verified_scores = StockDailyScore.objects(status="VERIFIED")
    total = verified_scores.count()

    if total == 0:
        return wrap_response(data={"message": "No verified data yet"})

    effective = verified_scores.filter(is_effective=True).count()

    # Calculate average profit for top recommendations (score >= 60)
    top_verified = verified_scores.filter(score__gte=60)
    top_total = top_verified.count()
    avg_max_profit = 0.0
    if top_total > 0:
        avg_max_profit = top_verified.average("max_profit_percentage")

    return wrap_response(
        data={
            "total_verified": total,
            "effective_predictions": effective,
            "accuracy_rate": round(effective / total, 4) if total > 0 else 0,
            "top_recommendations_count": top_total,
            "avg_max_profit_top": round(avg_max_profit, 4) if avg_max_profit else 0,
        }
    )
