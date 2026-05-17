# -*- coding: utf-8 -*-
"""Decision support API — score alerts and quality monitoring."""

import datetime
import logging
from collections import defaultdict
from typing import Dict

from flask import Blueprint, jsonify, request

from app.model.scoring import StockScorePrediction

logger = logging.getLogger(__name__)

decisions_bp = Blueprint("decisions", __name__, url_prefix="/api/decisions")


def _now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC).replace(hour=0, minute=0, second=0, microsecond=0)


# ---------------------------------------------------------------------------
# Score Alerts — detect significant score changes
# ---------------------------------------------------------------------------
@decisions_bp.route("/alerts", methods=["GET"])
def score_alerts():
    """Detect score jumps and strong signals for today.

    Query params:
        horizon     : int (default 20) — 5, 20, or 60
        jump_threshold : float (default 15) — min score increase to alert
        strong_threshold : float (default 80) — min absolute score for strong signal
        limit       : int (default 20)

    Returns alerts for: BUY signals with strong scores, and score jumps vs yesterday.
    """
    horizon = int(request.args.get("horizon", 20))
    jump_threshold = float(request.args.get("jump_threshold", 15))
    strong_threshold = float(request.args.get("strong_threshold", 80))
    limit = min(int(request.args.get("limit", 20)), 100)

    today = _now_utc()

    # Get today's predictions
    today_preds = list(
        StockScorePrediction.objects(
            date=today, horizon=horizon
        ).order_by("-score").limit(limit * 2)
    )

    # Get yesterday's predictions for jump detection
    yesterday = today - datetime.timedelta(days=1)
    # Find last trading day
    yesterday_preds_list = list(
        StockScorePrediction.objects(
            date__lte=yesterday, horizon=horizon
        ).order_by("-date").limit(limit * 2)
    )
    yesterday_date = yesterday_preds_list[0].date if yesterday_preds_list else None
    yesterday_scores = {}
    if yesterday_date:
        for p in yesterday_preds_list:
            if p.date == yesterday_date:
                yesterday_scores[p.stock_code] = p.score or 0

    alerts = []
    for pred in today_preds[:limit]:
        alert_type = None
        alert_detail = ""

        # Strong BUY signal
        if pred.score and pred.score >= strong_threshold and pred.recommendation == "BUY":
            alert_type = "strong_buy"
            alert_detail = f"Score{pred.horizon}={pred.score:.0f} — 强买入信号"

        # Score jump vs yesterday
        prev_score = yesterday_scores.get(pred.stock_code)
        if prev_score is not None and pred.score is not None:
            delta = pred.score - prev_score
            if delta >= jump_threshold:
                alert_type = alert_type or "score_jump"
                alert_detail = f"{alert_detail} (Δ+{delta:.0f} vs 昨日)".strip()

        if alert_type:
            alerts.append({
                "stock_code": pred.stock_code,
                "stock_name": pred.stock_name,
                "date": pred.date.isoformat() if pred.date else None,
                "horizon": pred.horizon,
                "score": pred.score,
                "previous_score": prev_score if prev_score is not None else None,
                "score_delta": round(pred.score - prev_score, 1) if prev_score is not None and pred.score is not None else None,
                "recommendation": pred.recommendation,
                "alert_type": alert_type,
                "alert_detail": alert_detail,
                "rank": pred.rank,
                "status": pred.status,
            })

    return jsonify({
        "success": True,
        "data": {
            "horizon": horizon,
            "date": today.isoformat(),
            "alerts": alerts,
            "total": len(alerts),
        }
    })


# ---------------------------------------------------------------------------
# Score Quality Monitoring — rolling hit rates and decay detection
# ---------------------------------------------------------------------------
@decisions_bp.route("/quality", methods=["GET"])
def score_quality():
    """Monitor score prediction quality with rolling metrics.

    Query params:
        horizon      : int (default 20)
        window_days  : int (default 30) — rolling window size
        lookback_days: int (default 90) — total lookback period
    """
    horizon = int(request.args.get("horizon", 20))
    window_days = int(request.args.get("window_days", 30))
    lookback_days = int(request.args.get("lookback_days", 90))

    today = _now_utc()
    start_date = today - datetime.timedelta(days=lookback_days * 2)  # generous range

    # Get all verified predictions in the lookback
    predictions = list(
        StockScorePrediction.objects(
            date__gte=start_date,
            horizon=horizon,
            status="VERIFIED",
        ).order_by("date")
    )

    if not predictions:
        return jsonify({"success": True, "data": {"rolling_hit_rates": [], "message": "No verified data"}})

    # Group by date
    by_date: Dict[datetime.datetime, list] = defaultdict(list)
    for p in predictions:
        by_date[p.date].append(p)

    sorted_dates = sorted(by_date.keys())

    # Compute daily hit rates
    daily_hit_rates = []
    for date in sorted_dates:
        day_preds = by_date[date]
        if not day_preds:
            continue
        hits = sum(1 for p in day_preds if (p.verification or {}).get("hit_target_close"))
        daily_hit_rates.append({
            "date": date.isoformat(),
            "count": len(day_preds),
            "hits": hits,
            "hit_rate": round(hits / len(day_preds), 4),
        })

    # Compute rolling window hit rates
    rolling_hit_rates = []
    for i in range(len(daily_hit_rates)):
        window = daily_hit_rates[max(0, i - window_days + 1):i + 1]
        total_count = sum(d["count"] for d in window)
        total_hits = sum(d["hits"] for d in window)
        if total_count > 0:
            rolling_hit_rates.append({
                "date": daily_hit_rates[i]["date"],
                "window_days": len(window),
                "total_predictions": total_count,
                "total_hits": total_hits,
                "rolling_hit_rate": round(total_hits / total_count, 4),
            })

    # Detect decay: compare latest window to historical baseline
    decay_detected = False
    decay_detail = ""
    if len(rolling_hit_rates) >= 2:
        latest = rolling_hit_rates[-1]["rolling_hit_rate"]
        baseline_window = rolling_hit_rates[:len(rolling_hit_rates) // 2]
        if baseline_window:
            baseline = sum(r["rolling_hit_rate"] for r in baseline_window) / len(baseline_window)
            decay = (baseline - latest) / max(baseline, 0.001)
            if decay > 0.15:  # 15% degradation threshold
                decay_detected = True
                decay_detail = f"Rolling hit rate degraded {decay*100:.1f}% vs baseline ({latest:.2%} vs {baseline:.2%})"

    return jsonify({
        "success": True,
        "data": {
            "horizon": horizon,
            "window_days": window_days,
            "rolling_hit_rates": rolling_hit_rates[-30:],  # last 30 windows
            "latest_hit_rate": rolling_hit_rates[-1]["rolling_hit_rate"] if rolling_hit_rates else None,
            "decay_detected": decay_detected,
            "decay_detail": decay_detail,
        }
    })
