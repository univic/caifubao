# -*- coding: utf-8 -*-
# Score-strategy backtest and calibration APIs.
# Bridges scoring predictions with actionable investment decisions.

import datetime
import math
from collections import defaultdict

from flask import Blueprint, jsonify, request

from app.model.scoring import StockScorePrediction

score_strategies_bp = Blueprint(
    "score_strategies", __name__, url_prefix="/api/score-strategies"
)

SUPPORTED_HORIZONS = {5, 20, 60}
SCORE_BUCKETS = ((0, 20), (20, 40), (40, 60), (60, 80), (80, 100))
DEFAULT_MODEL_VERSION = "score_v2_202605b"


def _parse_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime.datetime):
        return value.replace(hour=0, minute=0, second=0, microsecond=0)
    text = str(value).strip().replace("Z", "+00:00")
    if not text:
        return None
    if len(text) == 10:
        text = f"{text}T00:00:00"
    try:
        parsed = datetime.datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed.replace(hour=0, minute=0, second=0, microsecond=0)


def _format_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    return str(value)


def _parse_int(value, default, minimum=0, maximum=10000):
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


def _parse_horizon(value, default=5):
    horizon = _parse_int(value, default, minimum=1, maximum=999)
    return horizon if horizon in SUPPORTED_HORIZONS else None


def _avg(values):
    filtered = [v for v in values if v is not None]
    if not filtered:
        return None
    return round(sum(filtered) / len(filtered), 6)


def _median(values):
    filtered = sorted([v for v in values if v is not None])
    if not filtered:
        return None
    n = len(filtered)
    if n % 2 == 1:
        return round(filtered[n // 2], 6)
    return round((filtered[n // 2 - 1] + filtered[n // 2]) / 2, 6)


@score_strategies_bp.route("/backtest", methods=["POST"])
def run_score_backtest():
    body = request.get_json(silent=True) or {}
    horizon = _parse_horizon(body.get("horizon"), default=5)
    if horizon is None:
        return jsonify({"success": False, "message": "Unsupported horizon"}), 400
    top_n = _parse_int(body.get("top_n"), 10, minimum=1, maximum=50)
    start_date = _parse_datetime(body.get("start_date"))
    end_date = _parse_datetime(body.get("end_date"))
    model_version = (body.get("model_version") or DEFAULT_MODEL_VERSION).strip()
    if not start_date or not end_date:
        return jsonify(
            {"success": False, "message": "start_date and end_date are required"}
        ), 400
    if start_date > end_date:
        return jsonify(
            {"success": False, "message": "start_date must be <= end_date"}
        ), 400

    predictions = list(
        StockScorePrediction.objects(
            date__gte=start_date,
            date__lte=end_date,
            horizon=horizon,
            model_version=model_version,
            status="VERIFIED",
        ).order_by("date", "-score")
    )
    if not predictions:
        return jsonify(
            {
                "success": False,
                "message": (
                    f"No VERIFIED predictions found for horizon={horizon}, "
                    f"model_version={model_version}"
                ),
            }
        ), 404

    by_date: dict = defaultdict(list)
    for p in predictions:
        by_date[p.date].append(p)

    daily_results = []
    all_returns = []
    hit_count = 0
    total_positions = 0

    for date_key in sorted(by_date.keys()):
        day_predictions = sorted(
            by_date[date_key], key=lambda p: p.score or 0, reverse=True
        )[:top_n]
        day_returns = [
            (p.verification or {}).get("return_at_target") for p in day_predictions
        ]
        day_returns = [r for r in day_returns if r is not None]
        day_max_returns = [
            (p.verification or {}).get("max_return") for p in day_predictions
        ]
        day_max_returns = [r for r in day_max_returns if r is not None]
        day_hits = [(p.verification or {}).get("hit_target") for p in day_predictions]
        day_hits = [bool(h) for h in day_hits if h is not None]
        if not day_returns:
            continue

        avg_return = _avg(day_returns)
        avg_max_return = _avg(day_max_returns)
        day_hit_rate = round(sum(day_hits) / len(day_hits), 4) if day_hits else None

        daily_results.append(
            {
                "date": _format_datetime(date_key),
                "position_count": len(day_predictions),
                "avg_return": avg_return,
                "avg_max_return": avg_max_return,
                "hit_rate": day_hit_rate,
                "top_stocks": [
                    {
                        "stock_code": p.stock_code,
                        "stock_name": p.stock_name,
                        "score": p.score,
                        "return_at_target": (p.verification or {}).get(
                            "return_at_target"
                        ),
                        "hit_target": (p.verification or {}).get("hit_target"),
                    }
                    for p in day_predictions[:5]
                ],
            }
        )
        all_returns.extend(day_returns)
        total_positions += len(day_predictions)
        hit_count += sum(day_hits)

    if not daily_results:
        return jsonify(
            {"success": False, "message": "No days with sufficient data"}
        ), 404

    total_days = len(daily_results)
    avg_daily_return = _avg(all_returns)
    overall_hit_rate = (
        round(hit_count / total_positions, 4) if total_positions else None
    )

    cum_return = 1.0
    equity_curve = [{"date": _format_datetime(start_date), "value": 1.0}]
    for day in daily_results:
        daily_contrib = (day["avg_return"] or 0) / horizon
        cum_return *= 1 + daily_contrib
        equity_curve.append({"date": day["date"], "value": round(cum_return, 6)})

    total_return = round(cum_return - 1, 6)
    year_span = max((end_date - start_date).days, 1) / 365.25
    annualized_return = round((cum_return ** (1 / max(year_span, 0.01))) - 1, 6)

    peak = 1.0
    max_dd = 0.0
    for point in equity_curve:
        peak = max(peak, point["value"])
        dd = (point["value"] - peak) / peak
        max_dd = min(max_dd, dd)

    daily_rets = [(day["avg_return"] or 0) / horizon for day in daily_results]
    if daily_rets:
        mean_ret = _avg(daily_rets)
        std_ret = (
            math.sqrt(sum((r - mean_ret) ** 2 for r in daily_rets) / len(daily_rets))
            if len(daily_rets) > 1
            else 0.001
        )
        sharpe = round(mean_ret / max(std_ret, 0.0001) * math.sqrt(252), 4)
    else:
        sharpe = None

    win_days = sum(1 for day in daily_results if (day["avg_return"] or 0) > 0)
    win_rate = round(win_days / total_days, 4) if total_days else None

    return jsonify(
        {
            "success": True,
            "strategy": {
                "horizon": horizon,
                "top_n": top_n,
                "model_version": model_version,
                "start_date": _format_datetime(start_date),
                "end_date": _format_datetime(end_date),
            },
            "summary": {
                "total_trading_days": total_days,
                "total_positions": total_positions,
                "avg_return_per_position": avg_daily_return,
                "overall_hit_rate": overall_hit_rate,
                "total_return": total_return,
                "annualized_return": annualized_return,
                "max_drawdown": round(max_dd, 6),
                "sharpe_ratio": sharpe,
                "win_rate": win_rate,
            },
            "equity_curve": equity_curve,
            "daily_results": daily_results,
        }
    ), 200


@score_strategies_bp.route("/calibration", methods=["GET"])
def get_calibration():
    horizon = _parse_horizon(request.args.get("horizon"), default=5)
    if horizon is None:
        return jsonify({"success": False, "message": "Unsupported horizon"}), 400
    model_version = (request.args.get("model_version") or DEFAULT_MODEL_VERSION).strip()
    days = _parse_int(request.args.get("days"), 180, minimum=30, maximum=365 * 5)

    end_dt = datetime.datetime.now(datetime.UTC).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    start_dt = end_dt - datetime.timedelta(days=days)

    predictions = list(
        StockScorePrediction.objects(
            date__gte=start_dt,
            date__lte=end_dt,
            horizon=horizon,
            model_version=model_version,
            status="VERIFIED",
        )
    )
    if not predictions:
        return jsonify(
            {
                "success": True,
                "horizon": horizon,
                "model_version": model_version,
                "lookback_days": days,
                "prediction_count": 0,
                "buckets": [],
                "message": "No verified predictions in lookback window",
            }
        ), 200

    buckets = []
    for low, high in SCORE_BUCKETS:
        items = [
            p
            for p in predictions
            if low <= (p.score or 0) < high or (high == 100 and (p.score or 0) == 100)
        ]
        returns = [(p.verification or {}).get("return_at_target") for p in items]
        returns = [r for r in returns if r is not None]
        max_returns = [(p.verification or {}).get("max_return") for p in items]
        max_returns = [r for r in max_returns if r is not None]
        hits = [(p.verification or {}).get("hit_target") for p in items]
        hits = [bool(h) for h in hits if h is not None]
        stop_loss_hits = [(p.verification or {}).get("hit_stop_loss") for p in items]
        stop_loss_hits = [bool(h) for h in stop_loss_hits if h is not None]

        hit_rate = round(sum(hits) / len(hits), 4) if hits else None
        if hit_rate is not None:
            if hit_rate >= 0.60:
                confidence = "high"
            elif hit_rate >= 0.40:
                confidence = "medium"
            else:
                confidence = "low"
        else:
            confidence = None

        if len(items) >= 5 and returns:
            stop_loss = _median(
                [(p.verification or {}).get("max_drawdown") for p in items]
            )
            take_profit = _median(max_returns)
        else:
            stop_loss = None
            take_profit = None

        buckets.append(
            {
                "bucket": f"{low}-{high}",
                "count": len(items),
                "avg_score": _avg([p.score for p in items]),
                "avg_return": _avg(returns),
                "avg_max_return": _avg(max_returns),
                "hit_rate": hit_rate,
                "stop_loss_hit_rate": (
                    round(sum(stop_loss_hits) / len(stop_loss_hits), 4)
                    if stop_loss_hits
                    else None
                ),
                "confidence": confidence,
                "suggested_stop_loss": stop_loss,
                "suggested_take_profit": take_profit,
            }
        )

    all_returns = [(p.verification or {}).get("return_at_target") for p in predictions]
    all_returns = [r for r in all_returns if r is not None]
    all_hits = [(p.verification or {}).get("hit_target") for p in predictions]
    all_hits = [bool(h) for h in all_hits if h is not None]

    return jsonify(
        {
            "success": True,
            "horizon": horizon,
            "model_version": model_version,
            "lookback_days": days,
            "prediction_count": len(predictions),
            "overall": {
                "avg_return": _avg(all_returns),
                "hit_rate": (
                    round(sum(all_hits) / len(all_hits), 4) if all_hits else None
                ),
            },
            "buckets": buckets,
        }
    ), 200


@score_strategies_bp.route("/confidence", methods=["GET"])
def get_confidence():
    stock_code = (request.args.get("stock_code") or "").strip()
    date = _parse_datetime(request.args.get("date"))
    horizon = _parse_horizon(request.args.get("horizon"), default=5)
    model_version = (request.args.get("model_version") or DEFAULT_MODEL_VERSION).strip()
    if not stock_code or not date:
        return jsonify(
            {"success": False, "message": "stock_code and date are required"}
        ), 400
    if horizon is None:
        return jsonify({"success": False, "message": "Unsupported horizon"}), 400

    prediction = StockScorePrediction.objects(
        stock_code=stock_code,
        date=date,
        horizon=horizon,
        model_version=model_version,
    ).first()
    if prediction is None:
        return jsonify(
            {
                "success": False,
                "message": f"No prediction found for {stock_code} on {date.date()}",
            }
        ), 404

    score = prediction.score or 0

    bucket_low = 0
    bucket_high = 20
    for low, high in SCORE_BUCKETS:
        if low <= score < high or (high == 100 and score == 100):
            bucket_low = low
            bucket_high = high
            break

    bucket_label = f"{bucket_low}-{bucket_high}"
    verified = list(
        StockScorePrediction.objects(
            horizon=horizon,
            model_version=model_version,
            status="VERIFIED",
        )
    )

    bucket_items = [
        p
        for p in verified
        if bucket_low <= (p.score or 0) < bucket_high
        or (bucket_high == 100 and (p.score or 0) == 100)
    ]
    bucket_count = len(bucket_items)

    bucket_confidence = None
    bucket_hit_rate = None
    if bucket_items:
        hits = [(p.verification or {}).get("hit_target") for p in bucket_items]
        hits = [bool(h) for h in hits if h is not None]
        if hits:
            bucket_hit_rate = round(sum(hits) / len(hits), 4)
            if bucket_hit_rate >= 0.60:
                bucket_confidence = "high"
            elif bucket_hit_rate >= 0.40:
                bucket_confidence = "medium"
            else:
                bucket_confidence = "low"

    trade_suggestions = None
    if bucket_count >= 5:
        drawdowns = [(p.verification or {}).get("max_drawdown") for p in bucket_items]
        drawdowns = [d for d in drawdowns if d is not None]
        max_rets = [(p.verification or {}).get("max_return") for p in bucket_items]
        max_rets = [r for r in max_rets if r is not None]
        if max_rets:
            trade_suggestions = {
                "stop_loss": _median(drawdowns),
                "take_profit": _median(max_rets),
                "basis": (
                    f"Based on {bucket_count} verified predictions in "
                    f"score bucket {bucket_label} (horizon={horizon}, median)"
                ),
            }

    return jsonify(
        {
            "success": True,
            "stock_code": stock_code,
            "date": _format_datetime(date),
            "horizon": horizon,
            "score": score,
            "score_bucket": bucket_label,
            "confidence": bucket_confidence,
            "bucket_hit_rate": bucket_hit_rate,
            "bucket_sample_count": bucket_count,
            "trade_suggestions": trade_suggestions,
            "prediction_status": prediction.status,
            "prediction_verification": (
                {
                    "return_at_target": (prediction.verification or {}).get(
                        "return_at_target"
                    ),
                    "max_return": (prediction.verification or {}).get("max_return"),
                    "hit_target": (prediction.verification or {}).get("hit_target"),
                }
                if prediction.status == "VERIFIED"
                else None
            ),
        }
    ), 200
