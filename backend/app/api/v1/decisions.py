# -*- coding: utf-8 -*-
"""Decision support API — score alerts, quality monitoring, daily dashboard."""

import datetime
import logging
from collections import defaultdict
from typing import Dict

from flask import Blueprint, jsonify, request
from flask_jwt_extended import get_jwt_identity

from app.model.scoring import StockScorePrediction
from app.model.stock import StockDailyQuote, IndividualStock
from app.model.decision_journal import DecisionJournal
from app.model.watchlist import Watchlist
from app.lib.auth_decorators import block_service_tokens

logger = logging.getLogger(__name__)

# Backend-local default (mirror score_strategies.py). Kept here instead of
# importing from datahub's app.lib.scoring_engine.config: backend API tests
# and the API package must not depend on the datahub library at module load.
# Registry validation in _requested_model_version() still guards overrides.
DEFAULT_MODEL_VERSION = "score_v2_202605b"

decisions_bp = Blueprint("decisions", __name__, url_prefix="/api/decisions")
decisions_bp.before_request(block_service_tokens)


class _ModelVersionError(Exception):
    """Raised when an explicit ?model_version= override is not usable."""


def _resolve_model_version():
    """Return (model_version, None) or (None, jsonify(400)) on bad override."""
    try:
        return _requested_model_version(), None
    except _ModelVersionError as exc:
        return None, (
            jsonify({"success": False, "message": str(exc), "data": None}),
            400,
        )


def _requested_model_version() -> str:
    """Resolve the model version for a production-view query.

    Defaults to DEFAULT_MODEL_VERSION so the dashboard / alerts / quality
    views never mix predictions across scoring model versions (a flipped
    construction-layer version would otherwise interleave with the default
    and invert rankings silently). An explicit ?model_version= override is
    allowed for inspection, but it must match a known version name; unknown
    labels raise _ModelVersionError so the view can respond JSON 400 (not an
    unhandled 500).
    """
    return _validate_model_version(request.args.get("model_version") or "")


def _validate_model_version(raw_value) -> str:
    """Validate an explicit model_version override ('' means default)."""
    override = str(raw_value or "").strip()
    if override and override != DEFAULT_MODEL_VERSION:
        try:
            from app.model.scoring import ScoreModelVersion

            known = ScoreModelVersion.objects(model_version=override).first()
        except Exception:  # noqa: BLE001 - registry best-effort
            known = None
        if known is None:
            raise _ModelVersionError(
                f"unknown model_version {override!r} (not registered); "
                f"omit the parameter to use {DEFAULT_MODEL_VERSION!r}"
            )
    return override or DEFAULT_MODEL_VERSION


def _now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC).replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=None
    )


def _latest_score_date(horizon: int, model_version: str) -> datetime.datetime | None:
    """Return most recent date with score predictions for a horizon/version."""
    pred = (
        StockScorePrediction.objects(horizon=horizon, model_version=model_version)
        .order_by("-date")
        .only("date")
        .first()
    )
    if pred and pred.date:
        return pred.date.replace(hour=0, minute=0, second=0, microsecond=0)
    return None


def _get_horizon_config(horizon: int) -> dict:
    """Return per-horizon config for invalidation conditions."""
    try:
        from app.lib.scoring_engine.config import get_horizon_config

        return get_horizon_config(horizon)
    except Exception:
        return {}


def _confidence_metadata(predictions: list, current_score: float) -> dict:
    """Confidence from same-bucket hit rate over last 90 days.

    Trend compares most recent 30 days vs prior 60 days.
    """
    if not predictions:
        return {
            "confidence": "low",
            "sample_size": 0,
            "hit_rate": None,
            "trend": "stable",
        }

    bucket_low = max(0, (current_score or 0) - 10)
    bucket_high = min(100, (current_score or 0) + 10)
    bucket = [p for p in predictions if bucket_low <= (p.score or 0) <= bucket_high]

    cutoff = _now_utc() - datetime.timedelta(days=30)
    recent = [p for p in bucket if p.date and p.date >= cutoff]
    older = [p for p in bucket if p.date and p.date < cutoff]

    def _hit_rate(ps):
        if not ps:
            return None
        hits = sum(1 for p in ps if (p.verification or {}).get("hit_target_close"))
        return round(hits / len(ps), 4)

    overall_hr = _hit_rate(bucket)
    recent_hr = _hit_rate(recent)
    older_hr = _hit_rate(older)
    trend = (
        "improving"
        if recent_hr is not None and older_hr is not None and recent_hr > older_hr
        else "declining"
        if recent_hr is not None and older_hr is not None and recent_hr < older_hr
        else "stable"
    )

    return {
        "confidence": (
            "high"
            if len(bucket) >= 20 and (recent_hr or 0) >= 0.5
            else "medium"
            if len(bucket) >= 10
            else "low"
        ),
        "sample_size": len(bucket),
        "hit_rate": overall_hr,
        "recent_hit_rate": recent_hr,
        "trend": trend,
    }


def _position_sizing(stock_code: str, portfolio_cash: float = 100_000.0) -> dict:
    """Compute position sizing suggestion for a stock.

    Uses preflight_check for trading status and liquidity, then computes
    max shares based on a 10 % position cap.
    """
    from app.services.backtest_service import preflight_check

    flight = preflight_check(stock_code, check_liquidity_flag=True)
    capacity_ok = flight.get("pass", False)

    latest = (
        StockDailyQuote.objects(code=stock_code)
        .order_by("-date")
        .only("close_hfq", "close", "turnover_rate", "volume")
        .first()
    )
    close_price = 0.0
    if latest:
        close_price = latest.close_hfq or latest.close or 0.0

    max_shares = 0
    if close_price > 0:
        target_amount = portfolio_cash * 0.10  # 10% position cap
        raw_shares = int(target_amount / close_price)
        max_shares = (raw_shares // 100) * 100  # round down to 100-share lots

    return {
        "target_weight_pct": 10.0,
        "max_shares": max(max_shares, 0),
        "capacity_check": capacity_ok,
        "current_price": round(close_price, 2) if close_price else None,
    }


# ---------------------------------------------------------------------------
# Daily Decision Dashboard
# ---------------------------------------------------------------------------
@decisions_bp.route("/dashboard", methods=["GET"])
def daily_dashboard():
    """Comprehensive daily decision support.

    Uses latest available score date per horizon (not exact UTC midnight)
    so dashboard works on weekends, before scoring runs, or when data lags.

    Query params:
        horizon : int (default 5) — primary horizon for ranking
        limit   : int (default 20) — top-N per horizon
    """
    horizon = int(request.args.get("horizon", 5))
    n_limit = min(int(request.args.get("limit", 20)), 100)
    model_version, version_error = _resolve_model_version()
    if version_error is not None:
        return version_error

    def _load_for(h: int) -> dict:
        score_date = _latest_score_date(h, model_version)
        if not score_date:
            return {"horizon": h, "date": None, "count": 0, "items": []}

        today_preds = list(
            StockScorePrediction.objects(
                date=score_date, horizon=h, model_version=model_version
            )
            .order_by("-score")
            .limit(n_limit)
        )

        # Previous score date — fetch all scores for today's stock codes
        prev = (
            StockScorePrediction.objects(
                date__lt=score_date, horizon=h, model_version=model_version
            )
            .order_by("-date")
            .only("date")
            .first()
        )
        prev_date = prev.date if prev else None
        yday_scores = {}
        if prev_date:
            today_codes = [p.stock_code for p in today_preds]
            for p in StockScorePrediction.objects(
                date=prev_date,
                horizon=h,
                stock_code__in=today_codes,
                model_version=model_version,
            ):
                yday_scores[p.stock_code] = p

        # Historical VERIFIED for confidence
        conf_start = score_date - datetime.timedelta(days=90)
        verified = list(
            StockScorePrediction.objects(
                horizon=h,
                status="VERIFIED",
                date__gte=conf_start,
                model_version=model_version,
            )
        )

        cfg = _get_horizon_config(h)
        items = []
        for pred in today_preds:
            prev_p = yday_scores.get(pred.stock_code)
            prev_score = prev_p.score if prev_p else None
            delta = (
                round((pred.score or 0) - (prev_score or 0), 1)
                if prev_score is not None
                else None
            )
            conf = _confidence_metadata(verified, pred.score)

            sizing = _position_sizing(pred.stock_code)
            items.append(
                {
                    "stock_code": pred.stock_code,
                    "stock_name": pred.stock_name,
                    "score": pred.score,
                    "rank": pred.rank,
                    "recommendation": pred.recommendation,
                    "previous_score": prev_score,
                    "score_delta": delta,
                    "base_price": pred.base_price,
                    "status": pred.status,
                    "confidence": conf["confidence"],
                    "sample_size": conf["sample_size"],
                    "hit_rate": conf["hit_rate"],
                    "trend": conf["trend"],
                    "position_sizing": sizing,
                    "invalidation": {
                        "exit_threshold": cfg.get("watch_threshold", 50),
                        "stop_loss_pct": cfg.get("stop_loss_threshold", -5.0),
                        "expiry_days": h,
                    },
                }
            )

        return {
            "horizon": h,
            "date": score_date.isoformat(),
            "count": len(items),
            "items": items,
        }

    data = {
        "primary_horizon": horizon,
        "score5": _load_for(5),
        "score20": _load_for(20),
        "score60": _load_for(60),
    }

    return jsonify({"success": True, "data": data})


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
    """
    horizon = int(request.args.get("horizon", 20))
    jump_threshold = float(request.args.get("jump_threshold", 15))
    strong_threshold = float(request.args.get("strong_threshold", 80))
    limit = min(int(request.args.get("limit", 20)), 100)
    model_version, version_error = _resolve_model_version()
    if version_error is not None:
        return version_error

    today = _now_utc()

    today_preds = list(
        StockScorePrediction.objects(
            date=today, horizon=horizon, model_version=model_version
        )
        .order_by("-score")
        .limit(limit * 2)
    )

    yesterday = today - datetime.timedelta(days=1)
    yesterday_preds_list = list(
        StockScorePrediction.objects(
            date__lte=yesterday, horizon=horizon, model_version=model_version
        )
        .order_by("-date")
        .limit(limit * 2)
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

        if (
            pred.score
            and pred.score >= strong_threshold
            and pred.recommendation == "BUY"
        ):
            alert_type = "strong_buy"
            alert_detail = f"Score{pred.horizon}={pred.score:.0f} 强买入信号"

        prev_score = yesterday_scores.get(pred.stock_code)
        if prev_score is not None and pred.score is not None:
            delta = pred.score - prev_score
            if delta >= jump_threshold:
                alert_type = alert_type or "score_jump"
                alert_detail = f"{alert_detail} (+{delta:.0f} vs 昨日)".strip()

        if alert_type:
            alerts.append(
                {
                    "stock_code": pred.stock_code,
                    "stock_name": pred.stock_name,
                    "date": pred.date.isoformat() if pred.date else None,
                    "horizon": pred.horizon,
                    "score": pred.score,
                    "previous_score": prev_score if prev_score is not None else None,
                    "score_delta": round(pred.score - prev_score, 1)
                    if prev_score is not None and pred.score is not None
                    else None,
                    "recommendation": pred.recommendation,
                    "alert_type": alert_type,
                    "alert_detail": alert_detail,
                    "rank": pred.rank,
                    "status": pred.status,
                }
            )

    return jsonify(
        {
            "success": True,
            "data": {
                "horizon": horizon,
                "date": today.isoformat(),
                "alerts": alerts,
                "total": len(alerts),
            },
        }
    )


# ---------------------------------------------------------------------------
# Score Quality Monitoring — rolling hit rates, decay, drift
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
    model_version, version_error = _resolve_model_version()
    if version_error is not None:
        return version_error

    today = _now_utc()
    start_date = today - datetime.timedelta(days=lookback_days * 2)

    predictions = list(
        StockScorePrediction.objects(
            date__gte=start_date,
            horizon=horizon,
            status="VERIFIED",
            model_version=model_version,
        ).order_by("date")
    )

    if not predictions:
        return jsonify(
            {
                "success": True,
                "data": {"rolling_hit_rates": [], "message": "No verified data"},
            }
        )

    by_date: Dict[datetime.datetime, list] = defaultdict(list)
    for p in predictions:
        by_date[p.date].append(p)

    sorted_dates = sorted(by_date.keys())

    daily_hit_rates = []
    for date in sorted_dates:
        day_preds = by_date[date]
        if not day_preds:
            continue
        hits = sum(
            1 for p in day_preds if (p.verification or {}).get("hit_target_close")
        )
        daily_hit_rates.append(
            {
                "date": date.isoformat(),
                "count": len(day_preds),
                "hits": hits,
                "hit_rate": round(hits / len(day_preds), 4),
            }
        )

    rolling_hit_rates = []
    for i in range(len(daily_hit_rates)):
        window = daily_hit_rates[max(0, i - window_days + 1) : i + 1]
        total_count = sum(d["count"] for d in window)
        total_hits = sum(d["hits"] for d in window)
        if total_count > 0:
            rolling_hit_rates.append(
                {
                    "date": daily_hit_rates[i]["date"],
                    "window_days": len(window),
                    "total_predictions": total_count,
                    "total_hits": total_hits,
                    "rolling_hit_rate": round(total_hits / total_count, 4),
                }
            )

    # Decay detection
    decay_detected = False
    decay_detail = ""
    if len(rolling_hit_rates) >= 2:
        latest = rolling_hit_rates[-1]["rolling_hit_rate"]
        baseline_window = rolling_hit_rates[: len(rolling_hit_rates) // 2]
        if baseline_window:
            baseline = sum(r["rolling_hit_rate"] for r in baseline_window) / len(
                baseline_window
            )
            decay = (baseline - latest) / max(baseline, 0.001)
            if decay > 0.15:
                decay_detected = True
                decay_detail = (
                    f"Rolling hit rate degraded {decay * 100:.1f}% "
                    f"vs baseline ({latest:.2%} vs {baseline:.2%})"
                )

    # Model drift: score distribution shift
    drift_detected = False
    drift_detail = ""
    recent_preds = [
        p
        for p in predictions
        if p.date and p.date >= today - datetime.timedelta(days=20)
    ]
    older_preds = [
        p
        for p in predictions
        if p.date and p.date < today - datetime.timedelta(days=20)
    ]
    if recent_preds and older_preds:

        def _pct(pred_list, p):
            scores = sorted([s.score or 0 for s in pred_list])
            if not scores:
                return 0
            idx = int(len(scores) * p / 100)
            return scores[min(idx, len(scores) - 1)]

        rp50 = _pct(recent_preds, 50)
        op50 = _pct(older_preds, 50)
        rp90 = _pct(recent_preds, 90)
        op90 = _pct(older_preds, 90)
        if abs(rp50 - op50) > 10 or abs(rp90 - op90) > 10:
            drift_detected = True
            drift_detail = (
                f"Score distribution shift: P50 {op50}->{rp50} "
                f"({rp50 - op50:+d}), P90 {op90}->{rp90} ({rp90 - op90:+d})"
            )

    return jsonify(
        {
            "success": True,
            "data": {
                "horizon": horizon,
                "window_days": window_days,
                "rolling_hit_rates": rolling_hit_rates[-30:],
                "latest_hit_rate": rolling_hit_rates[-1]["rolling_hit_rate"]
                if rolling_hit_rates
                else None,
                "decay_detected": decay_detected,
                "decay_detail": decay_detail,
                "drift_detected": drift_detected,
                "drift_detail": drift_detail,
            },
        }
    )


# ---------------------------------------------------------------------------
# Rebalance Preview
# ---------------------------------------------------------------------------
@decisions_bp.route("/rebalance-preview", methods=["POST"])
def rebalance_preview():
    """Suggest portfolio adjustments based on current scores.

    Request body (JSON):
        portfolio_stocks : list[str] — current holdings (stock codes)
        cash             : float     — available cash (default 100000)
        model_version    : str       — scoring model version (default: the
                                       production default; must be registered
                                       if provided)
    """
    payload = request.get_json(silent=True) or {}
    portfolio_stocks = payload.get("portfolio_stocks", [])
    cash = float(payload.get("cash", 100_000.0))
    try:
        model_version = _validate_model_version(
            (payload.get("model_version") or "").strip()
        )
    except _ModelVersionError as exc:
        return jsonify({"success": False, "message": str(exc), "data": None}), 400

    if not portfolio_stocks:
        return jsonify(
            {"success": False, "message": "portfolio_stocks is required", "data": None}
        ), 400

    from app.services.backtest_service import preflight_check

    today = _now_utc()

    recommendations = []
    for stock_code in portfolio_stocks:
        # Get latest score for each horizon
        scores = {}
        top_recommendation = None
        for h in [5, 20, 60]:
            pred = (
                StockScorePrediction.objects(
                    stock_code=stock_code, horizon=h, model_version=model_version
                )
                .order_by("-date")
                .first()
            )
            if pred:
                scores[f"score{h}"] = pred.score
                if top_recommendation is None:
                    top_recommendation = pred.recommendation

        # Get latest close price
        quote = (
            StockDailyQuote.objects(code=stock_code)
            .order_by("-date")
            .only("close_hfq", "close", "turnover_rate")
            .first()
        )
        current_price = quote.close_hfq or quote.close if quote else None

        # Position sizing
        flight = preflight_check(stock_code, check_liquidity_flag=True)
        capacity_ok = flight.get("pass", False)
        suggested_shares = 0
        if current_price and current_price > 0 and capacity_ok:
            suggested_shares = int(cash * 0.10 / (current_price * 100)) * 100

        # Determine action from scores
        buy_signals = sum(1 for v in scores.values() if v is not None and v >= 60)
        avoid_signals = sum(1 for v in scores.values() if v is not None and v < 40)

        if buy_signals >= 2:
            action = "BUY_MORE"
        elif avoid_signals >= 2:
            action = "SELL"
        elif top_recommendation == "AVOID":
            action = "REDUCE"
        elif top_recommendation == "BUY":
            action = "HOLD_BUY"
        else:
            action = "HOLD"

        recommendations.append(
            {
                "stock_code": stock_code,
                "current_price": round(current_price, 2) if current_price else None,
                "scores": scores,
                "action": action,
                "suggested_shares": max(suggested_shares, 0),
                "capacity_check": capacity_ok,
            }
        )

    return jsonify(
        {
            "success": True,
            "data": {
                "cash": cash,
                "recommendations": recommendations,
                "generated_at": today.isoformat(),
            },
        }
    )


# ---------------------------------------------------------------------------
# Decision Journal — log and query executed/missed decisions
# ---------------------------------------------------------------------------
@decisions_bp.route("/journal", methods=["POST"])
def create_journal():
    """Log a decision (executed or missed).

    Request body (JSON):
        stock_code       : str (required)
        stock_name       : str
        date             : str (required, YYYY-MM-DD)
        horizon          : int (5, 20, 60)
        score            : float
        recommendation   : str
        recommended_action : str (BUY, SELL, HOLD, WATCH)
        confidence       : str (high, medium, low)
        entry_price      : float
        target_price     : float
        stop_loss        : float
        position_size_pct: float
        executed         : bool
        executed_at      : str (ISO-8601)
        executed_price   : float
        executed_quantity : int
        execution_type   : str (followed, deviated, missed)
        realized_pnl     : float
        realized_pnl_pct : float
        exit_date        : str
        exit_price       : float
        exit_reason      : str
        dominant_component : str
        notes            : str
    """
    payload = request.get_json(silent=True) or {}

    stock_code = (payload.get("stock_code") or "").strip()
    if not stock_code:
        return jsonify(
            {"success": False, "message": "stock_code is required", "data": None}
        ), 400

    date_str = payload.get("date")
    if not date_str:
        return jsonify(
            {"success": False, "message": "date is required", "data": None}
        ), 400
    try:
        date = datetime.datetime.fromisoformat(str(date_str))
    except (ValueError, TypeError):
        return jsonify(
            {"success": False, "message": "Invalid date format", "data": None}
        ), 400

    executed_at = None
    exec_at_str = payload.get("executed_at")
    if exec_at_str:
        try:
            executed_at = datetime.datetime.fromisoformat(str(exec_at_str))
        except (ValueError, TypeError):
            pass

    exit_date = None
    exit_date_str = payload.get("exit_date")
    if exit_date_str:
        try:
            exit_date = datetime.datetime.fromisoformat(str(exit_date_str))
        except (ValueError, TypeError):
            pass

    entry = DecisionJournal(
        stock_code=stock_code,
        stock_name=payload.get("stock_name"),
        date=date,
        horizon=payload.get("horizon"),
        score=payload.get("score"),
        recommendation=payload.get("recommendation"),
        recommended_action=payload.get("recommended_action"),
        confidence=payload.get("confidence"),
        entry_price=payload.get("entry_price"),
        target_price=payload.get("target_price"),
        stop_loss=payload.get("stop_loss"),
        position_size_pct=payload.get("position_size_pct"),
        executed=bool(payload.get("executed", False)),
        executed_at=executed_at,
        executed_price=payload.get("executed_price"),
        executed_quantity=payload.get("executed_quantity"),
        execution_type=payload.get("execution_type"),
        realized_pnl=payload.get("realized_pnl"),
        realized_pnl_pct=payload.get("realized_pnl_pct"),
        exit_date=exit_date,
        exit_price=payload.get("exit_price"),
        exit_reason=payload.get("exit_reason"),
        dominant_component=payload.get("dominant_component"),
        notes=payload.get("notes"),
    )
    try:
        entry.save()
    except Exception as exc:
        logger.exception("Failed to save journal entry")
        return jsonify(
            {
                "success": False,
                "message": f"Failed to save: {exc}",
                "data": None,
            }
        ), 500

    return jsonify(
        {
            "success": True,
            "data": {
                "id": str(entry.id),
                "stock_code": entry.stock_code,
                "date": date_str,
                "created_at": entry.created_at.isoformat()
                if entry.created_at
                else None,
            },
        }
    ), 201


@decisions_bp.route("/journal", methods=["GET"])
def list_journal():
    """List journal entries.

    Query params:
        stock_code     : str  — filter by stock code
        date           : str  — YYYY-MM-DD
        execution_type : str  — followed, deviated, missed
        page           : int  (default 1)
        per_page       : int  (default 20, max 100)
    """
    stock_code = (request.args.get("stock_code") or "").strip()
    date_str = (request.args.get("date") or "").strip()
    execution_type = (request.args.get("execution_type") or "").strip()
    page = max(int(request.args.get("page", 1)), 1)
    per_page = min(int(request.args.get("per_page", 20)), 100)

    query = DecisionJournal.objects()
    if stock_code:
        query = query.filter(stock_code=stock_code)
    if date_str:
        try:
            d = datetime.datetime.fromisoformat(date_str)
            query = query.filter(date=d)
        except (ValueError, TypeError):
            pass
    if execution_type:
        query = query.filter(execution_type=execution_type)

    total = query.count()
    entries = list(
        query.order_by("-created_at").skip((page - 1) * per_page).limit(per_page)
    )

    def _serialize(e: DecisionJournal) -> dict:
        return {
            "id": str(e.id),
            "stock_code": e.stock_code,
            "stock_name": e.stock_name,
            "date": e.date.isoformat() if e.date else None,
            "horizon": e.horizon,
            "score": e.score,
            "recommendation": e.recommendation,
            "recommended_action": e.recommended_action,
            "confidence": e.confidence,
            "entry_price": e.entry_price,
            "target_price": e.target_price,
            "stop_loss": e.stop_loss,
            "position_size_pct": e.position_size_pct,
            "executed": e.executed,
            "executed_at": e.executed_at.isoformat() if e.executed_at else None,
            "executed_price": e.executed_price,
            "executed_quantity": e.executed_quantity,
            "execution_type": e.execution_type,
            "realized_pnl": e.realized_pnl,
            "realized_pnl_pct": e.realized_pnl_pct,
            "exit_date": e.exit_date.isoformat() if e.exit_date else None,
            "exit_price": e.exit_price,
            "exit_reason": e.exit_reason,
            "dominant_component": e.dominant_component,
            "notes": e.notes,
            "created_at": e.created_at.isoformat() if e.created_at else None,
        }

    return jsonify(
        {
            "success": True,
            "data": {
                "items": [_serialize(e) for e in entries],
                "total": total,
                "page": page,
                "per_page": per_page,
            },
        }
    )


@decisions_bp.route("/journal/summary", methods=["GET"])
def journal_summary():
    """Aggregate journal stats: model quality + execution discipline.

    Returns:
        model_quality : hit rate of recommendations (score >= 60)
        execution_discipline : follow-through rate (executed / total)
    """
    entries = list(DecisionJournal.objects())

    if not entries:
        return jsonify(
            {
                "success": True,
                "data": {
                    "total_entries": 0,
                    "model_quality": None,
                    "execution_discipline": None,
                    "message": "No journal entries",
                },
            }
        )

    # Model quality: how many recommendations actually had positive score signals
    high_conf = [e for e in entries if (e.score or 0) >= 60]
    model_quality = round(len(high_conf) / len(entries), 4) if entries else None

    # Execution discipline: how many recommended actions were followed
    executed = [e for e in entries if e.executed]
    execution_discipline = round(len(executed) / len(entries), 4) if entries else None

    # P&L breakdown
    total_pnl = sum(e.realized_pnl or 0 for e in entries)
    positive_trades = sum(1 for e in entries if (e.realized_pnl or 0) > 0)
    negative_trades = sum(1 for e in entries if (e.realized_pnl or 0) < 0)

    return jsonify(
        {
            "success": True,
            "data": {
                "total_entries": len(entries),
                "model_quality": model_quality,
                "execution_discipline": execution_discipline,
                "total_pnl": round(total_pnl, 2),
                "positive_trades": positive_trades,
                "negative_trades": negative_trades,
                "win_rate": (
                    round(positive_trades / (positive_trades + negative_trades), 4)
                    if (positive_trades + negative_trades) > 0
                    else None
                ),
            },
        }
    )


@decisions_bp.route("/journal/attribution", methods=["GET"])
def journal_attribution():
    """P&L attribution by scoring component and horizon."""
    entries = list(DecisionJournal.objects())

    if not entries:
        return jsonify(
            {
                "success": True,
                "data": {
                    "by_component": [],
                    "by_horizon": [],
                    "message": "No journal entries",
                },
            }
        )

    # By dominant component
    comp_pnl: dict[str, float] = defaultdict(float)
    comp_count: dict[str, int] = defaultdict(int)
    for e in entries:
        comp = e.dominant_component or "unknown"
        comp_pnl[comp] += e.realized_pnl or 0
        comp_count[comp] += 1

    by_component = sorted(
        [
            {
                "component": c,
                "total_pnl": round(pnl, 2),
                "trade_count": comp_count[c],
                "avg_pnl_per_trade": (
                    round(pnl / comp_count[c], 2) if comp_count[c] > 0 else 0
                ),
            }
            for c, pnl in comp_pnl.items()
        ],
        key=lambda x: -x["total_pnl"],
    )

    # By horizon
    hoz_pnl: dict[int, float] = defaultdict(float)
    hoz_count: dict[int, int] = defaultdict(int)
    for e in entries:
        h = e.horizon or 0
        hoz_pnl[h] += e.realized_pnl or 0
        hoz_count[h] += 1

    by_horizon = sorted(
        [
            {
                "horizon": h,
                "total_pnl": round(pnl, 2),
                "trade_count": hoz_count[h],
                "avg_pnl_per_trade": (
                    round(pnl / hoz_count[h], 2) if hoz_count[h] > 0 else 0
                ),
            }
            for h, pnl in hoz_pnl.items()
        ],
        key=lambda x: -x["total_pnl"],
    )

    return jsonify(
        {
            "success": True,
            "data": {
                "by_component": by_component,
                "by_horizon": by_horizon,
            },
        }
    )


# ---------------------------------------------------------------------------
# Watchlists — user-defined stock collections with current scores
# ---------------------------------------------------------------------------
@decisions_bp.route("/watchlists", methods=["POST"])
def create_watchlist():
    """Create or update a named watchlist.

    Request body (JSON):
        name        : str (required)
        stock_codes : list[str]  — stock codes
    """
    payload = request.get_json(silent=True) or {}
    name = (payload.get("name") or "").strip()
    if not name:
        return jsonify(
            {"success": False, "message": "name is required", "data": None}
        ), 400

    stock_codes = list(payload.get("stock_codes", []))
    user_id = get_jwt_identity() or "anonymous"

    # Find existing by name AND user_id to prevent cross-user overwrites
    try:
        wl = Watchlist.objects(name=name, user_id=user_id).first()
    except Exception:
        wl = None

    if wl:
        wl.stock_codes = stock_codes
        try:
            wl.save()
        except Exception as exc:
            logger.exception("Failed to update watchlist")
            return jsonify(
                {
                    "success": False,
                    "message": f"Failed to update: {exc}",
                    "data": None,
                }
            ), 500
    else:
        wl = Watchlist(name=name, stock_codes=stock_codes, user_id=user_id)
        try:
            wl.save()
        except Exception as exc:
            logger.exception("Failed to create watchlist")
            return jsonify(
                {
                    "success": False,
                    "message": f"Failed to create: {exc}",
                    "data": None,
                }
            ), 500

    return jsonify(
        {
            "success": True,
            "data": {
                "id": str(wl.id),
                "name": wl.name,
                "stock_codes": wl.stock_codes,
                "created_at": wl.created_at.isoformat() if wl.created_at else None,
            },
        }
    ), 201


@decisions_bp.route("/watchlists", methods=["GET"])
def list_watchlists():
    """List all watchlists for the current user."""
    user_id = get_jwt_identity() or "anonymous"
    try:
        wls = list(
            Watchlist.objects(user_id=user_id)
            .order_by("name")
            .only("id", "name", "stock_codes", "created_at")
        )
    except Exception:
        wls = []

    return jsonify(
        {
            "success": True,
            "data": {
                "items": [
                    {
                        "id": str(w.id),
                        "name": w.name,
                        "stock_count": len(w.stock_codes or []),
                        "created_at": w.created_at.isoformat()
                        if w.created_at
                        else None,
                    }
                    for w in wls
                ],
                "total": len(wls),
            },
        }
    )


@decisions_bp.route("/watchlists/<wl_id>", methods=["GET"])
def get_watchlist(wl_id: str):
    """Get a watchlist with current scores for each stock."""
    model_version, version_error = _resolve_model_version()
    if version_error is not None:
        return version_error
    try:
        wl = Watchlist.objects(id=wl_id).first()
    except Exception:
        wl = None

    if wl is None:
        return (
            jsonify({"success": False, "message": "Watchlist not found", "data": None}),
            404,
        )

    stocks = []
    today = _now_utc()
    for code in wl.stock_codes or []:
        # Look up stock name
        name = None
        try:
            ind = IndividualStock.objects(code=code).only("name").first()
            if ind:
                name = ind.name
        except Exception:
            pass

        # Latest scores per horizon
        scores = {}
        for h in [5, 20, 60]:
            pred = (
                StockScorePrediction.objects(
                    stock_code=code, horizon=h, model_version=model_version
                )
                .order_by("-date")
                .first()
            )
            if pred:
                scores[f"score{h}"] = {
                    "value": pred.score,
                    "recommendation": pred.recommendation,
                    "date": pred.date.isoformat() if pred.date else None,
                }

        # Latest price
        quote = (
            StockDailyQuote.objects(code=code)
            .order_by("-date")
            .only("close_hfq", "close", "date")
            .first()
        )
        price = round(quote.close_hfq or quote.close or 0, 2) if quote else None

        stocks.append(
            {
                "stock_code": code,
                "stock_name": name,
                "current_price": price,
                "scores": scores,
            }
        )

    return jsonify(
        {
            "success": True,
            "data": {
                "id": str(wl.id),
                "name": wl.name,
                "stock_codes": wl.stock_codes,
                "stocks": stocks,
                "created_at": wl.created_at.isoformat() if wl.created_at else None,
                "updated_at": wl.updated_at.isoformat() if wl.updated_at else None,
                "generated_at": today.isoformat(),
            },
        }
    )


@decisions_bp.route("/watchlists/<wl_id>", methods=["DELETE"])
def delete_watchlist(wl_id: str):
    """Delete a watchlist by ID."""
    try:
        wl = Watchlist.objects(id=wl_id).first()
    except Exception:
        wl = None

    if wl is None:
        return (
            jsonify({"success": False, "message": "Watchlist not found", "data": None}),
            404,
        )

    try:
        wl.delete()
    except Exception as exc:
        logger.exception("Failed to delete watchlist")
        return jsonify(
            {"success": False, "message": f"Failed to delete: {exc}", "data": None}
        ), 500

    return (
        jsonify({"success": True, "data": {"deleted": str(wl_id)}}),
        200,
    )
