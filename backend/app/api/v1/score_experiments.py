# -*- coding: utf-8 -*-
# Score experiment APIs for research calibration and model-version comparison.

import datetime
from collections import defaultdict
from typing import Any

from flask import Blueprint, jsonify, request
from mongoengine import ValidationError

from app.model.scoring import ScoreExperiment, StockScorePrediction

score_experiments_bp = Blueprint(
    "score_experiments", __name__, url_prefix="/api/score-experiments"
)

SUPPORTED_HORIZONS = {5, 20, 60}
SCORE_BUCKETS = ((0, 20), (20, 40), (40, 60), (60, 80), (80, 100))


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


def _serialize_value(value: Any):
    if isinstance(value, datetime.datetime):
        return _format_datetime(value)
    if isinstance(value, dict):
        return {key: _serialize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    return value


def _parse_horizons(value):
    if value is None:
        return [5, 20, 60]
    if isinstance(value, str):
        raw_items = [item.strip() for item in value.split(",") if item.strip()]
    else:
        raw_items = value

    horizons = []
    for item in raw_items:
        try:
            horizon = int(item)
        except (TypeError, ValueError):
            continue
        if horizon in SUPPORTED_HORIZONS and horizon not in horizons:
            horizons.append(horizon)
    return horizons


def _serialize_experiment(experiment):
    return {
        "id": str(experiment.id),
        "name": experiment.name,
        "description": experiment.description,
        "model_version": experiment.model_version,
        "baseline_model_version": experiment.baseline_model_version,
        "start_date": _format_datetime(experiment.start_date),
        "end_date": _format_datetime(experiment.end_date),
        "horizons": list(experiment.horizons or []),
        "config": _serialize_value(experiment.config or {}),
        "status": experiment.status,
        "report": _serialize_value(experiment.report or {}),
        "error_msg": experiment.error_msg,
        "created_at": _format_datetime(experiment.created_at),
        "updated_at": _format_datetime(experiment.updated_at),
        "completed_at": _format_datetime(experiment.completed_at),
    }


def _experiment_or_404(experiment_id):
    try:
        experiment = ScoreExperiment.objects(id=experiment_id).first()
    except ValidationError:
        experiment = None
    if experiment is None:
        return None, (
            jsonify({"success": False, "message": "Experiment not found"}),
            404,
        )
    return experiment, None


def _query_predictions(model_version, start_date, end_date, horizon):
    return list(
        StockScorePrediction.objects(
            model_version=model_version,
            date__gte=start_date,
            date__lte=end_date,
            horizon=horizon,
            status="VERIFIED",
        ).order_by("date", "-score")
    )


def _build_report(experiment):
    result = {
        "model_version": experiment.model_version,
        "baseline_model_version": experiment.baseline_model_version,
        "start_date": _format_datetime(experiment.start_date),
        "end_date": _format_datetime(experiment.end_date),
        "horizons": {},
    }
    for horizon in experiment.horizons or []:
        predictions = _query_predictions(
            experiment.model_version,
            experiment.start_date,
            experiment.end_date,
            horizon,
        )
        horizon_report = _summarize_predictions(predictions)
        if experiment.baseline_model_version:
            baseline = _query_predictions(
                experiment.baseline_model_version,
                experiment.start_date,
                experiment.end_date,
                horizon,
            )
            baseline_report = _summarize_predictions(baseline)
            horizon_report["baseline"] = baseline_report
            horizon_report["comparison"] = _compare_summary(
                horizon_report["overall"],
                baseline_report["overall"],
            )
        result["horizons"][str(horizon)] = horizon_report
    return result


def _summarize_predictions(predictions):
    return {
        "overall": _metric_summary(predictions),
        "score_buckets": _bucket_summary(predictions),
        "top_n": _top_n_summary(predictions),
        "component_summary": _component_summary(predictions),
        "false_positives": _sample(
            [
                item
                for item in predictions
                if (item.score or 0) >= 70
                and (item.verification or {}).get("return_at_target", 0) < 0
            ]
        ),
        "false_negatives": _sample(
            [
                item
                for item in predictions
                if (item.score or 0) < 40
                and (item.verification or {}).get("max_return", 0) >= 0.08
            ]
        ),
    }


def _bucket_summary(predictions):
    result = []
    for low, high in SCORE_BUCKETS:
        bucket_items = [
            item
            for item in predictions
            if low <= (item.score or 0) < high
            or (high == 100 and (item.score or 0) == 100)
        ]
        result.append({"bucket": f"{low}-{high}", **_metric_summary(bucket_items)})
    return result


def _top_n_summary(predictions):
    by_date = defaultdict(list)
    for prediction in predictions:
        by_date[prediction.date].append(prediction)

    result = {}
    for top_n in (10, 30, 50):
        selected = []
        for items in by_date.values():
            selected.extend(
                sorted(items, key=lambda item: item.score or 0, reverse=True)[:top_n]
            )
        result[f"top_{top_n}"] = _metric_summary(selected)
    return result


def _component_summary(predictions):
    grouped = defaultdict(list)
    for prediction in predictions:
        explanation = prediction.explanation or {}
        for component in explanation.get("components", []):
            component_id = component.get("id")
            if component_id:
                grouped[component_id].append(prediction)

    return {
        component_id: _metric_summary(items)
        for component_id, items in sorted(grouped.items())
    }


def _metric_summary(predictions):
    if not predictions:
        return {
            "count": 0,
            "avg_score": None,
            "avg_return_at_target": None,
            "avg_max_return": None,
            "avg_min_return": None,
            "avg_max_drawdown": None,
            "hit_rate": None,
            "stop_loss_hit_rate": None,
        }

    return {
        "count": len(predictions),
        "avg_score": _avg([item.score for item in predictions]),
        "avg_return_at_target": _avg_metric(predictions, "return_at_target"),
        "avg_max_return": _avg_metric(predictions, "max_return"),
        "avg_min_return": _avg_metric(predictions, "min_return"),
        "avg_max_drawdown": _avg_metric(predictions, "max_drawdown"),
        "hit_rate": _rate(predictions, "hit_target"),
        "stop_loss_hit_rate": _rate(predictions, "hit_stop_loss"),
    }


def _compare_summary(current, baseline):
    keys = (
        "avg_return_at_target",
        "avg_max_return",
        "avg_min_return",
        "avg_max_drawdown",
        "hit_rate",
        "stop_loss_hit_rate",
    )
    comparison = {"count_delta": current["count"] - baseline["count"]}
    for key in keys:
        current_value = current.get(key)
        baseline_value = baseline.get(key)
        comparison[f"{key}_delta"] = (
            round(current_value - baseline_value, 6)
            if current_value is not None and baseline_value is not None
            else None
        )
    return comparison


def _sample(predictions, limit=10):
    return [
        {
            "stock_code": item.stock_code,
            "stock_name": item.stock_name,
            "date": _format_datetime(item.date),
            "score": item.score,
            "rank": item.rank,
            "verification": _serialize_value(item.verification or {}),
        }
        for item in predictions[:limit]
    ]


def _avg_metric(predictions, metric):
    return _avg([(item.verification or {}).get(metric) for item in predictions])


def _avg(values):
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    return round(sum(filtered) / len(filtered), 6)


def _rate(predictions, metric):
    values = [(item.verification or {}).get(metric) for item in predictions]
    values = [value for value in values if value is not None]
    if not values:
        return None
    return round(sum(1 for value in values if value) / len(values), 6)


@score_experiments_bp.route("", methods=["GET"])
def list_experiments():
    try:
        limit = int(request.args.get("limit", 50))
    except (TypeError, ValueError):
        limit = 50
    limit = max(1, min(limit, 200))
    rows = ScoreExperiment.objects.order_by("-created_at").limit(limit)
    return jsonify({"items": [_serialize_experiment(row) for row in rows]}), 200


@score_experiments_bp.route("", methods=["POST"])
def create_experiment():
    payload = request.get_json(silent=True) or {}
    name = (payload.get("name") or "").strip()
    model_version = (payload.get("model_version") or "").strip()
    start_date = _parse_datetime(payload.get("start_date"))
    end_date = _parse_datetime(payload.get("end_date"))
    horizons = _parse_horizons(payload.get("horizons"))

    if not name:
        return jsonify({"success": False, "message": "name is required"}), 400
    if not model_version:
        return jsonify({"success": False, "message": "model_version is required"}), 400
    if not start_date or not end_date or start_date > end_date:
        return jsonify({"success": False, "message": "invalid date range"}), 400
    if not horizons:
        return jsonify(
            {"success": False, "message": "at least one horizon is required"}
        ), 400

    experiment = ScoreExperiment(
        name=name,
        description=(payload.get("description") or "").strip(),
        model_version=model_version,
        baseline_model_version=(payload.get("baseline_model_version") or "").strip(),
        start_date=start_date,
        end_date=end_date,
        horizons=horizons,
        config=payload.get("config") or {},
    )
    experiment.save()

    if payload.get("run_now", True):
        _run_experiment(experiment)

    return jsonify(_serialize_experiment(experiment)), 201


@score_experiments_bp.route("/<experiment_id>", methods=["GET"])
def get_experiment(experiment_id):
    experiment, error_response = _experiment_or_404(experiment_id)
    if error_response:
        return error_response
    return jsonify(_serialize_experiment(experiment)), 200


@score_experiments_bp.route("/<experiment_id>/run", methods=["POST"])
def run_experiment(experiment_id):
    experiment, error_response = _experiment_or_404(experiment_id)
    if error_response:
        return error_response

    _run_experiment(experiment)
    return jsonify(_serialize_experiment(experiment)), 200


def _run_experiment(experiment):
    experiment.status = "RUNNING"
    experiment.error_msg = None
    experiment.save()
    try:
        experiment.report = _build_report(experiment)
        experiment.status = "COMPLETED"
        experiment.completed_at = datetime.datetime.now(datetime.UTC)
    except Exception as exc:  # pragma: no cover - defensive for live Mongo/report data
        experiment.status = "FAILED"
        experiment.error_msg = str(exc)
    experiment.save()
    return experiment
