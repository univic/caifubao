# -*- coding: utf-8 -*-
# Score experiment APIs for research calibration and model-version comparison.

import datetime
import math
from collections import defaultdict
from statistics import mean
from typing import Any

from flask import Blueprint, jsonify, request
from mongoengine import ValidationError

from app.lib.auth_decorators import block_service_tokens
from app.model.scoring import ScoreExperiment, ScoreModelVersion, StockScorePrediction

score_experiments_bp = Blueprint(
    "score_experiments", __name__, url_prefix="/api/score-experiments"
)
score_experiments_bp.before_request(block_service_tokens)

SUPPORTED_HORIZONS = {5, 20, 60}
SCORE_BUCKETS = ((0, 20), (20, 40), (40, 60), (60, 80), (80, 100))
DEFAULT_MODEL_VERSION = "score_v2_202605b"


class ScoreReportInputError(ValueError):
    """Raised when persisted predictions cannot produce a valid report."""


# ---------------------------------------------------------------------------
# Composite scoring constants (mirror backtest_service.py)
# ---------------------------------------------------------------------------
_COMPOSITE_EXCESS_WEIGHT = 1.0
_COMPOSITE_DRAWDOWN_PENALTY = 0.5
_COMPOSITE_IR_WEIGHT = 2.0
_COMPOSITE_TURNOVER_PENALTY = 0.1
_COMPOSITE_CONCENTRATION_PENALTY = 1.0
_MIN_TRADES_FOR_RANKING = 5
_MIN_TRADING_DAYS = 120
_CONCENTRATION_THRESHOLD = 0.40
_BONFERRONI_ALPHA = 0.05

_EXPERIMENT_COMPONENTS = [
    "signal_strength",
    "trend_alignment",
    "momentum",
    "breakout_or_position",
    "relative_strength",
    "industry_momentum",
    "real_relative_strength",
    "risk_penalty",
]


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


def _config_bucket_basis(config, horizon):
    if config is None:
        return None
    horizon_config = config.get(str(horizon)) or config.get(horizon) or {}
    if not isinstance(horizon_config, dict):
        return "score"
    directions = horizon_config.get("directions") or {}
    if not isinstance(directions, dict):
        return "score"
    has_component_flip = any(
        component != "risk_penalty" and value == -1
        for component, value in directions.items()
    )
    return "percentile" if has_component_flip else "score"


def _validated_percentile(item):
    value = getattr(item, "percentile", None)
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or not 0 <= value <= 1
    ):
        stock_code = getattr(item, "stock_code", "unknown")
        raise ScoreReportInputError(
            "percentile must be a finite number in [0, 1] for signed-score "
            f"calibration; invalid for {stock_code}"
        )
    return float(value)


def _resolve_bucket_basis(
    predictions, *, configured_basis=None, force_percentile=False
):
    signed_scores = any((item.score or 0) < 0 for item in predictions)
    basis = "percentile" if force_percentile or signed_scores else configured_basis
    basis = basis or "score"
    if basis == "percentile":
        for item in predictions:
            _validated_percentile(item)
    return basis


def _bucket_value(item, basis):
    if basis == "percentile":
        return _validated_percentile(item) * 100.0
    return float(item.score or 0)


def _registered_model_config(model_version):
    registered = ScoreModelVersion.objects(model_version=model_version).first()
    return registered.config if registered is not None else {}


def _build_report(experiment):
    experiment_config = getattr(experiment, "config", None) or {}
    candidate_config = experiment_config
    baseline_config = {}
    if isinstance(experiment, ScoreExperiment):
        if not candidate_config:
            candidate_config = _registered_model_config(experiment.model_version)
        if experiment.baseline_model_version:
            baseline_config = _registered_model_config(
                experiment.baseline_model_version
            )
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
        candidate_basis = _resolve_bucket_basis(
            predictions,
            configured_basis=_config_bucket_basis(candidate_config, horizon),
        )
        baseline = []
        baseline_basis = "score"
        if experiment.baseline_model_version:
            baseline = _query_predictions(
                experiment.baseline_model_version,
                experiment.start_date,
                experiment.end_date,
                horizon,
            )
            baseline_basis = _resolve_bucket_basis(
                baseline,
                configured_basis=_config_bucket_basis(baseline_config, horizon),
            )

        comparison_basis = (
            "percentile"
            if "percentile" in {candidate_basis, baseline_basis}
            else "score"
        )
        if comparison_basis == "percentile":
            _resolve_bucket_basis(predictions, force_percentile=True)
            _resolve_bucket_basis(baseline, force_percentile=True)

        horizon_report = _summarize_predictions(predictions, comparison_basis)
        if experiment.baseline_model_version:
            baseline_report = _summarize_predictions(baseline, comparison_basis)
            horizon_report["baseline"] = baseline_report
            horizon_report["comparison_basis"] = comparison_basis
            horizon_report["comparison"] = _compare_summary(
                horizon_report["overall"],
                baseline_report["overall"],
            )
        result["horizons"][str(horizon)] = horizon_report
    return result


def _summarize_predictions(predictions, basis="score"):
    return {
        "bucket_basis": basis,
        "overall": _metric_summary(predictions),
        "score_buckets": _bucket_summary(predictions, basis),
        "top_n": _top_n_summary(predictions, basis),
        "component_summary": _component_summary(predictions),
        "false_positives": _sample(
            [
                item
                for item in predictions
                if _bucket_value(item, basis) >= 70
                and (item.verification or {}).get("return_at_target", 0) < 0
            ]
        ),
        "false_negatives": _sample(
            [
                item
                for item in predictions
                if _bucket_value(item, basis) < 40
                and (item.verification or {}).get("max_return", 0) >= 0.08
            ]
        ),
    }


def _bucket_summary(predictions, basis="score"):
    result = []
    for low, high in SCORE_BUCKETS:
        bucket_items = [
            item
            for item in predictions
            if low <= _bucket_value(item, basis) < high
            or (high == 100 and _bucket_value(item, basis) == 100)
        ]
        result.append({"bucket": f"{low}-{high}", **_metric_summary(bucket_items)})
    return result


def _top_n_summary(predictions, basis="score"):
    by_date = defaultdict(list)
    for prediction in predictions:
        by_date[prediction.date].append(prediction)

    result = {}
    for top_n in (10, 30, 50):
        selected = []
        for items in by_date.values():
            selected.extend(
                sorted(
                    items,
                    key=lambda item: _bucket_value(item, basis),
                    reverse=True,
                )[:top_n]
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
            "hit_rate_intra": None,
            "stop_loss_hit_rate": None,
        }

    return {
        "count": len(predictions),
        "avg_score": _avg([item.score for item in predictions]),
        "avg_return_at_target": _avg_metric(predictions, "return_at_target"),
        "avg_max_return": _avg_metric(predictions, "max_return"),
        "avg_min_return": _avg_metric(predictions, "min_return"),
        "avg_max_drawdown": _avg_metric(predictions, "max_drawdown"),
        "hit_rate": _rate(predictions, "hit_target_close"),
        "hit_rate_intra": _rate(predictions, "hit_target_intra"),
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


def _build_comparison_report(
    candidate_model_version,
    baseline_model_version,
    start_date,
    end_date,
    horizon,
    *,
    candidate_config=None,
    baseline_config=None,
):
    candidate_predictions = _query_predictions(
        candidate_model_version, start_date, end_date, horizon
    )
    baseline_predictions = _query_predictions(
        baseline_model_version, start_date, end_date, horizon
    )
    if not candidate_predictions or not baseline_predictions:
        raise ScoreReportInputError(
            "comparison requires verified predictions for both model versions"
        )

    candidate_basis = _resolve_bucket_basis(
        candidate_predictions,
        configured_basis=_config_bucket_basis(candidate_config, horizon),
    )
    baseline_basis = _resolve_bucket_basis(
        baseline_predictions,
        configured_basis=_config_bucket_basis(baseline_config, horizon),
    )
    comparison_basis = (
        "percentile" if "percentile" in {candidate_basis, baseline_basis} else "score"
    )
    if comparison_basis == "percentile":
        _resolve_bucket_basis(candidate_predictions, force_percentile=True)
        _resolve_bucket_basis(baseline_predictions, force_percentile=True)

    candidate = {
        "model_version": candidate_model_version,
        **_summarize_predictions(candidate_predictions, comparison_basis),
    }
    baseline = {
        "model_version": baseline_model_version,
        **_summarize_predictions(baseline_predictions, comparison_basis),
    }
    deltas = _comparison_deltas(candidate, baseline)
    return {
        "horizon": horizon,
        "start_date": _format_datetime(start_date),
        "end_date": _format_datetime(end_date),
        "comparison_basis": comparison_basis,
        "comparison_status": "ok",
        "candidate": candidate,
        "baseline": baseline,
        "deltas": deltas,
        "verdict": _comparison_verdict(deltas),
    }


def _comparison_deltas(candidate, baseline):
    current = candidate["overall"]
    previous = baseline["overall"]

    def delta(key):
        current_value = current.get(key)
        baseline_value = previous.get(key)
        if current_value is None or baseline_value is None:
            return None
        return round(current_value - baseline_value, 6)

    result = {
        "count": delta("count"),
        "avg_score": (
            None if candidate["bucket_basis"] == "percentile" else delta("avg_score")
        ),
        "avg_return_at_target": delta("avg_return_at_target"),
        "avg_max_return": delta("avg_max_return"),
        "avg_min_return": delta("avg_min_return"),
        "avg_max_drawdown": delta("avg_max_drawdown"),
        "hit_rate": delta("hit_rate"),
        "hit_rate_intra": delta("hit_rate_intra"),
        "stop_loss_hit_rate": delta("stop_loss_hit_rate"),
    }
    top_deltas = {}
    for key, current_top in candidate.get("top_n", {}).items():
        baseline_top = baseline.get("top_n", {}).get(key, {})
        top_deltas[key] = {}
        for metric in ("hit_rate", "avg_return_at_target", "avg_max_return"):
            current_value = current_top.get(metric)
            baseline_value = baseline_top.get(metric)
            top_deltas[key][f"{metric}_delta"] = (
                round(current_value - baseline_value, 6)
                if current_value is not None and baseline_value is not None
                else None
            )
    result["top_n"] = top_deltas
    return result


def _comparison_verdict(deltas):
    hit_delta = deltas.get("hit_rate") or 0
    return_delta = deltas.get("avg_return_at_target") or 0
    if hit_delta > 0.02 and return_delta > 0.002:
        return "Candidate clearly wins on both hit rate and return."
    if hit_delta > 0.02 and return_delta >= -0.002:
        return "Candidate wins on hit rate, return is comparable."
    if hit_delta >= -0.02 and return_delta > 0.005:
        return "Candidate wins on return, hit rate is comparable."
    if hit_delta < -0.05 or return_delta < -0.01:
        return "Baseline wins."
    if abs(hit_delta) <= 0.01 and abs(return_delta) <= 0.003:
        return "The two versions are not significantly different."
    if hit_delta > 0:
        return "Candidate shows modest improvement in hit rate."
    if return_delta > 0:
        return "Candidate shows modest improvement in return."
    return "Candidate shows mixed results — review in detail."


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
        try:
            _run_experiment(experiment)
        except ScoreReportInputError as exc:
            return jsonify(
                {
                    "success": False,
                    "message": str(exc),
                    "data": _serialize_experiment(experiment),
                }
            ), 422

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

    try:
        _run_experiment(experiment)
    except ScoreReportInputError as exc:
        return jsonify(
            {
                "success": False,
                "message": str(exc),
                "data": _serialize_experiment(experiment),
            }
        ), 422
    return jsonify(_serialize_experiment(experiment)), 200


@score_experiments_bp.route("/compare", methods=["GET"])
def compare_experiments():
    """Compare two experiments (or model versions) side-by-side.

    Query params:
        id_a : str (required) — experiment ID or model_version for candidate
        id_b : str (required) — experiment ID or model_version for baseline
        start_date : str (required) — YYYY-MM-DD
        end_date   : str (required) — YYYY-MM-DD
        horizon    : int (required) — 5, 20, or 60
    """

    id_a = (request.args.get("id_a") or "").strip()
    id_b = (request.args.get("id_b") or "").strip()
    start_date = _parse_datetime(request.args.get("start_date"))
    end_date = _parse_datetime(request.args.get("end_date"))
    horizon_raw = request.args.get("horizon")

    if not id_a:
        return jsonify({"success": False, "message": "id_a is required"}), 400
    if not id_b:
        return jsonify({"success": False, "message": "id_b is required"}), 400
    if not start_date or not end_date or start_date > end_date:
        return jsonify({"success": False, "message": "invalid date range"}), 400

    try:
        horizon = int(horizon_raw)
    except (TypeError, ValueError):
        return jsonify(
            {"success": False, "message": "horizon is required and must be an integer"}
        ), 400
    if horizon not in SUPPORTED_HORIZONS:
        return jsonify(
            {
                "success": False,
                "message": f"horizon must be one of {sorted(SUPPORTED_HORIZONS)}",
            }
        ), 400

    # Resolve id_a: try as experiment ID first, fall back to model_version
    target_a = _resolve_comparison_target(id_a)
    if target_a is None:
        return jsonify(
            {"success": False, "message": f"Cannot resolve id_a: {id_a}"}
        ), 404

    # Resolve id_b
    target_b = _resolve_comparison_target(id_b)
    if target_b is None:
        return jsonify(
            {"success": False, "message": f"Cannot resolve id_b: {id_b}"}
        ), 404

    try:
        result = _build_comparison_report(
            target_a["model_version"],
            target_b["model_version"],
            start_date,
            end_date,
            horizon,
            candidate_config=target_a["config"],
            baseline_config=target_b["config"],
        )
        return jsonify({"success": True, "data": result}), 200
    except ScoreReportInputError as exc:
        return jsonify({"success": False, "message": str(exc), "data": None}), 422
    except Exception:
        return jsonify(
            {
                "success": False,
                "message": "Comparison failed due to an internal error",
                "data": None,
            }
        ), 500


def _resolve_to_model_version(identifier: str) -> str | None:
    """If *identifier* is a ScoreExperiment ID, return its model_version.
    Otherwise return the identifier itself (assumed to be a model_version string).
    Returns None if an experiment ID was given but not found.
    """
    try:
        experiment = ScoreExperiment.objects(id=identifier).first()
    except ValidationError:
        experiment = None

    if experiment is not None:
        return experiment.model_version or identifier

    # Not a valid experiment ObjectId — treat as a raw model_version string.
    # But try once with the string path just in case it matches an experiment
    # with a non-ObjectId `id` field.
    try:
        experiment = ScoreExperiment.objects(id=identifier).first()
    except ValidationError:
        experiment = None

    if experiment is not None:
        return experiment.model_version or identifier

    # Fallback: treat the identifier as a model_version directly.
    return identifier or None


def _resolve_comparison_target(identifier):
    """Resolve an experiment/model version together with stable score semantics."""
    try:
        experiment = ScoreExperiment.objects(id=identifier).first()
    except ValidationError:
        experiment = None
    if experiment is not None:
        return {
            "model_version": experiment.model_version or identifier,
            "config": experiment.config or {},
        }

    model_version = _resolve_to_model_version(identifier)
    if model_version is None:
        return None
    registered = ScoreModelVersion.objects(model_version=model_version).first()
    return {
        "model_version": model_version,
        "config": registered.config if registered is not None else {},
    }


# ---------------------------------------------------------------------------
# Composite scoring helpers for experiment rankings
# ---------------------------------------------------------------------------
def _extract_experiment_weights(experiment, horizon: int) -> dict | None:
    """Extract component weights from experiment config for a given horizon."""
    config = experiment.config or {}
    h_config = config.get(str(horizon), config)
    if not isinstance(h_config, dict):
        return None
    weights = h_config.get("weights")
    if isinstance(weights, dict) and weights:
        return {k: v for k, v in weights.items()}
    # Fallback: check for direct weight keys at horizon level
    direct = {
        comp: h_config[comp] for comp in _EXPERIMENT_COMPONENTS if comp in h_config
    }
    return direct if direct else None


def _compute_composite_from_experiment(
    experiment, horizon_filter: int | None = None
) -> dict:
    """Compute composite score for a ScoreExperiment from its calibration report.

    Uses proxy metrics from the report since experiments have not necessarily
    been run through the backtest engine.

    Returns a dict with ``score``, ``rankable``, ``flags``, ``breakdown``,
    and ``metrics`` keys.
    """
    report = experiment.report or {}
    # Support both backend-created reports ("horizons") and datahub-created
    # reports ("reports" → {horizon: {...}} or {"5": {...}, "20": {...}})
    horizons_data = report.get("horizons") or report.get("reports", {})
    if not horizons_data:
        return {
            "score": -999.0,
            "flags": ["no_data"],
            "rankable": False,
            "breakdown": {},
            "metrics": {},
        }

    # Build a flat list of (count, avg_ret, avg_dd) per horizon
    horizon_entries: list[tuple[int, float, float]] = []
    target_strs = (
        [str(horizon_filter)]
        if horizon_filter is not None
        else [str(h) for h in SUPPORTED_HORIZONS if str(h) in horizons_data]
    )

    for h_str in target_strs:
        h_data = horizons_data.get(h_str, {})
        overall = h_data.get("overall", {})
        count = overall.get("count", 0) or 0
        if count == 0:
            continue
        ret = overall.get("avg_return_at_target") or 0.0
        dd = abs(overall.get("avg_max_drawdown") or 0.0)
        horizon_entries.append((count, ret, dd))

    if not horizon_entries:
        return {
            "score": -999.0,
            "flags": ["no_data"],
            "rankable": False,
            "breakdown": {},
            "metrics": {},
        }

    total_count = sum(e[0] for e in horizon_entries)
    if total_count < _MIN_TRADES_FOR_RANKING:
        return {
            "score": -999.0,
            "flags": ["low_sample"],
            "rankable": False,
            "breakdown": {},
            "metrics": {"total_trades": total_count},
        }

    # Weighted-average metrics
    avg_return = sum(e[0] * e[1] for e in horizon_entries) / total_count
    avg_dd = sum(e[0] * e[2] for e in horizon_entries) / total_count

    # Convert to backtest-compatible percentage scale (decimal → pct)
    excess_return_pct = avg_return * 100.0
    max_drawdown = max(avg_dd * 100.0, 20.0) if avg_dd > 0 else 20.0

    # IR not available from calibration reports — use 0
    information_ratio = 0.0

    # Estimate trading days from experiment date range
    if experiment.start_date and experiment.end_date:
        calendar_days = (experiment.end_date - experiment.start_date).days
        trading_days = max(int(calendar_days * 252.0 / 365.0), 1)
    else:
        trading_days = 120

    # -- Composite scoring ------------------------------------------------
    base = excess_return_pct * _COMPOSITE_EXCESS_WEIGHT
    ir_contrib = information_ratio * _COMPOSITE_IR_WEIGHT

    dd = max(max_drawdown or 0.0, 0.0)
    dd_penalty = (
        dd * _COMPOSITE_DRAWDOWN_PENALTY
        if dd <= 20.0
        else 20.0 * _COMPOSITE_DRAWDOWN_PENALTY
        + (dd - 20.0) * _COMPOSITE_DRAWDOWN_PENALTY * 2.0
    )

    trade_rate = total_count / max(trading_days, 1)
    turnover_penalty = trade_rate * _COMPOSITE_TURNOVER_PENALTY

    # No per-trade PnL data in experiments — skip concentration penalty
    concentration_penalty = 0.0

    score = base + ir_contrib - dd_penalty - turnover_penalty - concentration_penalty

    # Flags
    flags: list[str] = []
    if dd > 30.0:
        flags.append("high_drawdown")
    if trading_days < _MIN_TRADING_DAYS:
        flags.append("insufficient_period")

    return {
        "score": round(score, 4),
        "rankable": True,
        "flags": flags,
        "breakdown": {
            "excess_contrib": round(base, 4),
            "ir_contrib": round(ir_contrib, 4),
            "dd_penalty": round(dd_penalty, 4),
            "turnover_penalty": round(turnover_penalty, 4),
            "concentration_penalty": round(concentration_penalty, 4),
        },
        "metrics": {
            "excess_return_pct": round(excess_return_pct, 4),
            "max_drawdown": round(max_drawdown, 4),
            "information_ratio": round(information_ratio, 4),
            "total_trades": total_count,
            "trading_days": trading_days,
        },
    }


# ---------------------------------------------------------------------------


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
        if isinstance(exc, ScoreReportInputError):
            raise
        return experiment
    experiment.save()
    return experiment


@score_experiments_bp.route("/consensus", methods=["GET"])
def multi_horizon_consensus():
    """Detect consensus or divergence across Score5/20/60 for a stock+date.

    Requires predictions for all 3 horizons to form a valid consensus.
    Returns partial=true with available data when fewer than 3 horizons
    have predictions.

    Query params:
        stock_code : str (required)
        date       : str (required)  YYYY-MM-DD
        model_version : str (optional) defaults from scoring config
    """
    stock_code = (request.args.get("stock_code") or "").strip()
    date = _parse_datetime(request.args.get("date"))
    model_version = (
        request.args.get("model_version") or ""
    ).strip() or DEFAULT_MODEL_VERSION

    if not stock_code or not date:
        return jsonify(
            {"success": False, "message": "stock_code and date are required"}
        ), 400

    predictions = {}
    for h in SUPPORTED_HORIZONS:
        pred = StockScorePrediction.objects(
            stock_code=stock_code,
            date=date,
            horizon=h,
            model_version=model_version,
        ).first()
        if pred:
            predictions[str(h)] = {
                "score": pred.score,
                "rank": pred.rank,
                "percentile": pred.percentile,
                "recommendation": pred.recommendation,
            }

    if not predictions:
        return jsonify(
            {"success": False, "message": "No predictions found for this stock+date"}
        ), 404

    required_horizons = set(str(h) for h in SUPPORTED_HORIZONS)
    have_all_horizons = required_horizons.issubset(set(predictions.keys()))

    # Detect consensus/divergence
    recs = {v["recommendation"] for v in predictions.values()}
    consensus = len(recs) == 1
    all_bullish = all(
        v["recommendation"] in ("BUY", "WATCH") for v in predictions.values()
    )
    all_bearish = all(
        v["recommendation"] in ("AVOID", "NONE") for v in predictions.values()
    )
    all_neutral = all(v["recommendation"] == "NONE" for v in predictions.values())

    label = "consensus" if consensus else "divergence"
    if consensus:
        rec_value = (list(recs)[0] or "unknown").lower()
        label += f"_{rec_value}"

    return jsonify(
        {
            "success": True,
            "data": {
                "stock_code": stock_code,
                "date": _format_datetime(date),
                "model_version": model_version,
                "label": label,
                "predictions": predictions,
                "consensus": consensus,
                "have_all_horizons": have_all_horizons,
                "all_bullish": all_bullish,
                "all_bearish": all_bearish,
                "all_neutral": all_neutral,
            },
        }
    ), 200


# ---------------------------------------------------------------------------
# Experiment rankings and heatmap
# ---------------------------------------------------------------------------
@score_experiments_bp.route("/rankings", methods=["GET"])
def rank_experiments():
    """Rank completed experiments by composite score.

    Query params:
        horizon : int (optional) — filter by horizon; when omitted,
                  aggregates metrics across all available horizons.
        limit   : int (default 20, max 100)
    """
    horizon_raw = request.args.get("horizon")
    horizon: int | None = None
    if horizon_raw is not None:
        try:
            horizon = int(horizon_raw)
        except (TypeError, ValueError):
            return jsonify(
                {"success": False, "message": "horizon must be an integer"}
            ), 400

    try:
        limit = int(request.args.get("limit", 20))
    except (TypeError, ValueError):
        limit = 20
    limit = max(1, min(limit, 100))

    # Query completed experiments
    query = ScoreExperiment.objects(status="COMPLETED")
    if horizon is not None:
        query = query.filter(horizons=horizon)
    experiments = list(query.order_by("-created_at"))

    total_experiments = len(experiments)

    rankings = []
    for exp in experiments:
        result = _compute_composite_from_experiment(exp, horizon)

        # Resolve horizon to use for weight extraction
        h_for_weights: int = (
            horizon if horizon is not None else (exp.horizons[0] if exp.horizons else 5)
        )
        weights = _extract_experiment_weights(exp, h_for_weights) or {}

        rankings.append(
            {
                "experiment_id": str(exp.id),
                "name": exp.name,
                "model_version": exp.model_version,
                "horizon": horizon,
                "composite_score": result["score"],
                "breakdown": result.get("breakdown", {}),
                "metrics": result.get("metrics", {}),
                "weights": weights,
                "flags": result.get("flags", []),
                "rankable": result["rankable"],
            }
        )

    # Sort by composite score descending
    rankings.sort(key=lambda r: r["composite_score"], reverse=True)

    # Assign ranks and take top-N
    top_rankings = []
    for i, entry in enumerate(rankings[:limit]):
        entry["rank"] = i + 1
        top_rankings.append(entry)

    # Bonferroni metadata
    n = max(total_experiments, 1)
    corrected_alpha = round(_BONFERRONI_ALPHA / n, 6) if n > 1 else _BONFERRONI_ALPHA

    return jsonify(
        {
            "success": True,
            "data": {
                "rankings": top_rankings,
                "total_experiments": total_experiments,
                "bonferroni": {
                    "alpha": _BONFERRONI_ALPHA,
                    "corrected_alpha": corrected_alpha,
                    "num_comparisons": n,
                },
            },
        }
    ), 200


@score_experiments_bp.route("/heatmap", methods=["GET"])
def component_heatmap():
    """Component pairwise heatmap data for a specific horizon.

    Query params:
        horizon : int (required) — 5, 20, or 60
    """
    horizon_raw = request.args.get("horizon")
    if not horizon_raw:
        return jsonify({"success": False, "message": "horizon is required"}), 400

    try:
        horizon = int(horizon_raw)
    except (TypeError, ValueError):
        return jsonify({"success": False, "message": "horizon must be an integer"}), 400

    if horizon not in SUPPORTED_HORIZONS:
        return jsonify(
            {
                "success": False,
                "message": f"horizon must be one of {sorted(SUPPORTED_HORIZONS)}",
            }
        ), 400

    experiments = list(ScoreExperiment.objects(status="COMPLETED", horizons=horizon))

    # Build per-experiment data: enabled components + composite score + weights
    exp_data: list[dict] = []
    for exp in experiments:
        result = _compute_composite_from_experiment(exp, horizon)
        weights = _extract_experiment_weights(exp, horizon)
        if not weights:
            continue
        enabled = {comp for comp, w in weights.items() if (w or 0) > 0}
        if not enabled:
            continue
        exp_data.append(
            {
                "enabled": enabled,
                "composite_score": result["score"],
                "weights": weights,
                "experiment_id": str(exp.id),
                "name": exp.name,
            }
        )

    # Build pair matrix: for every component pair (x, y), average composite_score
    # of experiments where both are enabled
    all_components = sorted(_EXPERIMENT_COMPONENTS)
    matrix: list[dict] = []

    for comp_x in all_components:
        for comp_y in all_components:
            pair_experiments = [
                e for e in exp_data if comp_x in e["enabled"] and comp_y in e["enabled"]
            ]
            if not pair_experiments:
                avg_score = 0.0
                best_config = None
            else:
                avg_score = round(
                    mean(e["composite_score"] for e in pair_experiments), 4
                )
                best = max(pair_experiments, key=lambda e: e["composite_score"])
                best_config = {
                    "experiment_id": best["experiment_id"],
                    "name": best["name"],
                    "composite_score": best["composite_score"],
                    "weights": best["weights"],
                }

            matrix.append(
                {
                    "component_x": comp_x,
                    "component_y": comp_y,
                    "avg_score": avg_score,
                    "experiment_count": len(pair_experiments),
                    "best_config": best_config,
                }
            )

    return jsonify(
        {
            "success": True,
            "data": {
                "horizon": horizon,
                "components": all_components,
                "matrix": matrix,
            },
        }
    ), 200
