# -*- coding: utf-8 -*-
"""Factor evaluation report API — list, detail, and component discovery."""

import logging
from collections import defaultdict

from flask import Blueprint, jsonify, request

from app.lib.auth_decorators import block_service_tokens
from app.model.factor_eval import FactorEvalReport

logger = logging.getLogger(__name__)

factor_eval_bp = Blueprint("factor_eval", __name__, url_prefix="/api/factor-eval")
factor_eval_bp.before_request(block_service_tokens)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _serialize_report(r: FactorEvalReport, detail: bool = False) -> dict:
    ic_summary = r.ic_summary or {}
    icir_summary = r.icir_summary or {}
    correlation_raw = r.correlation_matrix or {}
    decay_raw = r.decay_curve or {}

    # Flat IC/ICIR fields for list view (pick Score20 as default display)
    score20_ic = ic_summary.get("20", {})
    score20_icir = icir_summary.get("20")

    result: dict = {
        "id": str(r.id),
        "factor_name": r.factor_name,
        "factor_description": r.factor_description,
        "start_date": r.start_date.isoformat() if r.start_date else None,
        "end_date": r.end_date.isoformat() if r.end_date else None,
        "observation_count": r.observation_count,
        "ic_summary": ic_summary,
        "icir_summary": icir_summary,
        "quintile_analysis": r.quintile_analysis,
        "status": r.status,
        "error_msg": r.error_msg,
        "created_at": r.created_at.isoformat() if r.created_at else None,
        # Flattened fields for list view (frontend FactorEvalReportSummary)
        "ic_mean_score20": score20_ic.get("ic_mean") if score20_ic else None,
        "icir_score20": score20_icir,
    }

    if detail:
        result.update(
            {
                # Frontend expects arrays of {component, corr} and {horizon, ic_mean}
                "correlation_matrix": [
                    {"component": cid, "corr": cv}
                    for cid, cv in correlation_raw.items()
                ],
                "decay_curve": [
                    {"horizon": int(h), "ic_mean": v} for h, v in decay_raw.items()
                ],
                "regime_ic": r.regime_ic,
                "component_contribution": r.component_contribution,
                "win_rate_by_component": r.win_rate_by_component,
            }
        )

    return result


# ---------------------------------------------------------------------------
# List reports
# ---------------------------------------------------------------------------


@factor_eval_bp.route("/reports", methods=["GET"])
def list_reports():
    """List factor evaluation reports.

    Query params:
        factor_name : str  — filter by factor name
        limit       : int  — max results (default 20)
    """
    factor_name = (request.args.get("factor_name") or "").strip()
    limit = min(int(request.args.get("limit", 20)), 100)

    query = FactorEvalReport.objects()
    if factor_name:
        query = query.filter(factor_name=factor_name)

    reports = list(query.order_by("-created_at").limit(limit))

    return jsonify(
        {
            "success": True,
            "data": {
                "items": [_serialize_report(r) for r in reports],
                "total": len(reports),
            },
        }
    )


# ---------------------------------------------------------------------------
# Single report detail
# ---------------------------------------------------------------------------


@factor_eval_bp.route("/reports/<report_id>", methods=["GET"])
def get_report(report_id: str):
    """Get a single factor evaluation report with full detail."""
    try:
        report = FactorEvalReport.objects(id=report_id).first()
    except Exception:
        report = None

    if report is None:
        return (
            jsonify({"success": False, "message": "Report not found", "data": None}),
            404,
        )

    return jsonify({"success": True, "data": _serialize_report(report, detail=True)})


# ---------------------------------------------------------------------------
# List available scoring components
# ---------------------------------------------------------------------------


@factor_eval_bp.route("/components", methods=["GET"])
def list_components():
    """List available scoring component names from score predictions.

    Scans StockScorePrediction explanations to discover which component
    IDs have been used, and how many predictions they appear in.
    """
    from app.model.scoring import StockScorePrediction

    preview = list(StockScorePrediction.objects().only("explanation").limit(2000))

    component_counts: dict[str, int] = defaultdict(int)
    component_labels: dict[str, str] = {}

    for pred in preview:
        explanation = pred.explanation or {}
        components = explanation.get("components", [])
        for comp in components:
            if isinstance(comp, dict):
                comp_id = comp.get("id")
                if comp_id:
                    component_counts[comp_id] += 1
                    if comp_id not in component_labels:
                        component_labels[comp_id] = comp.get("label", comp_id)

    items = sorted(
        [
            {
                "component_id": cid,
                "label": component_labels.get(cid, cid),
                "prediction_count": cnt,
            }
            for cid, cnt in component_counts.items()
        ],
        key=lambda c: -c["prediction_count"],
    )

    return jsonify(
        {
            "success": True,
            "data": {
                "items": items,
                "total": len(items),
            },
        }
    )
