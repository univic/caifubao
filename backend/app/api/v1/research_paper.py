"""Read-only Research paper observations; never a Trading Paper account.

All endpoints require a user JWT. FORWARD is counted only against an ACTIVE,
exact-spec certified window. NAV snapshots are explicitly UNCERTIFIED because
the research corporate-action projection is still undergoing reconciliation.
"""

from __future__ import annotations

import datetime
import logging
import uuid

from caifubao_contracts.time import iso_utc
from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required

from app.lib.auth_decorators import block_service_tokens
from app.lib.research_paper_lineage import inspect_research_trace

logger = logging.getLogger(__name__)
research_paper_bp = Blueprint(
    "research_paper", __name__, url_prefix="/api/strategy/paper"
)
research_paper_bp.before_request(block_service_tokens)

VALID_HORIZONS = {5, 20, 60}
FORWARD_REQUIRED = 120
MAX_RUNS = 400


def _db():
    from mongoengine import get_db

    return get_db()


def _ok(payload):
    return jsonify(
        {
            "success": True,
            "message": "ok",
            "request_id": str(uuid.uuid4()),
            "generated_at": iso_utc(),
            "data": payload,
        }
    )


def _fail(message: str, code: int = 400):
    return (
        jsonify(
            {
                "success": False,
                "message": message,
                "request_id": str(uuid.uuid4()),
                "generated_at": iso_utc(),
                "data": None,
            }
        ),
        code,
    )


def _date(value):
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    return str(value)[:10] if value else None


def _stamp(value):
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    return str(value) if value else None


def _identity():
    model_version = (request.args.get("model_version") or "").strip()
    config_hash = (request.args.get("config_hash") or "").strip()
    kind = (request.args.get("hash_kind") or "spec").strip()
    try:
        horizon = int(request.args.get("horizon", "20"))
    except ValueError:
        return None
    if (
        not model_version
        or len(model_version) > 100
        or not config_hash
        or len(config_hash) > 128
        or horizon not in VALID_HORIZONS
        or kind not in ("spec", "legacy")
    ):
        return None
    return {
        "model_version": model_version,
        "horizon": horizon,
        "config_hash": config_hash,
        "hash_kind": kind,
    }


def _window(db, identity):
    row = db["strategy_forward_windows"].find_one(
        {**identity, "status": "ACTIVE"}, {"_id": 0, "start_date": 1, "status": 1}
    )
    return {"status": "ACTIVE", "start_date": _date(row["start_date"])} if row else None


def _run_projection(row):
    nav = row.get("nav_snapshot") or {}
    return {
        "date": _date(row.get("date")),
        "decision_at": _stamp(row.get("decision_at")),
        "execution_date": _date(row.get("execution_date")),
        "status": row.get("status"),
        "skip_reason": row.get("skip_reason"),
        "evidence_kind": row.get("evidence_kind") or "REPLAY",
        "hash_kind": row.get("hash_kind") or "legacy",
        "model_version": row.get("model_version"),
        "config_hash": row.get("config_hash"),
        "horizon": row.get("horizon"),
        "strategy_name": row.get("strategy_name"),
        "target_holdings": row.get("target_holdings") or [],
        "rebalance": row.get("rebalance") or {},
        "nav_snapshot": {
            "date": _date(nav.get("date")),
            "nav": nav.get("nav"),
            "daily_return": nav.get("daily_return"),
            "turnover": nav.get("turnover"),
            "drawdown": nav.get("drawdown"),
            "benchmark_return": nav.get("benchmark_return"),
            "positions_count": nav.get("positions_count"),
            "stale_positions_count": nav.get("stale_positions_count"),
        }
        if nav
        else None,
        # Intents, risk decisions and simulated fills are NOT stored in these
        # plans; do not invent a trading account or pretend targets are fills.
        "execution_trace": "NOT_LINKED",
        "accounting_status": "UNVERIFIED",
    }


def _forward_count(rows, window, identity):
    if not window or identity["hash_kind"] != "spec":
        return 0
    return len(
        {
            _date(row.get("date"))
            for row in rows
            if row.get("status") == "COMPLETED"
            and row.get("evidence_kind") == "FORWARD"
            and row.get("hash_kind") == "spec"
            and _date(row.get("date")) >= window["start_date"]
        }
    )


@research_paper_bp.route("/identities", methods=["GET"])
@jwt_required()
def list_research_paper_identities():
    try:
        rows = list(
            _db()["strategy_paper_runs"]
            .find(
                {},
                {
                    "_id": 0,
                    "date": 1,
                    "model_version": 1,
                    "horizon": 1,
                    "hash_kind": 1,
                    "config_hash": 1,
                    "evidence_kind": 1,
                },
            )
            .sort("date", -1)
            .limit(300)
        )
    except Exception:  # noqa: BLE001
        logger.exception("Cannot list research paper identities")
        return _fail("Failed to load research cohorts", 500)
    identities = {}
    for row in rows:
        key = (
            row.get("model_version"),
            row.get("horizon"),
            row.get("hash_kind") or "legacy",
            row.get("config_hash"),
        )
        if not all(key):
            continue
        if key not in identities:
            identities[key] = {
                "model_version": key[0],
                "horizon": key[1],
                "hash_kind": key[2],
                "config_hash": key[3],
                "latest_date": _date(row.get("date")),
                "last_record_kind": row.get("evidence_kind") or "REPLAY",
            }
    return _ok({"items": list(identities.values()), "scan_limit": 300})


@research_paper_bp.route("/overview", methods=["GET"])
@jwt_required()
def research_paper_overview():
    identity = _identity()
    if identity is None:
        return _fail("model_version, horizon, hash_kind and config_hash required", 400)
    try:
        db = _db()
        window = _window(db, identity)
        rows = list(
            db["strategy_paper_runs"]
            .find({**identity}, {"_id": 0})
            .sort("date", -1)
            .limit(MAX_RUNS)
        )
    except Exception:  # noqa: BLE001
        logger.exception("Cannot read research paper overview")
        return _fail("Failed to load research paper overview", 500)
    rows.reverse()
    projected = [_run_projection(row) for row in rows]
    curve = [
        {
            "date": item["nav_snapshot"]["date"] or item["execution_date"],
            "nav": item["nav_snapshot"]["nav"],
            "daily_return": item["nav_snapshot"]["daily_return"],
            "drawdown": item["nav_snapshot"]["drawdown"],
            "benchmark_return": item["nav_snapshot"]["benchmark_return"],
            "stale_positions_count": item["nav_snapshot"]["stale_positions_count"],
        }
        for item in projected
        if item["nav_snapshot"] is not None
        and isinstance(item["nav_snapshot"]["nav"], (int, float))
    ]
    count = _forward_count(rows, window, identity)
    return _ok(
        {
            "identity": identity,
            "window": window,
            "forward_observed_sessions": count,
            "forward_required": FORWARD_REQUIRED,
            "forward_eligible": False,
            "forward_status": (
                "ACTIVE_OBSERVATION_ONLY" if window else "NO_ACTIVE_WINDOW"
            ),
            "accounting_status": "UNVERIFIED",
            "account_type": "RESEARCH_SIMULATION_NOT_TRADING_ACCOUNT",
            "provenance_note": (
                "Only matching completed FORWARD plans in an ACTIVE spec "
                "window count; historical replay does not. "
                "NAV lacks certified corporate-action reconciliation."
            ),
            "curve": curve,
            "decisions": projected[-50:][::-1],
            "has_more_runs": len(rows) == MAX_RUNS,
            "total_loaded": len(rows),
        }
    )


def _simulation_candidates(db, run):
    """Fetch at most two matching derivations to detect ambiguous provenance."""
    execution = run.get("execution_date")
    if not isinstance(execution, datetime.datetime):
        return []
    return list(
        db["strategy_simulation_artifacts"]
        .find(
            {
                "strategy_name": run.get("strategy_name"),
                "config_hash": run.get("config_hash"),
                "hash_kind": run.get("hash_kind") or "legacy",
                "execution_from": {"$lte": execution},
                "execution_to": {"$gte": execution},
            },
            {
                "_id": 0,
                "strategy_name": 1,
                "config_hash": 1,
                "hash_kind": 1,
                "artifact_hash": 1,
                "payload": 1,
            },
        )
        .limit(2)
    )


@research_paper_bp.route("/decisions/<date>", methods=["GET"])
@jwt_required()
def research_paper_decision(date):
    identity = _identity()
    if identity is None:
        return _fail("model_version, horizon, hash_kind and config_hash required", 400)
    try:
        parsed = datetime.date.fromisoformat(date)
        if parsed.isoformat() != date:
            raise ValueError("noncanonical date")
    except ValueError:
        return _fail("decision date must be YYYY-MM-DD", 400)
    target = datetime.datetime.combine(parsed, datetime.time.min)
    try:
        row = _db()["strategy_paper_runs"].find_one(
            {**identity, "date": target}, {"_id": 0}
        )
    except Exception:  # noqa: BLE001
        logger.exception("Cannot read research paper decision")
        return _fail("Failed to load research decision", 500)
    if row is None:
        return _fail("Research strategy decision not found", 404)
    try:
        candidates = _simulation_candidates(_db(), row)
    except Exception:  # noqa: BLE001 - lineage DB failures must not suggest fills
        logger.exception("Research artifact read failed for decision %s", date)
        return _fail("Failed to verify research simulation lineage", 500)
    return _ok(
        {
            "identity": identity,
            "decision": _run_projection(row),
            "trace": inspect_research_trace(row, candidates),
        }
    )
