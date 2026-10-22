# -*- coding: utf-8 -*-
"""Read-only API for strategy replay research artifacts.

``GET /api/strategy/replays`` returns paginated summaries and
``GET /api/strategy/replays/<replay_id>`` returns one full artifact. Both
require a user JWT and reject OpenClaw service tokens. There is deliberately no
create/update/delete route: the collection is written only by the operator-run
datahub replay runner.
"""

import datetime

from caifubao_contracts.time import iso_utc
import logging
import uuid
from typing import Any, Dict

from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required

from app.lib.auth_decorators import block_service_tokens
from app.model.strategy_replay import StrategyReplay

logger = logging.getLogger(__name__)

strategy_replays_bp = Blueprint(
    "strategy_replays", __name__, url_prefix="/api/strategy/replays"
)
strategy_replays_bp.before_request(block_service_tokens)

DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 200
# Bound `page` so an absurd skip cannot overflow the Mongo int64 and 500.
MAX_PAGE = 1_000_000
DEFAULT_FORWARD_REQUIRED = 120

# Keys whose wire representation is a date-only ISO string ("YYYY-MM-DD").
_DATE_KEYS = {
    "date",
    "decision_date",
    "execution_date",
    "start_date",
    "end_date",
    "data_as_of",
}


# ---------------------------------------------------------------------------
# Envelope helpers (same contract as /api/backtest)
# ---------------------------------------------------------------------------


def _now_utc_iso() -> str:
    return iso_utc()


def _request_id() -> str:
    return str(uuid.uuid4())


def _ok(data: Any = None, message: str = "ok") -> Dict[str, Any]:
    return {
        "success": True,
        "message": message,
        "request_id": _request_id(),
        "generated_at": _now_utc_iso(),
        "data": data,
    }


def _fail(message: str, status_code: int = 400) -> tuple:
    return (
        jsonify(
            {
                "success": False,
                "message": message,
                "request_id": _request_id(),
                "generated_at": _now_utc_iso(),
                "data": None,
            }
        ),
        status_code,
    )


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def _serialize_value(value: Any, key: str | None = None) -> Any:
    """Convert datetimes to ISO strings recursively.

    The frontend must never depend on BSON datetime serialization. Date-like
    keys are normalized to date-only ISO strings; other datetimes keep their
    full ISO timestamp.
    """
    if isinstance(value, datetime.datetime):
        if key in _DATE_KEYS:
            return value.date().isoformat()
        return value.isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, dict):
        return {
            item_key: _serialize_value(item, item_key)
            for item_key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_serialize_value(item, key) for item in value]
    if key in _DATE_KEYS and isinstance(value, str) and len(value) >= 10:
        return value[:10]
    return value


def _forward_fields() -> Dict[str, Any]:
    """Re-assert the replay-is-not-forward contract for every response.

    The artifact persists these values, but the API enforces them so a
    malformed document can never make a replay look like forward evidence.
    """
    return {
        "forward_sessions": 0,
        "forward_required": DEFAULT_FORWARD_REQUIRED,
        "forward_eligible": False,
    }


def _summary(row: StrategyReplay) -> Dict[str, Any]:
    """List-view summary: metadata + metrics only (no curve/decisions/fills)."""
    payload = {
        "replay_id": getattr(row, "replay_id", None),
        "name": getattr(row, "name", None),
        "strategy": getattr(row, "strategy", None),
        "description": getattr(row, "description", None),
        "start_date": getattr(row, "start_date", None),
        "end_date": getattr(row, "end_date", None),
        "aum": getattr(row, "aum", None),
        "config_hash": getattr(row, "config_hash", None),
        "input_hash": getattr(row, "input_hash", None),
        "content_hash": getattr(row, "content_hash", None),
        "schema_version": getattr(row, "schema_version", None),
        "source_artifact": getattr(row, "source_artifact", None),
        "evidence_kind": getattr(row, "evidence_kind", None) or "REPLAY",
        "data_as_of": getattr(row, "data_as_of", None),
        "generated_at": getattr(row, "generated_at", None),
        "research_grade": getattr(row, "research_grade", None) or "RESEARCH",
        "survivorship_gap": bool(getattr(row, "survivorship_gap", True)),
        "disclaimer": getattr(row, "disclaimer", None),
        "limitations": list(getattr(row, "limitations", None) or []),
        "metrics": dict(getattr(row, "metrics", None) or {}),
    }
    payload.update(_forward_fields())
    return _serialize_value(payload)


def _detail(row: StrategyReplay) -> Dict[str, Any]:
    """Full artifact including the decision, fill and curve process."""
    payload = _summary(row)
    payload.update(
        {
            "config": dict(getattr(row, "config", None) or {}),
            "curve": list(getattr(row, "curve", None) or []),
            "decisions": list(getattr(row, "decisions", None) or []),
            "fills": list(getattr(row, "fills", None) or []),
            "blocked_orders": list(getattr(row, "blocked_orders", None) or []),
        }
    )
    return _serialize_value(payload)


# ---------------------------------------------------------------------------
# Query parsing
# ---------------------------------------------------------------------------


def _parse_int(
    value: Any, default: int, minimum: int = 0, maximum: int | None = None
) -> int:
    """Parse and clamp a query integer; invalid input uses the default."""
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    parsed = max(parsed, minimum)
    if maximum is not None:
        parsed = min(parsed, maximum)
    return parsed


# ---------------------------------------------------------------------------
# Routes (read-only)
# ---------------------------------------------------------------------------


@strategy_replays_bp.route("", methods=["GET"])
@jwt_required()
def list_strategy_replays():
    """Paginated replay summaries (default 20, max 200)."""
    page = _parse_int(request.args.get("page"), 1, minimum=1, maximum=MAX_PAGE)
    page_size = _parse_int(
        request.args.get("page_size"),
        DEFAULT_PAGE_SIZE,
        minimum=1,
        maximum=MAX_PAGE_SIZE,
    )

    try:
        query = StrategyReplay.objects()
        total = query.count()
        rows = list(
            query.order_by("-generated_at", "-replay_id")
            .skip((page - 1) * page_size)
            .limit(page_size)
        )
    except Exception:  # noqa: BLE001 - DB failure returns the JSON envelope
        logger.exception("Failed to list strategy replays")
        return _fail("Failed to load strategy replays", 500)

    return (
        jsonify(
            _ok(
                data={
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                    "items": [_summary(row) for row in rows],
                }
            )
        ),
        200,
    )


@strategy_replays_bp.route("/<replay_id>", methods=["GET"])
@jwt_required()
def get_strategy_replay(replay_id: str):
    """Return one full replay artifact, or the 404 envelope when missing."""
    try:
        row = StrategyReplay.objects(replay_id=replay_id).first()
    except Exception:  # noqa: BLE001 - DB failure returns the JSON envelope
        logger.exception("Failed to read strategy replay %s", replay_id)
        return _fail("Failed to load strategy replay", 500)

    if row is None:
        return _fail(f"Strategy replay not found: {replay_id}", 404)

    return jsonify(_ok(data=_detail(row))), 200
