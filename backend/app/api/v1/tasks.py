# -*- coding: utf-8 -*-
"""Compute-task management API blueprint.

Provides endpoints to create async compute tasks and poll their results.
These tasks are consumed by the compute-worker service running on dedicated
hardware (5600X node).
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

from flask import Blueprint, jsonify, request
from mongoengine import ValidationError

from app.model.compute_task import ComputeTask, VALID_TASK_TYPES
from app.lib.auth_decorators import block_service_tokens
from app.services.backtest_service import SCORE_DRIVEN_STRATEGIES

logger = logging.getLogger(__name__)

tasks_bp = Blueprint("tasks", __name__, url_prefix="/api/tasks")
tasks_bp.before_request(block_service_tokens)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _request_id() -> str:
    return str(uuid.uuid4())


def _ok(data: Any = None, message: str = "ok") -> Dict[str, Any]:
    return {
        "success": True,
        "message": message,
        "request_id": _request_id(),
        "generated_at": _now_utc(),
        "data": data,
    }


def _fail(message: str, status_code: int = 400) -> tuple:
    return (
        jsonify(
            {
                "success": False,
                "message": message,
                "request_id": _request_id(),
                "generated_at": _now_utc(),
                "data": None,
            }
        ),
        status_code,
    )


def _serialize_task(task: ComputeTask) -> Dict[str, Any]:
    return {
        "id": str(task.id),
        "task_type": task.task_type,
        "status": task.status,
        "progress": task.progress,
        "progress_message": task.progress_message,
        "result": task.result,
        "error": task.error,
        "params": task.params,
        "retry_count": task.retry_count,
        "created_at": task.created_at.isoformat() if task.created_at else None,
        "started_at": task.started_at.isoformat() if task.started_at else None,
        "completed_at": task.completed_at.isoformat() if task.completed_at else None,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@tasks_bp.route("", methods=["POST"])
def create_task():
    """Create a new compute task.

    Request body (JSON):
        task_type  : str (required) — one of the valid task types
        params     : dict (required) — task-specific parameters

    Valid task_type values:
        BACKTEST_SINGLE    — single-stock backtest
        BACKTEST_MULTI     — multi-stock portfolio backtest
        GRID_SEARCH        — scoring weight/threshold grid search
        SCORE_REPLAY       — historical score backfill
        SCORE_VERIFY       — verification run
        CALIBRATION_REPORT — calibration report generation
        FACTOR_EVAL        — factor evaluation
        ROLLING_VALIDATION — rolling cross-validation
    """
    payload = request.get_json(silent=True) or {}
    task_type = (payload.get("task_type") or "").strip()
    params = payload.get("params")

    if not task_type:
        return _fail("task_type is required")
    if not isinstance(params, dict):
        return _fail("params must be a dict")

    valid_types = list(VALID_TASK_TYPES)
    if task_type not in valid_types:
        return _fail(f"Invalid task_type. Must be one of: {', '.join(valid_types)}")
    if task_type in ("BACKTEST_SINGLE", "BACKTEST_MULTI", "BACKTEST_SCAN"):
        strategy = str(params.get("strategy") or "").strip().upper()
        model_version = str(params.get("model_version") or "").strip()
        if strategy in SCORE_DRIVEN_STRATEGIES and not model_version:
            return _fail("model_version is required for score-driven strategies")

    try:
        task = ComputeTask(
            task_type=task_type,
            params=params,
            status="PENDING",
        )
        task.save()
        logger.info("Created task %s type=%s", task.id, task_type)
        return jsonify(_ok(_serialize_task(task), "Task created")), 201
    except ValidationError as exc:
        return _fail(str(exc))


@tasks_bp.route("/<task_id>", methods=["GET"])
def get_task(task_id: str):
    """Poll a task by its ID.

    Returns the current status, progress, result, and error (if any).
    """
    try:
        task = ComputeTask.objects(id=task_id).first()
    except ValidationError:
        task = None

    if task is None:
        return _fail("Task not found", 404)

    return jsonify(_ok(_serialize_task(task)))


@tasks_bp.route("", methods=["GET"])
def list_tasks():
    """List tasks with optional status filter and pagination.

    Query params:
        status  : str (optional) — filter by status
        page    : int (optional) — default 1
        per_page: int (optional) — default 20
    """
    page = max(1, int(request.args.get("page", 1)))
    per_page = min(max(1, int(request.args.get("per_page", 20))), 100)
    status_filter = request.args.get("status")

    query = ComputeTask.objects
    if status_filter:
        query = query.filter(status=status_filter)

    total = query.count()
    items = list(
        query.order_by("-created_at").skip((page - 1) * per_page).limit(per_page)
    )

    return jsonify(
        _ok(
            {
                "items": [_serialize_task(t) for t in items],
                "total": total,
                "page": page,
                "per_page": per_page,
            }
        )
    )


@tasks_bp.route("/<task_id>", methods=["DELETE"])
def cancel_task(task_id: str):
    """Cancel a pending or running task."""
    try:
        task = ComputeTask.objects(id=task_id).first()
    except ValidationError:
        task = None

    if task is None:
        return _fail("Task not found", 404)

    if task.status not in ("PENDING", "RUNNING"):
        return _fail(f"Cannot cancel task with status {task.status}")

    task.status = "CANCELLED"
    task.completed_at = datetime.now(timezone.utc)
    task.save()

    logger.info("Cancelled task %s", task.id)
    return jsonify(_ok(_serialize_task(task), "Task cancelled"))
