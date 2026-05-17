# -*- coding: utf-8 -*-
"""Compute-worker main loop.

Polls the compute_tasks collection for PENDING tasks, dispatches them to
the appropriate handler, and writes results back to MongoDB.
"""

import logging
import sys
import time
import traceback
from datetime import datetime, timezone

from worker_app.config import MONGO_URI, MONGO_DB, POLL_INTERVAL_SECONDS, MAX_CONCURRENT_TASKS
from worker_app.model import ComputeTask

logger = logging.getLogger(__name__)


def _init_db() -> None:
    """Connect to MongoDB via mongoengine."""
    from mongoengine import connect

    connect(host=MONGO_URI, db=MONGO_DB)
    logger.info("Connected to MongoDB: %s / %s", MONGO_URI, MONGO_DB)


def _add_sys_paths() -> None:
    """Add backend and datahub to sys.path so handlers can import models."""
    import os
    scope_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    backend = os.path.join(scope_dir, "backend", "app")
    datahub = os.path.join(scope_dir, "datahub", "app")
    for p in [backend, datahub]:
        if os.path.isdir(p) and p not in sys.path:
            sys.path.insert(0, p)
    sys.path.insert(0, os.path.join(scope_dir, "datahub"))


def _fetch_next_task() -> ComputeTask | None:
    """Atomically claim the oldest PENDING task."""
    task = ComputeTask.objects(status="PENDING").order_by("created_at").first()
    if task is None:
        return None
    # Atomically claim
    updated = ComputeTask.objects(id=task.id, status="PENDING").update_one(
        set__status="RUNNING",
        set__started_at=datetime.now(timezone.utc),
    )
    if updated == 0:
        return None  # claimed by another worker
    task.reload()
    return task


def _run_task(task: ComputeTask) -> None:
    """Execute a single task and handle status updates."""
    from worker_app.handlers import handle_task

    try:
        handle_task(task)
    except Exception:
        logger.exception("Task %s failed", task.id)
        try:
            task.status = "FAILED"
            task.error = traceback.format_exc()
            task.completed_at = datetime.now(timezone.utc)
            task.save()
        except Exception:
            logger.exception("Failed to save FAILED status for task %s", task.id)


def run_forever() -> None:
    """Main worker loop. Blocks until interrupted."""
    _add_sys_paths()
    _init_db()

    logger.info(
        "Compute-worker starting — poll_interval=%ds, max_concurrent=%d",
        POLL_INTERVAL_SECONDS,
        MAX_CONCURRENT_TASKS,
    )

    while True:
        try:
            task = _fetch_next_task()
            if task is not None:
                logger.info("Claimed task %s type=%s", task.id, task.task_type)
                _run_task(task)
                logger.info("Task %s completed status=%s", task.id, task.status)
            else:
                time.sleep(POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("Worker shutting down")
            break
        except Exception:
            logger.exception("Worker loop error — sleeping %ds", POLL_INTERVAL_SECONDS)
            time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_forever()
