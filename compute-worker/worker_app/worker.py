# -*- coding: utf-8 -*-
"""Compute-worker main loop.

Polls the ``compute_tasks`` collection for PENDING tasks, dispatches them to
the appropriate handler, and writes results back to MongoDB.

The worker imports the backend services and the datahub research libraries, so
it runs against a *merged* ``app`` package:

* in the container image the tree is baked at ``/app/app``;
* in a repository checkout the tree is assembled into a temporary directory at
  startup (backend app + datahub model/lib overlay), which keeps local runs and
  the image on the same import layout.

Failure handling: a failed task is requeued with ``retry_count + 1`` until
``max_retries`` is exhausted, then marked FAILED. RUNNING tasks abandoned by a
dead worker are reaped at startup. SIGTERM/SIGINT stop claiming new tasks and
let in-flight tasks finish.
"""

import logging
import signal
import threading
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

from worker_app.config import (
    HEARTBEAT_FILE,
    MAX_CONCURRENT_TASKS,
    MONGO_DB,
    MONGO_URI,
    POLL_INTERVAL_SECONDS,
    TASK_MAX_RETRIES,
    TASK_STALE_MINUTES,
)
from worker_app.model import ComputeTask

logger = logging.getLogger(__name__)

_stop_event = threading.Event()


def _init_db() -> None:
    if MONGO_URI:
        from mongoengine import connect

        connect(host=MONGO_URI, db=MONGO_DB)
        logger.info("Connected to MongoDB via MONGO_URI (db=%s)", MONGO_DB)
        return

    from app.lib.db_watcher.mongoengine_tool import mongo_watcher

    mongo_watcher.get_db_connection()
    logger.info("Connected to MongoDB via shared db_watcher (db=%s)", MONGO_DB)


# ---------------------------------------------------------------------------
# Task lifecycle
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _fetch_next_task() -> ComputeTask | None:
    """Atomically claim the oldest PENDING task."""
    task = ComputeTask.objects(status="PENDING").order_by("created_at").first()
    if task is None:
        return None
    updated = ComputeTask.objects(id=task.id, status="PENDING").update_one(
        set__status="RUNNING",
        set__started_at=_utcnow(),
        set__updated_at=_utcnow(),
    )
    if updated == 0:
        return None  # claimed by another worker
    task.reload()
    return task


def _schedule_retry(task: ComputeTask, error: str) -> None:
    """Requeue a failed task until its retry budget is exhausted."""
    retries = int(task.retry_count or 0) + 1
    max_retries = (
        int(task.max_retries) if task.max_retries is not None else int(TASK_MAX_RETRIES)
    )
    task.error = error
    task.retry_count = retries
    if retries <= max_retries:
        task.status = "PENDING"
        task.progress = 0.0
        task.progress_message = f"retry {retries}/{max_retries}"
        task.started_at = None
        task.completed_at = None
        logger.warning(
            "Task %s failed (attempt %s/%s), requeued: %s",
            task.id,
            retries,
            max_retries,
            error.splitlines()[-1] if error else "unknown error",
        )
    else:
        task.status = "FAILED"
        task.completed_at = _utcnow()
        logger.error("Task %s exhausted its retry budget and failed", task.id)
    task.save()


def _run_task(task: ComputeTask) -> None:
    from worker_app.handlers import handle_task

    try:
        handle_task(task)
    except Exception:
        logger.exception("Task %s raised", task.id)
        _schedule_retry(task, traceback.format_exc())
        return

    if task.status == "FAILED":
        _schedule_retry(task, task.error or "task handler reported failure")
    else:
        logger.info("Task %s finished status=%s", task.id, task.status)


def _reap_stale_tasks() -> int:
    """Requeue RUNNING tasks abandoned by a dead worker."""
    cutoff = _utcnow() - timedelta(minutes=TASK_STALE_MINUTES)
    stale = list(ComputeTask.objects(status="RUNNING", started_at__lt=cutoff))
    for task in stale:
        logger.warning(
            "Reaping stale RUNNING task %s (started_at=%s)", task.id, task.started_at
        )
        _schedule_retry(
            task,
            f"stale RUNNING task reaped after {TASK_STALE_MINUTES} minutes",
        )
    return len(stale)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def _install_signal_handlers() -> None:
    def _handle(signum, _frame):
        logger.info(
            "Received %s — finishing in-flight tasks before exiting",
            signal.Signals(signum).name,
        )
        _stop_event.set()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)


def _touch_heartbeat() -> None:
    """Update the liveness heartbeat file (best effort)."""
    try:
        Path(HEARTBEAT_FILE).touch()
    except OSError:
        logger.debug("Cannot touch heartbeat file %s", HEARTBEAT_FILE)


def _worker_loop(name: str) -> None:
    while not _stop_event.is_set():
        _touch_heartbeat()
        try:
            task = _fetch_next_task()
        except Exception:
            logger.exception("[%s] failed to claim a task", name)
            _stop_event.wait(POLL_INTERVAL_SECONDS)
            continue

        if task is None:
            _stop_event.wait(POLL_INTERVAL_SECONDS)
            continue

        logger.info("[%s] claimed task %s type=%s", name, task.id, task.task_type)
        _run_task(task)

    logger.info("[%s] loop stopped", name)


def run_forever() -> None:
    """Start the configured number of workers and block until shutdown."""
    _init_db()
    _install_signal_handlers()

    reaped = _reap_stale_tasks()
    _touch_heartbeat()
    logger.info(
        "Compute-worker starting — concurrency=%d poll_interval=%ds "
        "max_retries=%d stale_after=%dm reaped=%d",
        MAX_CONCURRENT_TASKS,
        POLL_INTERVAL_SECONDS,
        TASK_MAX_RETRIES,
        TASK_STALE_MINUTES,
        reaped,
    )

    threads = [
        threading.Thread(target=_worker_loop, args=(f"worker-{i + 1}",))
        for i in range(MAX_CONCURRENT_TASKS)
    ]
    for thread in threads:
        thread.start()

    try:
        while any(thread.is_alive() for thread in threads):
            time.sleep(0.5)
    except KeyboardInterrupt:
        _stop_event.set()

    for thread in threads:
        thread.join()
    logger.info("Compute-worker stopped")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_forever()
