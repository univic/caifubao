# -*- coding: utf-8 -*-
"""Compute-worker configuration."""

import os

# Optional full connection string. When empty the worker connects through the
# shared ``app.lib.db_watcher`` using the standard MONGODB_* variables, so it
# consumes the same ConfigMap/Secret as the backend and datahub.
MONGO_URI = os.getenv("MONGO_URI", "")
MONGO_DB = os.getenv("MONGO_DB", os.getenv("MONGODB_NAME", "caifubao"))

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL", "5"))
# Number of tasks executed concurrently. Every thread claims tasks atomically
# from the ``compute_tasks`` collection, so this is safe to raise.
MAX_CONCURRENT_TASKS = max(int(os.getenv("MAX_CONCURRENT_TASKS", "1")), 1)
TASK_MAX_RETRIES = max(int(os.getenv("TASK_MAX_RETRIES", "3")), 0)
# A RUNNING task older than this is assumed to belong to a dead worker and is
# requeued (or failed once retries are exhausted) at startup.
TASK_STALE_MINUTES = max(int(os.getenv("TASK_STALE_MINUTES", "360")), 1)
# Liveness heartbeat: the worker touches this file on every poll cycle and the
# Kubernetes liveness probe fails when it goes stale, so a wedged process is
# actually restarted (the old probe only ran `sys.exit(0)`).
HEARTBEAT_FILE = os.getenv("HEARTBEAT_FILE", "/tmp/compute-worker.heartbeat")
HEARTBEAT_STALE_SECONDS = max(int(os.getenv("HEARTBEAT_STALE_SECONDS", "300")), 30)
