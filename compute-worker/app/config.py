# -*- coding: utf-8 -*-
"""Compute-worker configuration."""

import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/caifubao")
MONGO_DB = os.getenv("MONGO_DB", "caifubao")

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL", "5"))
MAX_CONCURRENT_TASKS = int(os.getenv("MAX_CONCURRENT_TASKS", "2"))
TASK_MAX_RETRIES = int(os.getenv("TASK_MAX_RETRIES", "3"))

# Module paths for shared model imports
MODEL_PATHS = [
    "backend/app",
    "datahub/app",
]
