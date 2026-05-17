# -*- coding: utf-8 -*-
"""Compute-worker model — re-exports from the shared model location.

The canonical ComputeTask model lives in backend/app/model/compute_task.py.
This module re-exports it for convenience within the worker_app package.
"""

from backend.app.model.compute_task import ComputeTask, VALID_TASK_TYPES  # noqa: F401
