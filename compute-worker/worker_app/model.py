# -*- coding: utf-8 -*-
"""Compute-worker model.

The canonical ``ComputeTask`` model lives in ``backend/app/model/compute_task.py``
and is also present in the worker's merged ``app`` tree, so the worker and the
API always agree on the collection shape.
"""

from app.model.compute_task import ComputeTask, VALID_TASK_TYPES  # noqa: F401

__all__ = ["ComputeTask", "VALID_TASK_TYPES"]
