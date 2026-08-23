# -*- coding: utf-8 -*-
"""ComputeTask model — the job queue document.

Shared between backend API (task creation/polling) and compute-worker
(task execution).  Defined here in backend/app/model so both services
can import it without cross-module dependencies.
"""

import datetime
from mongoengine import (
    DateTimeField,
    DictField,
    Document,
    FloatField,
    IntField,
    StringField,
)

VALID_TASK_TYPES = (
    "BACKTEST_SINGLE",
    "BACKTEST_MULTI",
    "BACKTEST_SCAN",
    "GRID_SEARCH",
    "SCORE_REPLAY",
    "SCORE_VERIFY",
    "CALIBRATION_REPORT",
    "FACTOR_EVAL",
    "ROLLING_VALIDATION",
)


class ComputeTask(Document):
    """A unit of asynchronous compute work.

    Created by the backend API, consumed by the compute-worker.
    """

    task_type = StringField(required=True, choices=VALID_TASK_TYPES)
    params = DictField(required=True)
    status = StringField(
        default="PENDING",
        choices=["PENDING", "RUNNING", "COMPLETED", "FAILED", "CANCELLED"],
    )
    progress = FloatField(default=0.0)
    progress_message = StringField()
    result = DictField()
    error = StringField()
    retry_count = IntField(default=0)
    max_retries = IntField(default=3)
    created_at = DateTimeField(default=lambda: datetime.datetime.now(datetime.UTC))
    started_at = DateTimeField()
    completed_at = DateTimeField()
    updated_at = DateTimeField()

    meta = {
        "collection": "compute_tasks",
        "indexes": [
            ("status", "-created_at"),
            ("task_type", "status"),
            "-created_at",
        ],
    }

    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.now(datetime.UTC)
        return super().save(*args, **kwargs)
