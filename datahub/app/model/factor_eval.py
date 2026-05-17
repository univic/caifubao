# -*- coding: utf-8 -*-
"""Factor evaluation report model."""

import datetime
from mongoengine import (
    DateTimeField,
    DictField,
    IntField,
    StringField,
)
from app.lib.db_watcher.mongoengine_tool import db


class FactorEvalReport(db.Document):
    """Stores a factor evaluation report for later retrieval and comparison."""

    factor_name = StringField(required=True)
    factor_description = StringField()
    start_date = DateTimeField(required=True)
    end_date = DateTimeField(required=True)
    observation_count = IntField(default=0)

    # IC stats per horizon (stored as dict)
    ic_summary = DictField()  # {horizon: {ic_mean, ic_std, count}}
    icir_summary = DictField()  # {horizon: icir_value}
    quintile_analysis = DictField()  # {horizon: [{quintile, avg_return}, ...]}
    correlation_matrix = DictField()  # {component_id: correlation}
    decay_curve = DictField()  # {horizon: ic_mean}

    status = StringField(default="COMPLETED")
    error_msg = StringField()

    created_at = DateTimeField(default=lambda: datetime.datetime.now(datetime.UTC))
    updated_at = DateTimeField()

    meta = {
        "collection": "factor_eval_reports",
        "indexes": [
            "factor_name",
            "-created_at",
            ("factor_name", "-created_at"),
        ],
    }

    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.now(datetime.UTC)
        return super(FactorEvalReport, self).save(*args, **kwargs)
