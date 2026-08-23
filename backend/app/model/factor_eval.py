# -*- coding: utf-8 -*-
"""Factor evaluation report model — shared with datahub collection."""

import datetime
from mongoengine import (
    DateTimeField,
    DictField,
    Document,
    IntField,
    StringField,
)


class FactorEvalReport(Document):
    """Stores a factor evaluation report for later retrieval and comparison.

    Shares the ``factor_eval_reports`` collection with the datahub
    module so that datahub-produced reports are directly readable by
    the backend API.
    """

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

    # Regime-split IC (15.5)
    regime_ic = DictField()  # {regime: {horizon: {ic_mean, ic_std, ...}}}

    # Component contribution P&L attribution (15.7)
    component_contribution = DictField()
    # {"entry_contributions": {comp: avg}, "exit_contributions": {comp: avg},
    #  "dominant_entry_component": comp_id, "dominant_exit_component": comp_id}

    # Component win-rate analysis (15.8)
    win_rate_by_component = DictField()
    # {component_id: {"trades": N, "win_rate": 0.XX}}

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
