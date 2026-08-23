# -*- coding: utf-8 -*-
"""Decision journal model — tracks recommended vs executed decisions with P&L."""

import datetime
from mongoengine import (
    BooleanField,
    DateTimeField,
    FloatField,
    IntField,
    StringField,
    Document,
)


class DecisionJournal(Document):
    """Tracks recommended vs executed decisions with P&L."""

    stock_code = StringField(required=True)
    stock_name = StringField()
    date = DateTimeField(required=True)
    horizon = IntField(choices=[5, 20, 60])
    score = FloatField()
    recommendation = StringField()

    # Recommendation details
    recommended_action = StringField(choices=["BUY", "SELL", "HOLD", "WATCH"])
    confidence = StringField(choices=["high", "medium", "low"])
    entry_price = FloatField()
    target_price = FloatField()
    stop_loss = FloatField()
    position_size_pct = FloatField()

    # Execution tracking
    executed = BooleanField(default=False)
    executed_at = DateTimeField()
    executed_price = FloatField()
    executed_quantity = IntField()
    execution_type = StringField()

    # P&L tracking
    realized_pnl = FloatField()
    realized_pnl_pct = FloatField()
    exit_date = DateTimeField()
    exit_price = FloatField()
    exit_reason = StringField()

    # Attribution
    dominant_component = StringField()

    notes = StringField()
    created_at = DateTimeField(default=lambda: datetime.datetime.now(datetime.UTC))
    updated_at = DateTimeField()

    meta = {
        "collection": "decision_journals",
        "indexes": [
            "stock_code",
            ("date", "-created_at"),
            ("executed", "-created_at"),
            ("execution_type", "-date"),
        ],
    }

    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.now(datetime.UTC)
        return super(DecisionJournal, self).save(*args, **kwargs)
