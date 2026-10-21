# -*- coding: utf-8 -*-
"""Read-only strategy replay artifact (research observation, not forward evidence).

One document per versioned replay id. Written only by the operator-run
``app.jobs.strategy_replay_runner``; the backend reads the same collection. This
collection is separate from ``strategy_paper_runs`` and from the certified
forward windows; it never advances forward evidence.
"""

import datetime

from mongoengine import (
    BooleanField,
    DateTimeField,
    DictField,
    Document,
    FloatField,
    IntField,
    ListField,
    StringField,
)


class StrategyReplay(Document):
    replay_id = StringField(required=True, unique=True)
    schema_version = StringField(required=True)
    source_artifact = StringField(required=True)
    name = StringField(required=True)
    strategy = StringField(required=True)
    description = StringField(default="")
    start_date = StringField(required=True)
    end_date = StringField(required=True)
    data_as_of = StringField()
    aum = FloatField(required=True)
    config = DictField()
    config_hash = StringField(required=True)
    input_hash = StringField(required=True)
    content_hash = StringField()
    evidence_kind = StringField(default="REPLAY", choices=["REPLAY"])
    research_grade = StringField(default="RESEARCH")
    forward_sessions = IntField(default=0)
    forward_required = IntField(default=120)
    forward_eligible = BooleanField(default=False)
    survivorship_gap = BooleanField(default=True)
    limitations = ListField(StringField())
    disclaimer = StringField()
    metrics = DictField()
    curve = ListField(DictField())
    decisions = ListField(DictField())
    fills = ListField(DictField())
    blocked_orders = ListField(DictField())
    generated_at = DateTimeField(default=lambda: datetime.datetime.now(datetime.UTC))
    created_at = DateTimeField(default=lambda: datetime.datetime.now(datetime.UTC))
    updated_at = DateTimeField()

    meta = {
        "collection": "strategy_replays",
        "indexes": [
            "replay_id",
            "-generated_at",
            ("strategy", "-data_as_of"),
        ],
    }

    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.now(datetime.UTC)
        return super(StrategyReplay, self).save(*args, **kwargs)
