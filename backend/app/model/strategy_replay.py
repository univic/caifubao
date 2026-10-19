# -*- coding: utf-8 -*-
"""Read model for the immutable strategy replay research artifact.

The ``strategy_replays`` collection is produced by the operator-run datahub
research runner (``datahub/app/jobs/strategy_replay_runner.py``); the backend
only reads it. A replay is research evidence (``evidence_kind=REPLAY``), never
forward validation, and it never feeds the forward-progress counters.
"""

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
    """One versioned, read-only strategy replay artifact.

    ``replay_id`` is the stable versioned slug ``<strategy>_<window>_<aum>_vN``.
    All process fields (``curve``/``decisions``/``fills``/``blocked_orders``)
    belong to the same run and config hash; the backend never mutates them.
    """

    replay_id = StringField(required=True, unique=True)
    name = StringField()
    strategy = StringField()
    description = StringField()

    # As-of range. Date-only values ("YYYY-MM-DD") are the canonical wire
    # format; the serializer also tolerates legacy datetime values.
    start_date = StringField()
    end_date = StringField()
    aum = FloatField()

    config = DictField()
    config_hash = StringField()
    input_hash = StringField()
    content_hash = StringField()
    schema_version = StringField()
    source_artifact = StringField()

    # A replay is never forward evidence. The API re-asserts these values.
    evidence_kind = StringField(default="REPLAY")
    forward_sessions = IntField(default=0)
    forward_required = IntField(default=120)
    forward_eligible = BooleanField(default=False)

    data_as_of = StringField()
    generated_at = DateTimeField()
    created_at = DateTimeField()
    updated_at = DateTimeField()
    research_grade = StringField(default="RESEARCH")
    disclaimer = StringField()
    survivorship_gap = BooleanField(default=True)
    limitations = ListField(StringField())

    metrics = DictField()
    curve = ListField(DictField())
    decisions = ListField(DictField())
    fills = ListField(DictField())
    blocked_orders = ListField(DictField())

    meta = {
        "collection": "strategy_replays",
        "indexes": ["-generated_at"],
    }
