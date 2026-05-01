import datetime

from mongoengine import (
    BooleanField,
    DateTimeField,
    DictField,
    Document,
    IntField,
    StringField,
)


STATUS_RUNNING = "RUNNING"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_SKIPPED = "SKIPPED"


class DatahubJobRun(Document):
    meta = {
        "collection": "datahub_job_runs",
        "indexes": [
            ("job_family", "scheduled_at"),
            ("job_name", "-started_at"),
            ("status", "-started_at"),
            ("job_family", "status"),
        ],
    }

    job_family = StringField(required=True)
    job_name = StringField(required=True)
    trigger = StringField(required=True)
    source = StringField(required=True)
    target = StringField()
    include_factors = BooleanField(default=False)
    scheduled_at = DateTimeField()
    started_at = DateTimeField(default=datetime.datetime.utcnow, required=True)
    completed_at = DateTimeField()
    status = StringField(required=True)
    failed_phase = StringField()
    pulled_total = IntField(default=0)
    written_total = IntField(default=0)
    phase_stats = DictField(default=dict)
    error_message = StringField()
    summary = DictField(default=dict)
    extra = DictField(default=dict)
