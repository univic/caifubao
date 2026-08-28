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

# Startup catch-up is the only path that can race across overlapping pod
# rollouts (the deployment restarts while the previous pod's catch-up thread
# is still deciding), so its RUNNING claims are uniqueness-constrained. The
# partial filter keeps every other runner (manual re-runs, retries) free to
# create as many records as before.
JOB_NAME_STARTUP_CATCHUP = "datahub_quote_startup_catchup"


class DatahubJobRun(Document):
    meta = {
        "collection": "datahub_job_runs",
        "indexes": [
            ("job_family", "scheduled_at"),
            ("job_name", "-started_at"),
            ("status", "-started_at"),
            ("job_family", "status"),
            ("job_family", "status", "-started_at"),
            {
                "fields": ["job_family", "scheduled_at"],
                "unique": True,
                "partialFilterExpression": {
                    "status": STATUS_RUNNING,
                    "job_name": JOB_NAME_STARTUP_CATCHUP,
                },
            },
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
