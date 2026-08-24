import datetime

from mongoengine import (
    DateTimeField,
    DictField,
    Document,
    FloatField,
    IntField,
    StringField,
)


STATUS_OK = "OK"
STATUS_STALE = "STALE"
STATUS_AHEAD = "AHEAD"
STATUS_NO_DATA = "NO_DATA"
STATUS_NOT_APPLICABLE = "NOT_APPLICABLE"


class DataAssetStatus(Document):
    """
    Current per-code status for a derived data asset.

    This model is the target replacement for data_freshness_meta. During the
    migration window both collections can coexist, but new quality/read paths
    should prefer this wider status document.
    """

    meta = {
        "collection": "data_asset_status",
        "indexes": [
            {
                "fields": ["code", "object_type", "asset_type", "asset_name"],
                "unique": True,
            },
            ("object_type", "asset_type", "asset_name", "status", "code"),
            ("asset_type", "asset_name", "-latest_data_date"),
            ("code", "asset_type"),
            "status",
        ],
    }

    code = StringField(required=True)
    object_type = StringField(required=True)
    asset_type = StringField(required=True)
    asset_name = StringField(required=True)

    first_data_date = DateTimeField()
    latest_data_date = DateTimeField()
    data_count = IntField(default=0)
    expected_count = IntField()
    coverage_rate = FloatField()

    status = StringField(required=True)
    status_reason = StringField()

    last_calculated_at = DateTimeField(default=datetime.datetime.now)
    last_success_at = DateTimeField()
    last_job_name = StringField()
    last_job_id = StringField()
    error_message = StringField()
    extra = DictField()
