import datetime
from mongoengine import Document, StringField, DateTimeField


class DataFreshnessMeta(Document):
    """
    Deprecated metadata collection.

    New read/write paths should use DataAssetStatus (`data_asset_status`) instead.
    This document remains only for legacy diagnostics and migration fallback.
    """

    meta = {
        "indexes": [
            {
                "fields": ["code", "object_type", "meta_type", "meta_name"],
                "unique": True,
            },
            {"fields": ["code", "meta_name"], "unique": True},
        ]
    }
    code = StringField()
    object_type = StringField()
    meta_type = StringField()
    meta_name = StringField()
    freshness_datetime = DateTimeField()
    calculated_at = DateTimeField(default=datetime.datetime.now)
    backtest_name = StringField()
    status = StringField()  # OPEN/NO_UPD
