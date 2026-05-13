# -*- coding: utf-8 -*-
"""Industry classification model for the backend API service.

Mirrors datahub/app/model/industry.py. Only the model classes needed for
querying (read-only) are included here — the sync pipeline lives in datahub.
"""

from mongoengine import (
    DateTimeField,
    Document,
    FloatField,
    IntField,
    ListField,
    StringField,
)


class StockIndustryClassification(Document):
    """
    Shenwan (申万) industry classification for each stock.

    Updated monthly via the datahub sync pipeline.
    """

    stock_code = StringField(required=True, unique=True)
    stock_name = StringField()

    # Shenwan Level 1
    industry_code_sw_l1 = StringField()
    industry_name_sw_l1 = StringField()

    # Shenwan Level 2
    industry_code_sw_l2 = StringField()
    industry_name_sw_l2 = StringField()

    # Data quality
    assigned_at = DateTimeField()
    last_synced_at = DateTimeField()
    industry_change_log = ListField(default=[])

    meta = {
        "collection": "stock_industry",
        "indexes": [
            "stock_code",
            "industry_code_sw_l1",
            "industry_code_sw_l2",
            "industry_name_sw_l1",
        ],
    }


class IndustryDailyMetrics(Document):
    """
    Daily aggregated scoring metrics per Shenwan industry per horizon.

    One document per (industry, date, horizon, model_version). Generated
    during scoring runs and queried by the industry_momentum component.
    """

    industry_code = StringField(required=True)
    industry_name = StringField()
    date = DateTimeField(required=True)
    horizon = IntField(required=True)
    model_version = StringField(required=True)

    # Aggregation
    stock_count = IntField(default=0)
    avg_score = FloatField(default=0.0)
    max_score = FloatField(default=0.0)
    min_score = FloatField(default=0.0)
    std_dev_score = FloatField(default=0.0)
    avg_percentile = FloatField(default=0.0)
    avg_rank = FloatField(default=0.0)

    # Coverage
    buy_count = IntField(default=0)
    watch_count = IntField(default=0)
    avoid_count = IntField(default=0)

    generated_at = DateTimeField()

    meta = {
        "collection": "industry_daily_metrics",
        "indexes": [
            {
                "fields": ["industry_code", "date", "horizon", "model_version"],
                "unique": True,
            },
            ("date", "industry_code", "horizon"),
            "-date",
        ],
    }
