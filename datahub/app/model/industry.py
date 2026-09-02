# -*- coding: utf-8 -*-
"""Industry classification and daily aggregated metrics models."""

import datetime

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
    Industry classification for each stock.

    Updated monthly via the data pipeline from baostock's CSRC (证监会)
    industry API. The industry_code_sw_l1/industry_name_sw_l1 fields store the
    CSRC L1 code and name (e.g. ``J66`` / ``货币金融服务``); the field names
    retain the legacy ``sw`` suffix even though the data is CSRC, not Shenwan.
    Change history is preserved in industry_change_log.
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
            ("industry_code_sw_l1", "-last_synced_at"),
        ],
    }

    def save(self, *args, **kwargs):
        self.last_synced_at = datetime.datetime.now(datetime.UTC)
        return super(StockIndustryClassification, self).save(*args, **kwargs)


class IndustryDailyMetrics(Document):
    """
    Daily aggregated scoring metrics per Shenwan industry per horizon.

    One document per (industry, date, horizon, model_version). Generated
    after each scoring run so that the industry_momentum component can
    reference the average score of stocks within the same industry.
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

    def save(self, *args, **kwargs):
        self.generated_at = datetime.datetime.now(datetime.UTC)
        return super(IndustryDailyMetrics, self).save(*args, **kwargs)
