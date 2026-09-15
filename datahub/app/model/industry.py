# -*- coding: utf-8 -*-
"""Industry classification and daily aggregated metrics models."""

import datetime
from collections.abc import Mapping

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


def as_calendar_date(value):
    """A date for comparison, or None when the value is not usable.

    Compared as calendar dates so mixed naive/aware timestamps cannot raise or
    shift the comparison. ``industry_change_log`` entries store their timestamp
    as an ISO string (the writer uses ``now.isoformat()``), so strings are
    parsed; an unparseable value is unusable rather than assumed.
    """
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, str):
        try:
            return datetime.datetime.fromisoformat(value).date()
        except ValueError:
            return None
    return None


def _field(row, name):
    """Read one classification field from a document or a plain mapping."""
    if isinstance(row, Mapping):
        return row.get(name)
    return getattr(row, name, None)


def classification_in_effect_on(row, as_of_date) -> bool:
    """Whether the row's CURRENT classification is provably in effect on a date.

    ``assigned_at`` is written only when the row is created; the sync records
    later changes in ``industry_change_log`` instead of moving ``assigned_at``.
    So the current code can be attributed to the signal date only when the row
    existed on or before it AND no recorded change happened after it. An entry
    dated after the signal date — or an entry whose date cannot be read — means
    the current code may post-date the signal, so the row is not usable.

    Accepts a mongoengine document or a plain mapping (the H20 snapshot runner
    projects industry rows to dicts), so this stays the single point-in-time
    rule. Every consumer that attributes an industry to a past date (paper
    strategy, replayed scoring, industry metric aggregation, the H20 export)
    must use it: the store holds one current row per stock, so an unguarded
    read is look-ahead.
    """
    target = as_calendar_date(as_of_date)
    assigned = as_calendar_date(_field(row, "assigned_at"))
    if target is None or assigned is None or assigned > target:
        return False
    for entry in _field(row, "industry_change_log") or []:
        changed = as_calendar_date(
            entry.get("timestamp") if isinstance(entry, dict) else None
        )
        if changed is None or changed > target:
            return False
    return True
