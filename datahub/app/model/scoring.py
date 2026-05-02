# -*- coding: utf-8 -*-
# Author : Gemini CLI
# Date: 2026-04-17

import datetime
from mongoengine import (
    BooleanField,
    DateTimeField,
    DictField,
    FloatField,
    IntField,
    ListField,
    ReferenceField,
    StringField,
)
from app.lib.db_watcher.mongoengine_tool import db
from app.model.stock import BasicStock


class StockDailyScore(db.Document):
    """
    Daily scoring results for stocks with closed-loop verification.
    """

    stock = ReferenceField(BasicStock)
    stock_code = StringField(required=True)
    stock_name = StringField()
    date = DateTimeField(required=True)

    # Scoring Results
    score = FloatField(default=0.0)
    recommendation = StringField(choices=["BUY", "HOLD", "NONE"], default="NONE")
    scoring_basis = DictField()  # Snapshot of signals/factors

    # Verification (Closed-loop)
    status = StringField(default="PENDING", choices=["PENDING", "VERIFIED"])
    base_price = FloatField()  # Close price on evaluation date (T)
    target_date = DateTimeField()  # T+5 trading day

    actual_price_t5 = FloatField()  # Close price on T+5
    max_price_in_5d = FloatField()  # High price within [T+1, T+5]

    profit_percentage_t5 = FloatField()
    max_profit_percentage = FloatField()
    is_effective = BooleanField()  # Prediction successful?

    generated_at = DateTimeField(default=lambda: datetime.datetime.now(datetime.UTC))
    updated_at = DateTimeField()

    meta = {
        "collection": "stock_daily_scores",
        "indexes": [
            "stock_code",
            "date",
            "status",
            "target_date",
            ("stock_code", "date"),
        ],
    }

    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.now(datetime.UTC)
        return super(StockDailyScore, self).save(*args, **kwargs)


class StockScorePrediction(db.Document):
    """
    Horizon-specific daily stock score prediction.

    One document represents one prediction for a stock on one evaluation date,
    one horizon, and one model version.
    """

    stock = ReferenceField(BasicStock)
    stock_code = StringField(required=True)
    stock_name = StringField()
    date = DateTimeField(required=True)
    horizon = IntField(required=True, choices=[5, 20, 60])

    score = FloatField(default=0.0)
    rank = IntField()
    percentile = FloatField()
    recommendation = StringField(
        choices=["BUY", "WATCH", "AVOID", "NONE"], default="NONE"
    )

    base_price = FloatField()
    target_date = DateTimeField()
    status = StringField(
        choices=[
            "PENDING",
            "TRACKING",
            "VERIFIED",
            "INSUFFICIENT_DATA",
            "BLOCKED",
            "FAILED",
        ],
        default="PENDING",
    )

    explanation = DictField()
    verification = DictField()
    input_snapshot = DictField()
    model_version = StringField(required=True, default="score_v2_202604")

    generated_at = DateTimeField(default=lambda: datetime.datetime.now(datetime.UTC))
    updated_at = DateTimeField()

    meta = {
        "collection": "stock_score_predictions",
        "indexes": [
            {
                "fields": ["stock_code", "date", "horizon", "model_version"],
                "unique": True,
            },
            ("date", "horizon", "-score"),
            ("stock_code", "-date", "horizon"),
            ("status", "target_date", "horizon"),
        ],
    }

    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.now(datetime.UTC)
        return super(StockScorePrediction, self).save(*args, **kwargs)


class ScoreExperiment(db.Document):
    """
    Research experiment for comparing scoring model versions and factor weights.
    """

    name = StringField(required=True)
    description = StringField()
    model_version = StringField(required=True)
    baseline_model_version = StringField()
    start_date = DateTimeField(required=True)
    end_date = DateTimeField(required=True)
    horizons = ListField(IntField(choices=[5, 20, 60]), default=lambda: [5, 20, 60])
    config = DictField()
    status = StringField(
        choices=["CREATED", "RUNNING", "COMPLETED", "FAILED"], default="CREATED"
    )
    report = DictField()
    error_msg = StringField()
    created_at = DateTimeField(default=lambda: datetime.datetime.now(datetime.UTC))
    updated_at = DateTimeField()
    completed_at = DateTimeField()

    meta = {
        "collection": "score_experiments",
        "indexes": [
            "-created_at",
            "model_version",
            "baseline_model_version",
            ("status", "-created_at"),
        ],
    }

    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.now(datetime.UTC)
        return super(ScoreExperiment, self).save(*args, **kwargs)
