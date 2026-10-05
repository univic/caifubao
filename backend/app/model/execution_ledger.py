# -*- coding: utf-8 -*-
"""Manual execution bridge models for Portfolio-backed account ledgers."""

import datetime

from app.lib.db_watcher.mongoengine_tool import db
from app.model.portfolio import Portfolio


def _utcnow():
    return datetime.datetime.now(datetime.UTC)


class OrderIntent(db.Document):
    """Immutable trade intent created before a human-confirmed execution."""

    portfolio = db.ReferenceField(Portfolio, required=True)
    stock_code = db.StringField(required=True)
    stock_name = db.StringField()
    side = db.StringField(required=True, choices=["BUY", "SELL"])
    target_quantity = db.FloatField(required=True, min_value=0.000001)
    target_price = db.FloatField(min_value=0.0)
    filled_quantity = db.FloatField(default=0.0, min_value=0.0)
    status = db.StringField(
        required=True,
        choices=["OPEN", "PARTIAL", "FILLED", "CANCELLED"],
        default="OPEN",
    )
    source_type = db.StringField(default="MANUAL")
    source_ref = db.StringField()
    notes = db.StringField()
    created_at = db.DateTimeField(default=_utcnow)
    updated_at = db.DateTimeField()

    meta = {
        "collection": "order_intents",
        "indexes": [
            ("portfolio", "-created_at"),
            ("portfolio", "status", "-created_at"),
            "source_ref",
        ],
    }

    def save(self, *args, **kwargs):
        self.updated_at = _utcnow()
        return super().save(*args, **kwargs)


class ExecutionFill(db.Document):
    """Operator-imported fill, idempotent within a Portfolio."""

    portfolio = db.ReferenceField(Portfolio, required=True)
    intent = db.ReferenceField(OrderIntent)
    external_fill_id = db.StringField(required=True)
    stock_code = db.StringField(required=True)
    stock_name = db.StringField()
    side = db.StringField(required=True, choices=["BUY", "SELL"])
    quantity = db.FloatField(required=True, min_value=0.000001)
    price = db.FloatField(required=True, min_value=0.000001)
    fee = db.FloatField(default=0.0, min_value=0.0)
    trade_time = db.DateTimeField(required=True)
    import_source = db.StringField(default="JSON", choices=["JSON", "CSV"])
    apply_status = db.StringField(
        required=True, choices=["PENDING", "APPLIED"], default="PENDING"
    )
    portfolio_transaction_id = db.StringField()
    created_at = db.DateTimeField(default=_utcnow)

    meta = {
        "collection": "execution_fills",
        "indexes": [
            {
                "fields": ["portfolio", "external_fill_id"],
                "unique": True,
            },
            ("portfolio", "-trade_time"),
            ("intent", "trade_time"),
        ],
    }


class AccountReconciliation(db.Document):
    """Persisted comparison between a Portfolio ledger and supplied account state."""

    portfolio = db.ReferenceField(Portfolio, required=True)
    as_of = db.DateTimeField(required=True)
    status = db.StringField(required=True, choices=["PASS", "BREAK"])
    expected_cash = db.FloatField(required=True)
    actual_cash = db.FloatField(required=True)
    cash_drift = db.FloatField(required=True)
    cash_tolerance = db.FloatField(default=0.01, min_value=0.0)
    quantity_tolerance = db.FloatField(default=0.000001, min_value=0.0)
    expected_positions = db.ListField(db.DictField(), default=list)
    actual_positions = db.ListField(db.DictField(), default=list)
    breaks = db.ListField(db.DictField(), default=list)
    created_at = db.DateTimeField(default=_utcnow)

    meta = {
        "collection": "account_reconciliations",
        "indexes": [
            ("portfolio", "-as_of"),
            ("portfolio", "status", "-as_of"),
        ],
    }
