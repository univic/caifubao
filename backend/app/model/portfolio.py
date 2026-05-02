# -*- coding: utf-8 -*-

import datetime

from app.lib.db_watcher.mongoengine_tool import db


class Portfolio(db.Document):
    name = db.StringField(required=True)
    description = db.StringField()
    base_currency = db.StringField(default="CNY")
    benchmark = db.StringField(default="sh000001")
    initial_cash = db.FloatField(default=1_000_000.0)
    cash = db.FloatField(default=1_000_000.0)
    status = db.StringField(choices=["ACTIVE", "ARCHIVED"], default="ACTIVE")
    created_at = db.DateTimeField(default=lambda: datetime.datetime.now(datetime.UTC))
    updated_at = db.DateTimeField()

    meta = {
        "collection": "portfolios",
        "indexes": ["status", "-created_at"],
    }

    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.now(datetime.UTC)
        return super().save(*args, **kwargs)


class PortfolioPosition(db.Document):
    portfolio = db.ReferenceField(Portfolio, required=True)
    stock_code = db.StringField(required=True)
    stock_name = db.StringField()
    quantity = db.FloatField(default=0.0)
    avg_cost = db.FloatField(default=0.0)
    realized_pnl = db.FloatField(default=0.0)
    updated_at = db.DateTimeField()

    meta = {
        "collection": "portfolio_positions",
        "indexes": [
            {"fields": ["portfolio", "stock_code"], "unique": True},
            "stock_code",
        ],
    }

    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.now(datetime.UTC)
        return super().save(*args, **kwargs)


class PortfolioTransaction(db.Document):
    portfolio = db.ReferenceField(Portfolio, required=True)
    stock_code = db.StringField()
    stock_name = db.StringField()
    side = db.StringField(
        required=True, choices=["BUY", "SELL", "CASH_IN", "CASH_OUT", "DIVIDEND"]
    )
    quantity = db.FloatField(default=0.0)
    price = db.FloatField(default=0.0)
    fee = db.FloatField(default=0.0)
    amount = db.FloatField(default=0.0)
    trade_date = db.DateTimeField(required=True)
    reason = db.StringField()
    source_score_id = db.StringField()
    created_at = db.DateTimeField(default=lambda: datetime.datetime.now(datetime.UTC))

    meta = {
        "collection": "portfolio_transactions",
        "indexes": [
            ("portfolio", "-trade_date"),
            "stock_code",
            "side",
        ],
    }


class PortfolioSnapshot(db.Document):
    portfolio = db.ReferenceField(Portfolio, required=True)
    date = db.DateTimeField(required=True)
    total_value = db.FloatField(default=0.0)
    cash = db.FloatField(default=0.0)
    positions_value = db.FloatField(default=0.0)
    daily_return = db.FloatField()
    drawdown = db.FloatField()
    holdings = db.ListField(db.DictField())
    created_at = db.DateTimeField(default=lambda: datetime.datetime.now(datetime.UTC))

    meta = {
        "collection": "portfolio_snapshots",
        "indexes": [
            {"fields": ["portfolio", "date"], "unique": True},
            ("portfolio", "-date"),
        ],
    }
