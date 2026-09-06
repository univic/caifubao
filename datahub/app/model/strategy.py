# -*- coding: utf-8 -*-
"""Strategy-layer persistence for the paper-first strategy runner.

One StrategyPaperRun document per (date, model_version, horizon, config_hash):
the daily target portfolio, the rebalance diff vs the previous run, and the
paper NAV/equity-curve snapshot. Paper-only: nothing here places real orders
or mutates an account. The strategy engine (app.lib.strategy_engine) is pure;
this document is written by jobs/strategy_runner.
"""

import datetime

from mongoengine import (
    DateTimeField,
    DictField,
    Document,
    IntField,
    ListField,
    StringField,
)


class StrategyPaperRun(Document):
    strategy_name = StringField(required=True, default="flip_wide_paper")
    date = DateTimeField(required=True)
    model_version = StringField(required=True)
    horizon = IntField(required=True, choices=[5, 20, 60])
    config_hash = StringField(required=True)  # hash of the VALIDATED config
    config = DictField()

    status = StringField(
        choices=["RUNNING", "COMPLETED", "SKIPPED", "FAILED"], default="RUNNING"
    )
    skip_reason = StringField()

    target_holdings = ListField(DictField())  # [{stock_code, weight}]
    rebalance = DictField()  # {added, removed, unchanged}
    # Single paper-NAV curve point for THIS run's date, written by the `nav`
    # recompute command: {date, nav, daily_return, turnover, drawdown,
    # benchmark_return?, positions_count} (aggregate initial/terminal NAV lives
    # in the nav command's return, not here).
    nav_snapshot = DictField()

    created_at = DateTimeField(default=lambda: datetime.datetime.now(datetime.UTC))
    updated_at = DateTimeField()
    completed_at = DateTimeField()
    error_msg = StringField()

    meta = {
        "collection": "strategy_paper_runs",
        "indexes": [
            {
                "fields": [
                    "strategy_name",
                    "date",
                    "model_version",
                    "horizon",
                    "config_hash",
                ],
                "unique": True,
            },
            ("model_version", "-date"),
            "-date",
        ],
    }

    def save(self, *args, **kwargs):
        self.updated_at = datetime.datetime.now(datetime.UTC)
        return super(StrategyPaperRun, self).save(*args, **kwargs)
