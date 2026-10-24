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
    # Missing provenance on legacy records is retrospective, never forward.
    decision_at = DateTimeField()
    execution_date = DateTimeField()
    # FORWARD only when certified by the forward-evidence rules (NEXT.1);
    # legacy/missing provenance and every retrospective artifact stay REPLAY.
    evidence_kind = StringField(choices=["REPLAY", "FORWARD"], default="REPLAY")
    model_version = StringField(required=True)
    horizon = IntField(required=True, choices=[5, 20, 60])
    # config_hash remains the authoritative identity for legacy readers.
    # New spec-backed records pin spec_hash there; the hash_kind disambiguates
    # it from legacy config hashes, so old forward windows remain independent.
    config_hash = StringField(required=True)
    hash_kind = StringField(choices=["legacy", "spec"], default="legacy")
    spec_hash = StringField()
    legacy_config_hash = StringField()
    config = DictField()

    status = StringField(
        choices=["RUNNING", "COMPLETED", "SKIPPED", "FAILED"], default="RUNNING"
    )
    skip_reason = StringField()

    target_holdings = ListField(DictField())  # [{stock_code, weight}]
    rebalance = DictField()  # {added, removed, unchanged}
    # Single paper-NAV curve point for THIS run's execution_date, written by the `nav`
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


class StrategyForwardWindow(Document):
    """Append-only certified forward-evidence window (roadmap NEXT.1).

    One ACTIVE window per (model_version, horizon); config_hash is
    certification content — a record is FORWARD-eligible only when its
    config_hash matches the ACTIVE window's. Certifying a new window closes
    any ACTIVE predecessor on the pair (score/config changes mean evidence
    cannot span configurations). start_date is the certification session and
    is never backdated.
    """

    model_version = StringField(required=True)
    horizon = IntField(required=True, choices=[5, 20, 60])
    config_hash = StringField(required=True)
    hash_kind = StringField(choices=["legacy", "spec"], default="legacy")
    spec_hash = StringField()
    legacy_config_hash = StringField()
    start_date = DateTimeField(required=True)  # first signal date allowed FORWARD
    status = StringField(choices=["ACTIVE", "CLOSED"], default="ACTIVE")
    decision_at = DateTimeField()  # when the window was opened
    closed_at = DateTimeField()

    created_at = DateTimeField(default=lambda: datetime.datetime.now(datetime.UTC))

    meta = {
        "collection": "strategy_forward_windows",
        "indexes": [
            # At most one ACTIVE window per (model_version, horizon): config
            # changes must close the predecessor, never coexist.
            {
                "fields": ["model_version", "horizon"],
                "unique": True,
                "partialFilterExpression": {"status": "ACTIVE"},
            },
            ("model_version", "-start_date"),
        ],
    }


class StrategySimulationArtifact(Document):
    """Immutable research NAV/ledger derivation, not a trading account."""

    strategy_name = StringField(required=True)
    config_hash = StringField(required=True)
    hash_kind = StringField(choices=["legacy", "spec"], required=True)
    artifact_hash = StringField(required=True, unique=True)
    execution_from = DateTimeField(required=True)
    execution_to = DateTimeField(required=True)
    payload = DictField(required=True)
    created_at = DateTimeField(default=lambda: datetime.datetime.now(datetime.UTC))

    meta = {
        "collection": "strategy_simulation_artifacts",
        "indexes": [("strategy_name", "-execution_to")],
    }
