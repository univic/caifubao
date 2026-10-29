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
    BooleanField,
    DateTimeField,
    DictField,
    Document,
    FloatField,
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
    # Per-code holding state for the exit hysteresis: {code: {hold_count,
    # entry_reason}}. Absent on legacy rows, which simply start a fresh count.
    hold_meta = DictField()
    # Per-code exit reason for this run's rebalance ({code: reason}); kept out
    # of `rebalance` so that field's shape stays exactly added/removed/unchanged.
    exit_reasons = DictField()
    #: Price recorded for each target name on the decision date. A v2 declaration
    #: needs it: the per-asset stop is measured against a cost basis, and a basis
    #: invented on the fly would make the stop unmeasurable.
    reference_prices = DictField()
    #: The account state the plan was actually decided from. Persisted so a
    #: stop-out or a reserved slot can be explained after the fact instead of
    #: being re-derived from a book that has since moved on.
    decision_state = DictField()
    #: Content hash of that state, so two runs of one declaration can differ by
    #: account state and have that difference be locatable.
    decision_state_hash = StringField()
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


class AllocationPlan(Document):
    """One research plan for an allocation sleeve.

    Target weights are not holdings: the record says what the declaration would
    hold if executed, and it carries the accounting and benchmark identity the
    sleeve is measured under. ``decision_at`` must precede the execution session
    for the plan to count as forward evidence.
    """

    strategy_id = StringField(required=True)
    spec_hash = StringField(required=True)
    date = DateTimeField(required=True)  # decision session
    execution_date = DateTimeField()
    decision_at = DateTimeField()
    targets = DictField(required=True)  # code -> weight
    actions = DictField(required=True)  # code -> hold|rebalance|open|close|stop_out
    cash_weight = FloatField()
    reference_prices = DictField()  # code -> cost basis used for the stop
    #: Close used on the decision date, so the next session can drift the book
    #: by real returns instead of re-reading yesterday's target weights.
    marks = DictField()
    #: Last session on which the band actually rebalanced; the cadence is
    #: measured from here, not from yesterday's plan date.
    last_rebalance_date = DateTimeField()
    rebalance_session = BooleanField()
    status = StringField(choices=["PLANNED", "SKIPPED"], default="PLANNED")
    reason = StringField()

    created_at = DateTimeField(default=lambda: datetime.datetime.now(datetime.UTC))

    meta = {
        "collection": "allocation_plans",
        "indexes": [
            {"fields": ["strategy_id", "date"], "unique": True},
            ("strategy_id", "-date"),
        ],
    }


class AllocationForwardWindow(Document):
    """Append-only forward-evidence window for an allocation strategy.

    An allocation strategy builds target weights from a declared asset set and
    band; it has no scoring model or horizon, so keying it by
    ``(model_version, horizon)`` would need a placeholder version or make it
    unrepresentable. It therefore lives in its own collection, keyed by
    ``strategy_id``, and never appears in scoring-window progress.

    Certification closes only a predecessor with the same ``strategy_id``;
    ``start_date`` is the certification session and is never backdated. The
    120-session clock is the only irreversible resource here, so a window is
    refused when no plan path can fill it.
    """

    strategy_id = StringField(required=True)
    spec_hash = StringField(required=True)
    hash_kind = StringField(choices=["spec"], default="spec")
    accounting_policy = DictField(required=True)
    benchmark = DictField(required=True)
    start_date = DateTimeField(required=True)  # first decision date allowed FORWARD
    status = StringField(choices=["ACTIVE", "CLOSED"], default="ACTIVE")
    decision_at = DateTimeField()
    closed_at = DateTimeField()

    created_at = DateTimeField(default=lambda: datetime.datetime.now(datetime.UTC))

    meta = {
        "collection": "allocation_forward_windows",
        "indexes": [
            # At most one ACTIVE window per strategy: a spec change closes the
            # predecessor rather than letting two clocks run at once.
            {
                "fields": ["strategy_id"],
                "unique": True,
                "partialFilterExpression": {"status": "ACTIVE"},
            },
            ("strategy_id", "-start_date"),
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
