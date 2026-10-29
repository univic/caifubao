# -*- coding: utf-8 -*-
"""The durable adapters, exercised against an in-memory MongoDB.

Everything else in this change tests the pure contracts. These three adapters are
the only code that talks to Mongo, and until now they had no coverage at all —
which is how a manifest that was never packaged, and a plan that could never be
counted, both reached review.

mongomock stands in for the server: it enforces unique indexes and supports the
queries used here, so the tests below exercise the adapter code itself rather
than a hand-written fake pretending to be one.
"""

import datetime as dt
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "shared") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "shared"))

mongomock = pytest.importorskip("mongomock")

import mongoengine  # noqa: E402


@pytest.fixture()
def connection():
    """A fresh in-memory database per test, torn down afterwards."""
    mongoengine.disconnect_all()
    mongoengine.connect(
        "cfb_adapter_test",
        mongo_client_class=mongomock.MongoClient,
        uuidRepresentation="standard",
    )
    yield
    mongoengine.disconnect_all()


def _db():
    return mongoengine.connection.get_db()


def _registration(**overrides):
    base = {
        "schema": "research-registration-v1",
        "registration_id": "reg-adapter",
        "hypothesis": "the adapter round-trips what the pipeline decides",
        "family": "adapter-family",
        "label_horizon": 20,
        "embargo_sessions": 0,
        "confirmation_start": "2026-09-01",
        "train_cutoff": "2026-08-03",
        "max_trials": 5,
        "max_confirmations": 2,
        "success_criteria": {"min_excess_per_period": 0.0},
        "failure_criteria": {"max_drawdown": 0.3},
    }
    base.update(overrides)
    return base


def _verdict():
    return {
        "schema": "research-verdict-v1",
        "registration_id": "reg-adapter",
        "family": "adapter-family",
        "stage": "confirmation",
        "outcome": "conclusion",
        "spec_hash": "a" * 64,
        "dataset_id": "snapshot-1",
        "code_version": "b" * 40,
        "family_trials": 2,
        "registration_trials": 2,
        "confirmations": 1,
        "cutoff": {
            "train_cutoff": "2026-08-03",
            "label_horizon": 20,
            "embargo_sessions": 0,
        },
        "created_at": "2026-10-29T00:00:00+00:00",
        "detail": {"note": "adapter"},
    }


# --------------------------------------------------------------------------
# research ledger
# --------------------------------------------------------------------------


def test_research_store_round_trips_a_registration(connection):
    from app.lib.strategy_engine.research_store import MongoResearchStore

    store = MongoResearchStore(_db())
    store.add_registration(_registration())
    stored = store.get_registration("reg-adapter")
    assert stored["hypothesis"].startswith("the adapter round-trips")
    assert store.add_trial(_registration()) == 1
    assert store.family_trials("adapter-family") == 1
    assert store.add_confirmation(_registration()) == 1
    assert store.registration_confirmations("reg-adapter") == 1


def test_research_store_refuses_changed_criteria(connection):
    from app.lib.strategy_engine.research_store import (
        MongoResearchStore,
        ResearchStoreError,
    )

    store = MongoResearchStore(_db())
    store.add_registration(_registration())
    with pytest.raises(ResearchStoreError, match="frozen"):
        store.add_registration(
            _registration(success_criteria={"min_excess_per_period": 0.01})
        )


def test_research_store_verdicts_are_content_addressed(connection):
    from app.lib.strategy_engine.research_store import MongoResearchStore

    store = MongoResearchStore(_db())
    first = store.add_verdict(_verdict())
    assert store.add_verdict(_verdict()) == first
    assert store.get_verdict(first)["detail"]["note"] == "adapter"


def test_the_pipeline_survives_a_new_process(connection):
    """Durability means a second reader sees the same counts."""
    from app.lib.strategy_engine.research_pipeline import ResearchPipeline
    from app.lib.strategy_engine.research_store import MongoResearchStore

    first = ResearchPipeline(store=MongoResearchStore(_db()))
    first.register(_registration())
    first.record_exploration_trial("reg-adapter", "cfg-1")
    first.record_confirmation("reg-adapter")

    second = ResearchPipeline(store=MongoResearchStore(_db()))
    assert second.confirmation_gate("reg-adapter")["family_trials"] == 1
    verdict = second.verdict(
        "reg-adapter",
        stage="confirmation",
        outcome="conclusion",
        spec_hash="a" * 64,
        dataset_id="snapshot-1",
        code_version="b" * 40,
    )
    assert verdict["family_trials"] == 1
    assert verdict["confirmations"] == 1


def test_duplicate_registration_is_idempotent_against_the_durable_store(connection):
    from app.lib.strategy_engine.research_pipeline import ResearchPipeline
    from app.lib.strategy_engine.research_store import MongoResearchStore

    pipeline = ResearchPipeline(store=MongoResearchStore(_db()))
    first = pipeline.register(_registration())
    second = pipeline.register(_registration())
    assert first == second


# --------------------------------------------------------------------------
# plan store
# --------------------------------------------------------------------------


def _plan(**overrides):
    base = {
        "strategy_id": "sleeve-stable",
        "spec_hash": "c" * 64,
        "date": "2026-10-29",
        "targets": {"sh510300": 0.25},
        "actions": {"sh510300": "open"},
        "cash_weight": 0.75,
        "reference_prices": {"sh510300": 5.2},
        "marks": {"sh510300": 5.2},
        "last_rebalance_date": "2026-10-29",
        "rebalance_session": True,
    }
    base.update(overrides)
    return base


def test_plan_store_writes_and_reads_back(connection):
    from app.jobs.allocation_runner import MongoPlanStore

    store = MongoPlanStore()
    store.save(
        _plan(),
        decision_at=dt.datetime(2026, 10, 29, 9, 0, tzinfo=dt.timezone.utc),
        execution_date=dt.datetime(2026, 10, 30),
    )
    record = store.find("sleeve-stable", dt.date(2026, 10, 29))
    assert record["targets"] == {"sh510300": 0.25}
    assert record["marks"] == {"sh510300": 5.2}
    assert record["cash_weight"] == pytest.approx(0.75)


def test_plan_store_updates_the_same_day_instead_of_duplicating(connection):
    from app.jobs.allocation_runner import MongoPlanStore

    store = MongoPlanStore()
    at = dt.datetime(2026, 10, 29, 9, 0, tzinfo=dt.timezone.utc)
    store.save(_plan(), decision_at=at, execution_date=dt.datetime(2026, 10, 30))
    store.save(
        _plan(targets={"sh510300": 0.5}),
        decision_at=at,
        execution_date=dt.datetime(2026, 10, 30),
    )
    record = store.find("sleeve-stable", dt.date(2026, 10, 29))
    assert record["targets"] == {"sh510300": 0.5}
    assert _db()["allocation_plans"].count_documents({}) == 1


def test_plan_store_drops_a_plan_decided_after_its_execution(connection):
    from app.jobs.allocation_runner import MongoPlanStore

    store = MongoPlanStore()
    store.save(
        _plan(date="2026-10-29"),
        decision_at=dt.datetime(2026, 10, 29, 9, 0, tzinfo=dt.timezone.utc),
        execution_date=dt.datetime(2026, 10, 30),
    )
    store.save(
        _plan(date="2026-10-30"),
        # decided after that session had already opened
        decision_at=dt.datetime(2026, 11, 2, 9, 0, tzinfo=dt.timezone.utc),
        execution_date=dt.datetime(2026, 11, 2),
    )
    assert store.list_decided("sleeve-stable", "c" * 64) == ["2026-10-29"]


def test_plan_store_latest_ignores_later_sessions(connection):
    from app.jobs.allocation_runner import MongoPlanStore

    store = MongoPlanStore()
    at = dt.datetime(2026, 10, 29, 9, 0, tzinfo=dt.timezone.utc)
    for day in ("2026-10-28", "2026-10-29"):
        store.save(
            _plan(date=day),
            decision_at=at,
            execution_date=dt.datetime(2026, 10, 30),
        )
    assert store.latest("sleeve-stable", "c" * 64, dt.date(2026, 10, 29))["date"] == (
        "2026-10-28"
    )


# --------------------------------------------------------------------------
# allocation window
# --------------------------------------------------------------------------


def test_window_certification_closes_only_its_own_strategy(connection):
    from app.lib.strategy_engine.allocation_window import (
        certify_allocation_window,
        close_allocation_window,
    )
    from app.model.strategy import AllocationForwardWindow

    spec = {
        "spec_version": "2",
        "family": "asset_allocation",
        "accounting": {"corporate_actions": "price_only", "mark_price": "close_hfq"},
        "benchmark": {"kind": "equal_weight_universe"},
    }
    certify_allocation_window(
        spec=spec,
        strategy_id="sleeve-stable",
        spec_hash="1" * 64,
        has_plan_path=True,
        today=dt.date(2026, 10, 29),
    )
    certify_allocation_window(
        spec=spec,
        strategy_id="sleeve-aggressive",
        spec_hash="2" * 64,
        has_plan_path=True,
        today=dt.date(2026, 10, 29),
    )
    assert (
        AllocationForwardWindow.objects(
            strategy_id="sleeve-stable", status="ACTIVE"
        ).count()
        == 1
    )

    # Re-certifying the same strategy closes its predecessor.
    certify_allocation_window(
        spec=spec,
        strategy_id="sleeve-stable",
        spec_hash="3" * 64,
        has_plan_path=True,
        today=dt.date(2026, 10, 30),
    )
    active = AllocationForwardWindow.objects(
        strategy_id="sleeve-stable", status="ACTIVE"
    )
    assert active.count() == 1
    assert active.first().spec_hash == "3" * 64
    assert (
        AllocationForwardWindow.objects(
            strategy_id="sleeve-aggressive", status="ACTIVE"
        ).count()
        == 1
    )

    close_allocation_window(strategy_id="sleeve-aggressive")
    assert (
        AllocationForwardWindow.objects(
            strategy_id="sleeve-aggressive", status="ACTIVE"
        ).count()
        == 0
    )


def test_certification_without_a_plan_path_is_refused(connection):
    from app.lib.strategy_engine.allocation_window import (
        AllocationWindowError,
        certify_allocation_window,
    )
    from app.model.strategy import AllocationForwardWindow

    with pytest.raises(AllocationWindowError, match="120-session clock"):
        certify_allocation_window(
            spec={
                "spec_version": "2",
                "family": "asset_allocation",
                "accounting": {
                    "corporate_actions": "price_only",
                    "mark_price": "close_hfq",
                },
                "benchmark": {"kind": "equal_weight_universe"},
            },
            strategy_id="sleeve-stable",
            spec_hash="1" * 64,
            has_plan_path=False,
        )
    assert AllocationForwardWindow.objects.count() == 0
