# -*- coding: utf-8 -*-
"""Durable research ledger: append-only collections behind an injected database.

The adapter is tested against a minimal in-memory database so the logic is
verifiable without a cluster, and exercised against the real research database in
a scratch namespace by ``scripts/smoke_research_store.py``.
"""

import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

SHA = "a" * 64
DATASET = "snapshot-2026-10"


def _sessions(*, end="2026-09-01", count=60):
    """Business days before ``end``; enough to place the 21-session cutoff."""
    day = date.fromisoformat(end)
    out = []
    while len(out) < count:
        day -= timedelta(days=1)
        if day.weekday() < 5:
            out.append(day.isoformat())
    return sorted(out)


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "shared") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "shared"))

from caifubao_contracts.strategy_research import (  # noqa: E402
    OUTCOME_CONCLUSION,
    SCHEMA_REGISTRATION,
    verdict_hash,
)
from app.lib.strategy_engine.research_pipeline import ResearchPipeline  # noqa: E402
from app.lib.strategy_engine.research_store import (  # noqa: E402
    CONFIRMATIONS,
    REGISTRATIONS,
    TRIALS,
    VERDICTS,
    MongoResearchStore,
    ResearchStoreError,
)


class _Collection:
    def __init__(self):
        self.docs = []

    def insert_one(self, document):
        if document.get("_id") is not None and any(
            d.get("_id") == document["_id"] for d in self.docs
        ):
            raise KeyError("duplicate key")
        self.docs.append(dict(document))

    def update_one(self, query, update):
        for document in self.docs:
            if all(document.get(key) == value for key, value in query.items()):
                document.update(update.get("$set") or {})

    def find_one(self, query, projection=None):
        for document in self.docs:
            if all(document.get(key) == value for key, value in query.items()):
                found = dict(document)
                if projection:
                    found = {key: found.get(key) for key in projection if key in found}
                return found
        return None

    def find(self, query):
        return [
            dict(document)
            for document in self.docs
            if all(document.get(key) == value for key, value in query.items())
        ]

    def count_documents(self, query):
        return sum(
            1
            for document in self.docs
            if all(document.get(key) == value for key, value in query.items())
        )


class _FakeDb:
    def __init__(self):
        self.collections = {}

    def __getitem__(self, name):
        return self.collections.setdefault(name, _Collection())


def _registration(**overrides):
    base = {
        "schema": SCHEMA_REGISTRATION,
        "registration_id": "reg-store-1",
        "hypothesis": "the adapter persists what the pipeline decides",
        "family": "store-family",
        "label_horizon": 20,
        "embargo_sessions": 0,
        "confirmation_start": "2026-09-01",
        "train_cutoff": "2026-08-03",
        "max_trials": 3,
        "max_confirmations": 1,
        "success_criteria": {"min_excess_per_period": 0.0},
        "failure_criteria": {"max_drawdown": 0.3},
    }
    base.update(overrides)
    return base


@pytest.fixture()
def db():
    return _FakeDb()


@pytest.fixture()
def store(db):
    return MongoResearchStore(db)


# --------------------------------------------------------------------------
# registrations
# --------------------------------------------------------------------------


def test_registration_round_trips(store):
    store.add_registration(_registration())
    stored = store.get_registration("reg-store-1")
    assert stored["hypothesis"].startswith("the adapter persists")
    assert "_id" not in stored


def test_repeating_an_identical_registration_is_idempotent(store, db):
    store.add_registration(_registration())
    store.add_registration(_registration())
    assert db[REGISTRATIONS].count_documents({}) == 1


def test_changing_criteria_after_registration_is_refused(store):
    store.add_registration(_registration())
    with pytest.raises(ResearchStoreError, match="frozen"):
        store.add_registration(
            _registration(success_criteria={"min_excess_per_period": 0.01})
        )


def test_unknown_registration_returns_none(store):
    assert store.get_registration("nope") is None


# --------------------------------------------------------------------------
# trials and confirmations
# --------------------------------------------------------------------------


def test_trials_count_per_family_across_registrations(store):
    first = _registration()
    store.add_registration(first)
    assert store.add_trial(first) == 1
    assert store.add_trial(first) == 2

    second = _registration(registration_id="reg-store-2")
    store.add_registration(second)
    assert store.add_trial(second) == 3
    assert store.family_trials("store-family") == 3
    assert store.registration_trials("reg-store-2") == 1


def test_confirmations_count_per_family(store):
    registration = _registration()
    store.add_registration(registration)
    record = {
        "registration_id": "reg-store-1",
        "family": "store-family",
        "spec_hash": SHA,
        "dataset_id": DATASET,
        "p_value": 0.01,
        "threshold": 0.05,
        "passed": True,
    }
    assert store.add_confirmation(registration, record) == 1
    assert store.family_confirmations("store-family") == 1
    # The evidence is durable, not just the counter.
    stored = store.confirmations_for("reg-store-1")
    assert len(stored) == 1
    assert stored[0]["spec_hash"] == SHA
    assert stored[0]["passed"] is True
    assert "_id" not in stored[0]
    with pytest.raises(ValueError, match="contradicts"):
        store.add_confirmation(registration, dict(record, p_value=0.9, passed=True))


# --------------------------------------------------------------------------
# verdicts
# --------------------------------------------------------------------------


def _verdict():
    return {
        "schema": "research-verdict-v1",
        "registration_id": "reg-store-1",
        "family": "store-family",
        "stage": "confirmation",
        "outcome": OUTCOME_CONCLUSION,
        "spec_hash": "a" * 64,
        "dataset_id": "snapshot-1",
        "code_version": "b" * 40,
        "family_trials": 3,
        "registration_trials": 3,
        "confirmations": 1,
        "cutoff": {
            "train_cutoff": "2026-08-03",
            "label_horizon": 20,
            "embargo_sessions": 0,
        },
        "created_at": "2026-10-29T00:00:00+00:00",
        "detail": {"note": "fixture"},
    }


def test_verdict_is_addressed_by_content(store, db):
    digest = store.add_verdict(_verdict())
    assert digest == verdict_hash(_verdict())
    assert db[VERDICTS].count_documents({}) == 1
    assert store.get_verdict(digest)["detail"]["note"] == "fixture"


def test_writing_the_same_verdict_twice_is_idempotent(store, db):
    first = store.add_verdict(_verdict())
    second = store.add_verdict(_verdict())
    assert first == second
    assert db[VERDICTS].count_documents({}) == 1


def test_colliding_address_with_different_content_is_refused(store):
    digest = store.add_verdict(_verdict())
    tampered = dict(_verdict(), detail={"note": "tampered"})
    collision = dict(tampered, created_at=_verdict()["created_at"])
    # Force the same address by writing the tampered document directly.
    store.db[VERDICTS].docs[0]["detail"] = collision["detail"]
    with pytest.raises(ResearchStoreError, match="collision"):
        store.add_verdict(_verdict())
    assert digest is not None


# --------------------------------------------------------------------------
# the pipeline on the durable store
# --------------------------------------------------------------------------


def test_the_pipeline_works_against_the_durable_store(db):
    pipeline = ResearchPipeline(store=MongoResearchStore(db))
    pipeline.register(_registration(), trading_days=_sessions())
    pipeline.record_exploration_trial("reg-store-1", "cfg-1")
    pipeline.record_exploration_trial("reg-store-1", "cfg-2")
    gate = pipeline.confirmation_gate("reg-store-1")
    assert gate["family_trials"] == 2
    assert gate["adjusted_alpha"] == pytest.approx(0.05 / 2)

    pipeline.record_confirmation(
        "reg-store-1", spec_hash=SHA, dataset_id=DATASET, p_value=0.01
    )
    verdict = pipeline.verdict(
        "reg-store-1",
        stage="confirmation",
        outcome=OUTCOME_CONCLUSION,
        spec_hash=SHA,
        dataset_id=DATASET,
        code_version="b" * 40,
    )
    assert verdict["family_trials"] == 2
    assert verdict["confirmations"] == 1
    assert db[VERDICTS].count_documents({}) == 1
    assert db[TRIALS].count_documents({}) == 2
    assert db[CONFIRMATIONS].count_documents({}) == 1


def test_the_durable_store_survives_a_new_pipeline_instance(db):
    """Evidence is only durable if a fresh reader sees the same counts."""
    first = ResearchPipeline(store=MongoResearchStore(db))
    first.register(_registration(), trading_days=_sessions())
    first.record_exploration_trial("reg-store-1", "cfg-1")

    second = ResearchPipeline(store=MongoResearchStore(db))
    gate = second.confirmation_gate("reg-store-1")
    assert gate["family_trials"] == 1
