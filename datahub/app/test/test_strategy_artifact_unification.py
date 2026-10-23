# -*- coding: utf-8 -*-
"""Research ledger evidence remains immutable and separate from forward plans."""

import copy
import datetime
from types import SimpleNamespace

import pytest

from app.jobs.strategy_runner import _persist_research_simulation


def test_research_artifact_is_content_addressed_and_never_mutates_plan(monkeypatch):
    from app.model import strategy as strategy_model

    artifacts = {}

    class Query:
        def __init__(self, artifact_hash):
            self.artifact_hash = artifact_hash

        def first(self):
            return artifacts.get(self.artifact_hash)

    class FakeArtifact:
        @classmethod
        def objects(cls, **query):
            return Query(query["artifact_hash"])

        def __init__(self, **fields):
            self.__dict__.update(fields)

        def save(self):
            artifacts[self.artifact_hash] = self

    monkeypatch.setattr(strategy_model, "StrategySimulationArtifact", FakeArtifact)
    plan = SimpleNamespace(
        date=datetime.datetime(2026, 10, 23),
        evidence_kind="FORWARD",
        config_hash="legacy-hash",
        target_holdings=[{"stock_code": "sh600000", "weight": 1.0}],
    )
    original_plan = copy.deepcopy(vars(plan))
    data = {
        "ledger_events": [
            {"type": "REJECTED", "event_id": "blocked:1", "reason_code": "suspended"}
        ],
        "ledger_snapshot": {"cash": 1000, "positions": {}, "nav": 1000},
        "curve": [{"date": "2026-10-26", "nav": 1000}],
    }
    kwargs = {
        "runs": [plan],
        "config_hash": "spec-hash",
        "hash_kind": "spec",
        "schedule": [{"date": "2026-10-26", "holdings": {}}],
        "result": data,
        "benchmark": {},
        "execution_from": datetime.datetime(2026, 10, 26),
        "execution_to": datetime.datetime(2026, 10, 26),
    }
    identity = _persist_research_simulation(**kwargs)
    assert len(identity) == 64
    assert len(artifacts) == 1
    assert _persist_research_simulation(**kwargs) == identity
    assert len(artifacts) == 1
    assert artifacts[identity].payload["events"] == data["ledger_events"]
    assert vars(plan) == original_plan

    # An existing content hash cannot be silently overwritten.
    artifacts[identity].payload = {"different": True}
    with pytest.raises(ValueError, match="immutable research artifact"):
        _persist_research_simulation(**kwargs)
