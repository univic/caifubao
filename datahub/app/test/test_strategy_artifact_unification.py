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
        "nav_parity": {
            "tolerance_cny": 0.01,
            "max_abs_delta_cny": 0.0,
            "points": [{
                "date": "2026-10-26",
                "legacy_nav": 1000,
                "ledger_nav": 1000,
                "delta": 0,
            }],
        },
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
    assert artifacts[identity].payload["nav_parity"] == data["nav_parity"]
    assert vars(plan) == original_plan

    # An existing content hash cannot be silently overwritten.
    artifacts[identity].payload = {"different": True}
    with pytest.raises(ValueError, match="immutable research artifact"):
        _persist_research_simulation(**kwargs)


def test_report_exposes_only_matching_research_blocked_events(monkeypatch):
    from app.jobs.strategy_runner import research_simulation_report
    from app.model import strategy as strategy_model

    execution = datetime.datetime(2026, 10, 26)
    run = SimpleNamespace(
        strategy_name="flip_wide_paper",
        config_hash="spec-a",
        hash_kind="spec",
        execution_date=execution,
    )
    events = [
        {
            "type": "REJECTED",
            "execution_date": "2026-10-26",
            "stock_code": "sh600000",
            "side": "SELL",
            "reason_code": "at_limit_down",
            "order_id": "o-1",
        },
        {
            "type": "REJECTED",
            "execution_date": "2026-10-27",
            "stock_code": "sh600001",
            "reason_code": "suspended",
        },
        {
            "type": "FILL",
            "execution_date": "2026-10-26",
            "stock_code": "sh600002",
        },
    ]
    document = SimpleNamespace(artifact_hash="sha", payload={"events": events})

    class FakeQuery:
        def order_by(self, *args):
            return self

        def first(self):
            return document

    class FakeArtifact:
        @classmethod
        def objects(cls, **kwargs):
            assert kwargs["config_hash"] == "spec-a"
            assert kwargs["hash_kind"] == "spec"
            assert kwargs["execution_from__lte"] == execution
            return FakeQuery()

    monkeypatch.setattr(strategy_model, "StrategySimulationArtifact", FakeArtifact)
    summary = research_simulation_report(run)
    assert summary["research_only"] is True
    assert summary["blocked_count"] == 1
    assert summary["blocked"][0]["reason_code"] == "at_limit_down"
    assert summary["fills_count"] == 1
