# -*- coding: utf-8 -*-
"""Replay-runner persistence: immutable by replay_id + input hash."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.jobs import strategy_replay_runner as runner


def _artifact() -> dict:
    return {
        "replay_id": "balanced_50k_20200101_20201231_v1",
        "schema_version": "strategy-replay-v1",
        "source_artifact": "test",
        "name": "test",
        "strategy": "core_satellite",
        "description": "",
        "start_date": "2020-01-01",
        "end_date": "2020-12-31",
        "data_as_of": "2020-12-31",
        "aum": 50_000.0,
        "config": {"core": "40/30/30"},
        "config_hash": "cfg",
        "input_hash": "input-a",
        "content_hash": "content-a",
        "evidence_kind": "REPLAY",
        "research_grade": "RESEARCH",
        "forward_sessions": 0,
        "forward_required": 120,
        "forward_eligible": False,
        "survivorship_gap": True,
        "limitations": ["test"],
        "disclaimer": "test",
        "metrics": {},
        "curve": [],
        "decisions": [],
        "fills": [],
        "blocked_orders": [],
        "generated_at": "2020-12-31T00:00:00+00:00",
    }


class _Query:
    def __init__(self, collection):
        self.collection = collection

    def first(self):
        return self.collection.document


class _Collection:
    def __init__(self):
        self.document = None
        self.saved = []

    def __call__(self, **kwargs):
        return _Query(self)


class _Document:
    def __init__(self, artifact, collection=None):
        self.artifact = artifact
        self.input_hash = artifact["input_hash"]
        self.content_hash = artifact["content_hash"]
        self.saved = False
        self.collection = collection

    def save(self):
        self.saved = True
        if self.collection is not None:
            self.collection.saved.append(self)


def test_creates_a_new_artifact(monkeypatch):
    collection = _Collection()
    monkeypatch.setattr(
        runner,
        "_model_from_artifact",
        lambda artifact: _Document(artifact, collection=collection),
    )
    result = runner.persist_artifact(_artifact(), collection=collection)
    assert result["status"] == "CREATED"
    assert len(collection.saved) == 1
    assert collection.saved[0].input_hash == "input-a"
    assert collection.saved[0].content_hash == "content-a"


def test_same_input_and_content_is_unchanged(monkeypatch):
    collection = _Collection()
    collection.document = SimpleNamespace(
        input_hash="input-a", content_hash="content-a"
    )
    monkeypatch.setattr(
        runner, "_model_from_artifact", lambda artifact: _Document(artifact)
    )
    result = runner.persist_artifact(_artifact(), collection=collection)
    assert result["status"] == "UNCHANGED"


def test_changed_input_under_same_id_fails(monkeypatch):
    collection = _Collection()
    collection.document = SimpleNamespace(
        input_hash="input-b", content_hash="content-b"
    )
    monkeypatch.setattr(
        runner, "_model_from_artifact", lambda artifact: _Document(artifact)
    )
    with pytest.raises(ValueError, match="different input hash"):
        runner.persist_artifact(_artifact(), collection=collection)


def test_changed_content_under_same_input_fails(monkeypatch):
    collection = _Collection()
    collection.document = SimpleNamespace(
        input_hash="input-a", content_hash="content-b"
    )
    monkeypatch.setattr(
        runner, "_model_from_artifact", lambda artifact: _Document(artifact)
    )
    with pytest.raises(ValueError, match="different content"):
        runner.persist_artifact(_artifact(), collection=collection)
