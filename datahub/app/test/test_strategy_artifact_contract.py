# -*- coding: utf-8 -*-
"""Pinned strategy artifact contract tests, independent of publication time."""

import copy
import datetime as dt
import hashlib
import json

import pytest

from caifubao_contracts.strategy_artifact import (
    build_strategy_artifact,
    injected_code_version,
    verify_strategy_artifact,
)


def _contract(monkeypatch, *, grade="RESEARCH", points=None, generated_at=None):
    sha = "a" * 40
    monkeypatch.setenv("CAIFUBAO_BUILD_SHA", sha)
    return build_strategy_artifact(
        producer="research/strategy-replay",
        schema_version="strategy-replay-v2",
        as_of="2026-10-23T15:00:00+08:00",
        generated_at=generated_at or "2026-10-24T00:00:00Z",
        grade=grade,
        inputs={
            "strategy": {"hash": "b" * 64},
            "quote": {"version": "PIT-v1", "as_of": "2026-10-23T15:00:00+08:00"},
        },
        payload={"nav_cents": 100001 if points is None else points},
        field_manifest={
            "asserted_fields": ["nav_cents"],
            "fields": {
                "nav_cents": {
                    "kind": "payload", "derived": False,
                    "code_version": sha,
                    "inputs": ["strategy", "quote"],
                    "unit": "cny_cent",
                }
            },
        },
    )


def test_artifact_identity_is_closed_and_deterministic(monkeypatch):
    first = _contract(monkeypatch)
    later = _contract(
        monkeypatch,
        generated_at=dt.datetime(2026, 10, 25, tzinfo=dt.timezone.utc),
    )
    assert first["as_of"] == "2026-10-23T07:00:00Z"
    assert first["input_snapshot"]["inputs"]["quote"]["as_of"] == first["as_of"]
    assert first["artifact_hash"] == later["artifact_hash"]
    assert first["artifact_id"] == later["artifact_id"]
    assert first["generated_at"] != later["generated_at"]
    assert first["artifact_id"].startswith("sha256:")
    hashed = {
        field: first[field]
        for field in (
            "schema_version", "producer", "as_of", "grade",
            "input_snapshot", "field_manifest", "payload"
        )
    }
    expected = hashlib.sha256(
        json.dumps(
            hashed, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    assert first["artifact_hash"] == expected
    assert verify_strategy_artifact(first)


def test_artifact_grade_payload_and_manifest_changes_are_detected(monkeypatch):
    base = _contract(monkeypatch)
    changed_grade = _contract(monkeypatch, grade="OBSERVATION")
    changed_payload = _contract(monkeypatch, points=100002)
    assert base["artifact_id"] != changed_grade["artifact_id"]
    assert base["artifact_id"] != changed_payload["artifact_id"]
    for field in ("grade", "field_manifest", "payload"):
        tampered = copy.deepcopy(base)
        if field == "grade":
            tampered[field] = "OBSERVATION"
        elif field == "field_manifest":
            tampered[field]["fields"]["nav_cents"]["unit"] = "yuan"
        else:
            tampered[field]["nav_cents"] += 1
        with pytest.raises(ValueError, match="modified"):
            verify_strategy_artifact(tampered)


def test_artifact_rejects_non_integer_values_and_unknown_provenance(monkeypatch):
    monkeypatch.delenv("CAIFUBAO_BUILD_SHA", raising=False)
    monkeypatch.delenv("GITHUB_SHA", raising=False)
    with pytest.raises(ValueError, match="build-injected"):
        injected_code_version()
    sha = "a" * 40
    monkeypatch.setenv("CAIFUBAO_BUILD_SHA", sha)
    with pytest.raises(ValueError, match="hashed floats"):
        _contract(monkeypatch, points=1.5)
    with pytest.raises(ValueError, match="decimal strings"):
        _contract(monkeypatch, points="1.50")
