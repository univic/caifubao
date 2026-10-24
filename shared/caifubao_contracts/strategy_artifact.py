# -*- coding: utf-8 -*-
"""Strict strategy research artifact envelope; legacy artifacts stay legacy.

This is a pure contract, not an artifact store or a trading promotion gate.
All hashed numeric values use manifest-declared integer units. A producer
without build-injected source identity MUST NOT publish a conforming envelope.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re

_DECIMAL = re.compile(r"^-?[0-9]+\.[0-9]+$")
_HASH_KEYS = (
    "as_of",
    "field_manifest",
    "grade",
    "input_snapshot",
    "payload",
    "producer",
    "schema_version",
)


def utc_instant(value: str | dt.datetime) -> str:
    if isinstance(value, str):
        try:
            value = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("invalid ISO-8601 instant") from exc
    if not isinstance(value, dt.datetime) or value.tzinfo is None:
        raise ValueError("instant requires an explicit timezone")
    result = value.astimezone(dt.timezone.utc)
    return result.isoformat(timespec="seconds").replace("+00:00", "Z")


def injected_code_version(env: dict | None = None) -> str:
    source = os.environ if env is None else env
    version = source.get("CAIFUBAO_BUILD_SHA") or source.get("GITHUB_SHA")
    if not isinstance(version, str) or not re.fullmatch(
        r"[0-9a-f]{40}|[0-9a-f]{64}", version
    ):
        raise ValueError("artifact code version requires a build-injected SHA")
    return version


def _check_numbers(value, where: str) -> None:
    if isinstance(value, (float, complex)):
        raise ValueError(f"{where}: hashed floats/complex values are forbidden")
    if isinstance(value, bool) or value is None or isinstance(value, int):
        return
    if isinstance(value, str):
        if _DECIMAL.fullmatch(value):
            raise ValueError(f"{where}: decimal strings are forbidden")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _check_numbers(item, f"{where}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{where}: JSON keys must be strings")
            _check_numbers(item, f"{where}.{key}")
        return
    raise ValueError(f"{where}: unsupported hashed value")


def build_strategy_artifact(
    *,
    producer: str,
    as_of: str | dt.datetime,
    grade: str,
    schema_version: str,
    payload: dict,
    field_manifest: dict,
    inputs: dict,
    generated_at: str | dt.datetime,
) -> dict:
    """Create and validate an artifact-contract envelope, no persistence.

    `inputs` maps named inputs to {version|hash, as_of?}; timestamps in input
    snapshots are normalized to UTC. Numeric fields MUST already be encoded
    into integers using field-manifest units; no implicit float rounding.
    """
    if not all(isinstance(x, str) and x for x in (producer, grade, schema_version)):
        raise ValueError("artifact identity fields cannot be empty")
    if grade not in ("RESEARCH", "OBSERVATION"):
        raise ValueError("unpromoted strategy artifact must remain research-grade")
    code_version = injected_code_version()
    if not isinstance(inputs, dict) or not inputs:
        raise ValueError("artifact inputs must be nonempty")
    clean_inputs = {}
    for name, record in inputs.items():
        if not isinstance(record, dict) or not (
            record.get("version") or record.get("hash")
        ):
            raise ValueError(f"unresolvable input {name}")
        entry = dict(record)
        if "as_of" in entry:
            entry["as_of"] = utc_instant(entry["as_of"])
        clean_inputs[name] = entry
    snapshot = {"code_version": code_version, "inputs": clean_inputs}
    if not isinstance(payload, dict) or not isinstance(field_manifest, dict):
        raise ValueError("payload and field_manifest must be objects")
    declared = field_manifest.get("fields")
    asserted = field_manifest.get("asserted_fields")
    if (
        not isinstance(declared, dict)
        or not isinstance(asserted, list)
        or set(asserted) != set(payload)
        or set(asserted) != set(declared)
    ):
        raise ValueError("manifest must declare each asserted payload field")
    for field, info in declared.items():
        if (
            not isinstance(info, dict)
            or info.get("kind") != "payload"
            or type(info.get("derived")) is not bool
            or info.get("code_version") != code_version
            or not isinstance(info.get("inputs"), list)
            or not info["inputs"]
            or any(name not in inputs for name in info["inputs"])
        ):
            raise ValueError(f"invalid payload manifest declaration: {field}")
        if isinstance(payload[field], int) and not isinstance(
            payload[field], bool
        ) and not info.get("unit"):
            raise ValueError(f"numeric payload {field} requires a unit")
    as_of_utc = utc_instant(as_of)
    hashed = {
        "schema_version": schema_version,
        "producer": producer,
        "as_of": as_of_utc,
        "grade": grade,
        "input_snapshot": snapshot,
        "field_manifest": field_manifest,
        "payload": payload,
    }
    assert set(hashed) == set(_HASH_KEYS)
    _check_numbers(hashed, "artifact")
    encoded = json.dumps(
        hashed, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")
    artifact_hash = hashlib.sha256(encoded).hexdigest()
    identity = hashlib.sha256(
        f"{producer}\n{as_of_utc}\n{artifact_hash}".encode("utf-8")
    ).hexdigest()
    return {
        **hashed,
        "artifact_hash": artifact_hash,
        "artifact_id": "sha256:" + identity,
        "generated_at": utc_instant(generated_at),
    }


def verify_strategy_artifact(artifact: dict) -> bool:
    if not isinstance(artifact, dict):
        raise ValueError("artifact must be an object")
    hashed = {key: artifact[key] for key in _HASH_KEYS}
    _check_numbers(hashed, "artifact")
    bytes_value = json.dumps(
        hashed, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")
    expected = hashlib.sha256(bytes_value).hexdigest()
    producer, as_of = artifact["producer"], utc_instant(artifact["as_of"])
    artifact_id = hashlib.sha256(
        f"{producer}\n{as_of}\n{expected}".encode("utf-8")
    ).hexdigest()
    if expected != artifact.get("artifact_hash") or (
        "sha256:" + artifact_id != artifact.get("artifact_id")
    ):
        raise ValueError("strategy artifact content or identity was modified")
    return True
