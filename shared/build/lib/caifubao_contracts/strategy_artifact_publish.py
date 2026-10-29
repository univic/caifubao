# -*- coding: utf-8 -*-
"""Lossless integer-unit encoder for versioned strategy research artifacts.

A producer supplies a frozen payload and a complete numeric-field unit map.
Unknown or unrepresentable numeric measurements fail closed; nothing is silently
rounded, redacted or promoted to FORWARD/trading evidence.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import math

from caifubao_contracts.strategy_artifact import (
    build_strategy_artifact,
    injected_code_version,
)

_UNIT_SCALE = {
    "count": Decimal("1"),
    "shares": Decimal("1"),
    "sessions": Decimal("1"),
    "cny_cent": Decimal("100"),
    "price_1e4_cny": Decimal("10000"),
    "fraction_ppm": Decimal("1000000"),
    "basis_points": Decimal("10000"),
}
_TYPES = {
    "paper": "research/strategy-paper-v2",
    "replay": "research/strategy-replay-v2",
    "backtest": "research/strategy-backtest-v2",
}


def _integer_units(value, path: str, supplied: dict, used: set) -> object:
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return value
    if isinstance(value, (int, float, Decimal)):
        if path not in supplied:
            raise ValueError(f"missing numeric unit for {path}")
        unit = supplied[path]
        scale = _UNIT_SCALE.get(unit)
        if scale is None:
            raise ValueError(f"unsupported declared numeric unit for {path}: {unit}")
        used.add(path)
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"nonfinite numeric payload field {path}")
        try:
            number = Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError(f"invalid numeric payload field {path}") from exc
        if not number.is_finite():
            raise ValueError(f"numeric value of {path} cannot be represented as {unit}")
        # Integer-ratio arithmetic never rounds under the ambient Decimal context.
        numerator, denominator = number.as_integer_ratio()
        encoded, remainder = divmod(numerator * int(scale), denominator)
        if remainder:
            raise ValueError(f"numeric value of {path} cannot be represented as {unit}")
        return encoded
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise ValueError("artifact dictionary keys must be strings")
        return {
            key: _integer_units(item, f"{path}.{key}" if path else key, supplied, used)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [
            _integer_units(item, f"{path}[{index}]", supplied, used)
            for index, item in enumerate(value)
        ]
    raise ValueError(f"unsupported research artifact value at {path}")


def publish_versioned_research_artifact(
    *,
    artifact_type: str,
    legacy_payload: dict,
    numeric_units: dict[str, str],
    inputs: dict,
    as_of,
    generated_at,
    grade: str = "RESEARCH",
) -> dict:
    """Return a new immutable V2 envelope without mutating legacy output.

    `legacy_payload` is an asserted numeric result, not an opaque digest.
    Inputs MUST individually identify frozen source data, pinned Spec identity,
    model version and other evidence used to produce the payload.
    """
    if artifact_type not in _TYPES:
        raise ValueError("artifact_type must be paper, replay or backtest")
    if not isinstance(legacy_payload, dict) or not legacy_payload:
        raise ValueError("research payload must be a nonempty dictionary")
    if not isinstance(numeric_units, dict):
        raise ValueError("numeric_units must be a dictionary")
    if not isinstance(inputs, dict) or not inputs:
        raise ValueError("frozen artifact provenance must be provided")
    code_version = injected_code_version()
    used = set()
    converted = _integer_units(legacy_payload, "", numeric_units, used)
    if set(numeric_units) != used:
        raise ValueError("numeric unit manifest contains unused or incorrect paths")
    manifest = {
        "asserted_fields": sorted(converted),
        "numeric_units": {key: numeric_units[key] for key in sorted(used)},
        "fields": {
            key: {
                "kind": "payload",
                "derived": False,
                "code_version": code_version,
                "inputs": sorted(inputs),
            }
            for key in sorted(converted)
        },
    }
    return build_strategy_artifact(
        schema_version="strategy-research-artifact-v2",
        producer=_TYPES[artifact_type],
        grade=grade,
        as_of=as_of,
        generated_at=generated_at,
        inputs=inputs,
        payload=converted,
        field_manifest=manifest,
    )
