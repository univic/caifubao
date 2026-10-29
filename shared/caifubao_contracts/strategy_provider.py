# -*- coding: utf-8 -*-
"""One signal-provider contract for rule, model and large-model sources.

Three rules, each of which a previous version of this program got wrong:

* **Same schema, no branching.** Rule scores, ML model outputs and large-model
  judgements all reduce to ``(date, stock_code, score, percentile, rationale?)``.
  Selection, sizing, risk and execution must never ask which kind produced them,
  otherwise a second implementation appears and drifts.
* **AI emits scores, never instructions.** A provider that returns a side, a
  quantity or a target weight is rejected, because an AI opinion must not be able
  to bypass the risk gate or decide position size.
* **Non-deterministic output is replayable, not deterministic.** A large model
  cannot promise the same answer twice, so it records the request and response
  once and every later evaluation reads the recording. "Same behaviour" then means
  "same recorded answer", and an unrecorded result is not evidence.

Cost is part of the contract too: an evaluation that costs money must have that
cost deducted from net performance, or every AI strategy will look better in
backtest than it can be in practice.

Pure contract: no datahub, Mongo, broker or network imports.
"""

from __future__ import annotations

import hashlib
import json
import math

KIND_RULE_SCORE = "rule_score"
KIND_ML_MODEL = "ml_model"
KIND_LLM_MODEL = "llm_model"
KINDS = frozenset({KIND_RULE_SCORE, KIND_ML_MODEL, KIND_LLM_MODEL})
REPLAYABLE_KINDS = frozenset({KIND_LLM_MODEL})

#: A provider may describe a stock; it may not describe an instruction.
FORBIDDEN_RECORD_FIELDS = frozenset(
    {"side", "quantity", "target_weight", "weight", "order_id", "cash", "action"}
)


def canonical_json(value) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _finite(value, field: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
    ):
        raise ValueError(f"{field} must be a finite number")
    return float(value)


def validate_record(record: dict) -> dict:
    """Validate one provider record; a stock description, never an order."""
    if not isinstance(record, dict):
        raise ValueError("provider record must be an object")
    forbidden = set(record) & FORBIDDEN_RECORD_FIELDS
    if forbidden:
        raise ValueError(
            "a provider may not emit instructions; found "
            f"{sorted(forbidden)} — sizing belongs to the declaration and approval "
            "to the risk gate"
        )
    allowed = {"date", "stock_code", "score", "percentile", "rationale"}
    unknown = set(record) - allowed
    if unknown:
        raise ValueError(f"unknown provider record fields: {sorted(unknown)}")
    for field in ("date", "stock_code"):
        value = record.get(field)
        if not isinstance(value, str) or not value:
            raise ValueError(f"provider record {field} must be a non-empty string")
    result = {
        "date": str(record["date"])[:10],
        "stock_code": record["stock_code"],
        "score": _finite(record.get("score"), "score"),
        "percentile": _finite(record.get("percentile"), "percentile"),
    }
    if not 0.0 <= result["percentile"] <= 1.0:
        raise ValueError("percentile must be in [0, 1]; a missing percentile is not 0")
    rationale = record.get("rationale")
    if rationale is not None:
        if not isinstance(rationale, str):
            raise ValueError("rationale must be a string when present")
        result["rationale"] = rationale
    return result


def validate_identity(identity: dict) -> dict:
    """Validate the immutable identity of one provider result set."""
    if not isinstance(identity, dict):
        raise ValueError("provider identity must be an object")
    allowed = {
        "kind",
        "source_id",
        "artifact_digest",
        "feature_set_version",
        "prompt_digest",
        "model_id",
    }
    unknown = set(identity) - allowed
    if unknown:
        raise ValueError(f"unknown provider identity fields: {sorted(unknown)}")

    kind = identity.get("kind")
    if kind not in KINDS:
        raise ValueError(f"provider kind must be one of {sorted(KINDS)}")
    source = identity.get("source_id")
    if not isinstance(source, str) or not source:
        raise ValueError("provider identity source_id must be a non-empty string")

    result = {"kind": kind, "source_id": source}
    if kind == KIND_RULE_SCORE:
        _require_digest(identity, "artifact_digest", result)
    elif kind == KIND_ML_MODEL:
        _require_digest(identity, "artifact_digest", result)
        feature = identity.get("feature_set_version")
        if not isinstance(feature, str) or not feature:
            raise ValueError("ml_model identity requires feature_set_version")
        result["feature_set_version"] = feature
    else:
        _require_digest(identity, "prompt_digest", result)
        model = identity.get("model_id")
        if not isinstance(model, str) or not model:
            raise ValueError("llm_model identity requires model_id")
        result["model_id"] = model
    return result


def _require_digest(identity: dict, field: str, result: dict) -> None:
    value = identity.get(field)
    if not isinstance(value, str) or len(value) < 8:
        raise ValueError(f"provider identity requires {field}")
    result[field] = value


def record_digest(records: list[dict]) -> str:
    """Content address over the records, independent of input order."""
    normalised = sorted(
        (validate_record(record) for record in records),
        key=lambda record: (record["date"], record["stock_code"]),
    )
    return hashlib.sha256(canonical_json(normalised).encode("utf-8")).hexdigest()


def validate_result_set(identity: dict, records: list[dict]) -> dict:
    """Bind a validated identity to its records and their digest."""
    if not isinstance(records, list) or not records:
        raise ValueError("a result set must contain at least one record")
    validated_identity = validate_identity(identity)
    validated = [validate_record(record) for record in records]
    dates = {record["date"] for record in validated}
    if len(dates) != 1:
        raise ValueError("a result set covers exactly one decision date")
    codes = [record["stock_code"] for record in validated]
    if len(set(codes)) != len(codes):
        raise ValueError("a result set must not repeat a stock code")
    validated.sort(key=lambda record: (-record["score"], record["stock_code"]))
    return {
        "identity": validated_identity,
        "decision_date": dates.pop(),
        "records": validated,
        "digest": record_digest(validated),
    }


def validate_recording(recording: dict) -> dict:
    """Validate one recorded large-model exchange."""
    if not isinstance(recording, dict):
        raise ValueError("recording must be an object")
    allowed = {"decision_date", "prompt", "model_id", "response", "response_digest"}
    unknown = set(recording) - allowed
    if unknown:
        raise ValueError(f"unknown recording fields: {sorted(unknown)}")
    for field in ("decision_date", "prompt", "model_id", "response", "response_digest"):
        value = recording.get(field)
        if not isinstance(value, str) or not value:
            raise ValueError(f"recording.{field} must be a non-empty string")
    return {
        "decision_date": recording["decision_date"][:10],
        "prompt": recording["prompt"],
        "model_id": recording["model_id"],
        "response": recording["response"],
        "response_digest": recording["response_digest"],
    }


def prompt_digest(prompt: str) -> str:
    if not isinstance(prompt, str) or not prompt:
        raise ValueError("prompt must be a non-empty string")
    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()


def provider_cost(cost_per_evaluation: float, evaluations: int) -> float:
    """Money a provider spent producing a session's scores."""
    cost = _finite(cost_per_evaluation, "cost_per_evaluation")
    if cost < 0:
        raise ValueError("cost_per_evaluation must be non-negative")
    if type(evaluations) is not int or evaluations < 0:
        raise ValueError("evaluations must be a non-negative integer")
    return cost * evaluations


def net_return(gross_return: float, *, total_cost: float, capital: float) -> float:
    """Deduct provider spend from a gross return before it is reported."""
    gross = _finite(gross_return, "gross_return")
    if _finite(total_cost, "total_cost") < 0:
        raise ValueError("total_cost must be non-negative")
    if _finite(capital, "capital") <= 0:
        raise ValueError("capital must be positive")
    return gross - float(total_cost) / float(capital)
