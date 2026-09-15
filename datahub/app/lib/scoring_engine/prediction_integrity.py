"""Versioned Merkle commitments for immutable generated prediction fields."""

from __future__ import annotations

import datetime as dt
import hashlib
import math
import re
from copy import deepcopy
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Any, Mapping, Sequence

from app.lib.scoring_engine.pit_input_evidence import canonical_json, normalize_value


COMMITMENT_SCHEMA = "stock-prediction-merkle-v1"
LEAF_SCHEMA = "stock-prediction-merkle-leaf-v1"
_LEAF_TAG = b"caifubao:stock-prediction-merkle-leaf-v1\0"
_NODE_TAG = b"caifubao:stock-prediction-merkle-node-v1\0"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_COMMITMENT_FIELDS = {
    "schema_version",
    "leaf_sha256",
    "leaf_index",
    "leaf_count",
    "proof",
    "root_sha256",
}
_SCALE = Decimal("100000000")


def _normalize_leaf_value(value: Any) -> Any:
    """Canonical 1e-8 projection for generated values in the leaf protocol."""
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite prediction leaf value")
        try:
            scaled = (Decimal(str(value)) * _SCALE).quantize(
                Decimal("1"), rounding=ROUND_HALF_EVEN
            )
        except InvalidOperation as exc:
            raise ValueError("invalid prediction leaf value") from exc
        return {"scaled_1e8": int(scaled)}
    if isinstance(value, Mapping):
        return {str(key): _normalize_leaf_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_leaf_value(item) for item in value]
    return normalize_value(value)


def _getter(prediction):
    if isinstance(prediction, Mapping):
        return prediction.get
    return lambda key, default=None: getattr(prediction, key, default)


def _date_text(value: Any, field: str) -> str:
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, str):
        try:
            return dt.date.fromisoformat(value).isoformat()
        except ValueError as exc:
            raise ValueError(f"{field} must be YYYY-MM-DD") from exc
    raise ValueError(f"{field} must be YYYY-MM-DD")


def prediction_leaf_object(prediction: Any) -> dict:
    """Return the exact normalized immutable-leaf object."""
    get = _getter(prediction)
    snapshot = get("input_snapshot")
    if not isinstance(snapshot, Mapping):
        raise ValueError("prediction input_snapshot must be an object")
    snapshot = deepcopy(dict(snapshot))
    snapshot.pop("prediction_commitment", None)
    explanation = get("explanation")
    if not isinstance(explanation, Mapping):
        raise ValueError("prediction explanation must be an object")
    leaf = {
        "schema_version": LEAF_SCHEMA,
        "prediction": {
            "base_price": get("base_price"),
            "date": _date_text(get("date"), "prediction.date"),
            "explanation": deepcopy(dict(explanation)),
            "horizon": get("horizon"),
            "input_snapshot": snapshot,
            "model_version": get("model_version"),
            "percentile": get("percentile"),
            "rank": get("rank"),
            "recommendation": get("recommendation"),
            "score": get("score"),
            "stock_code": get("stock_code"),
            "stock_name": get("stock_name"),
            "target_date": _date_text(get("target_date"), "prediction.target_date"),
        },
    }
    return _normalize_leaf_value(leaf)


def prediction_leaf_hash(prediction: Any) -> str:
    return hashlib.sha256(
        _LEAF_TAG + canonical_json(prediction_leaf_object(prediction))
    ).hexdigest()


def _parent_hash(left: str, right: str) -> str:
    if not _SHA256_RE.fullmatch(left) or not _SHA256_RE.fullmatch(right):
        raise ValueError("Merkle node requires lowercase SHA-256 children")
    return hashlib.sha256(
        _NODE_TAG + bytes.fromhex(left) + bytes.fromhex(right)
    ).hexdigest()


def attach_prediction_commitments(predictions: Sequence[dict]) -> str:
    """Attach stock-code-order proofs to one complete horizon cohort."""
    rows = sorted(predictions, key=lambda row: row["stock_code"])
    if not rows or len({row["stock_code"] for row in rows}) != len(rows):
        raise ValueError("prediction commitment cohort must be non-empty and unique")
    leaves = [prediction_leaf_hash(row) for row in rows]
    levels = [leaves]
    while len(levels[-1]) > 1:
        current = levels[-1]
        levels.append(
            [
                _parent_hash(
                    current[index],
                    current[index + 1] if index + 1 < len(current) else current[index],
                )
                for index in range(0, len(current), 2)
            ]
        )
    root = levels[-1][0]
    leaf_count = len(rows)
    for leaf_index, row in enumerate(rows):
        proof = []
        index = leaf_index
        for level in levels[:-1]:
            sibling_index = index ^ 1
            if sibling_index >= len(level):
                sibling_index = index
            proof.append(
                {
                    "side": "left" if sibling_index < index else "right",
                    "sha256": level[sibling_index],
                }
            )
            index //= 2
        row["input_snapshot"]["prediction_commitment"] = {
            "schema_version": COMMITMENT_SCHEMA,
            "leaf_sha256": leaves[leaf_index],
            "leaf_index": leaf_index,
            "leaf_count": leaf_count,
            "proof": proof,
            "root_sha256": root,
        }
    return root


def verify_prediction_commitment(
    prediction: Any,
    *,
    expected_root: str,
    expected_leaf_count: int,
    expected_leaf_index: int,
) -> None:
    """Fail closed unless one prediction proves inclusion in the expected root."""
    get = _getter(prediction)
    snapshot = get("input_snapshot")
    if not isinstance(snapshot, Mapping):
        raise ValueError("prediction input_snapshot must be an object")
    commitment = snapshot.get("prediction_commitment")
    if not isinstance(commitment, Mapping) or set(commitment) != _COMMITMENT_FIELDS:
        raise ValueError("prediction commitment fields mismatch")
    if commitment.get("schema_version") != COMMITMENT_SCHEMA:
        raise ValueError("prediction commitment schema mismatch")
    leaf_count = commitment.get("leaf_count")
    leaf_index = commitment.get("leaf_index")
    if (
        isinstance(leaf_count, bool)
        or not isinstance(leaf_count, int)
        or leaf_count != expected_leaf_count
        or isinstance(leaf_index, bool)
        or not isinstance(leaf_index, int)
        or leaf_index != expected_leaf_index
        or not 0 <= leaf_index < leaf_count
    ):
        raise ValueError("prediction commitment index/count mismatch")
    leaf_hash = commitment.get("leaf_sha256")
    root_hash = commitment.get("root_sha256")
    if (
        not isinstance(leaf_hash, str)
        or not _SHA256_RE.fullmatch(leaf_hash)
        or not isinstance(root_hash, str)
        or not _SHA256_RE.fullmatch(root_hash)
        or not isinstance(expected_root, str)
        or not _SHA256_RE.fullmatch(expected_root)
        or root_hash != expected_root
        or leaf_hash != prediction_leaf_hash(prediction)
    ):
        raise ValueError("prediction commitment hash mismatch")
    proof = commitment.get("proof")
    if not isinstance(proof, list):
        raise ValueError("prediction commitment proof must be a list")

    current = leaf_hash
    index = leaf_index
    width = leaf_count
    proof_index = 0
    while width > 1:
        if proof_index >= len(proof):
            raise ValueError("prediction commitment proof length mismatch")
        item = proof[proof_index]
        if not isinstance(item, Mapping) or set(item) != {"side", "sha256"}:
            raise ValueError("prediction commitment proof element mismatch")
        sibling_index = index ^ 1
        odd_duplicate = sibling_index >= width
        if odd_duplicate:
            sibling_index = index
        expected_side = "left" if sibling_index < index else "right"
        sibling = item.get("sha256")
        if (
            item.get("side") != expected_side
            or not isinstance(sibling, str)
            or not _SHA256_RE.fullmatch(sibling)
            or (odd_duplicate and sibling != current)
        ):
            raise ValueError("prediction commitment proof path mismatch")
        current = (
            _parent_hash(sibling, current)
            if expected_side == "left"
            else _parent_hash(current, sibling)
        )
        index //= 2
        width = (width + 1) // 2
        proof_index += 1
    if proof_index != len(proof) or current != expected_root:
        raise ValueError("prediction commitment root mismatch")
