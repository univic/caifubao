# -*- coding: utf-8 -*-
"""Provider adapters: one shape out, and no second call for an AI result.

The contract is ``caifubao_contracts.strategy_provider``. This module supplies the
two adapters that can exist without new infrastructure:

* ``RuleScoreProvider`` maps stored score documents onto the uniform record shape.
  It is the existing production signal, expressed so that nothing downstream has
  to know it is rule-based.
* ``RecordedProvider`` replays a large model's recorded answers. It deliberately
  has no way to call a model: issuing the first call is a separate, authorised
  step, and a result set without a recording is not evidence.

Neither adapter sizes a position or approves a trade; they only describe stocks.
"""

from __future__ import annotations

from caifubao_contracts.strategy_provider import (
    KIND_LLM_MODEL,
    KIND_RULE_SCORE,
    prompt_digest,
    validate_identity,
    validate_recording,
    validate_result_set,
)


class ProviderError(ValueError):
    """A provider contract rule was violated."""


def _record(document: dict, *, percentile_fallback) -> dict:
    code = document.get("stock_code")
    score = document.get("score")
    if code is None or score is None:
        raise ProviderError("a score document needs stock_code and score")
    percentile = document.get("percentile")
    if percentile is None:
        percentile = percentile_fallback(document)
    return {
        "date": str(document.get("date"))[:10],
        "stock_code": code,
        "score": float(score),
        "percentile": float(percentile),
    }


class RuleScoreProvider:
    """Adapt production score documents to the uniform provider contract."""

    kind = KIND_RULE_SCORE

    def __init__(self, *, source_id: str, artifact_digest: str):
        self.identity = validate_identity(
            {
                "kind": self.kind,
                "source_id": source_id,
                "artifact_digest": artifact_digest,
            }
        )
        self.calls = 0

    def result_set(self, documents) -> dict:
        """Return the validated result set; raises when nothing usable exists."""
        documents = list(documents or [])
        if not documents:
            raise ProviderError("no score documents were supplied")
        ordered = sorted(
            documents, key=lambda row: (-float(row["score"]), str(row["stock_code"]))
        )

        def fallback(document) -> float:
            index = next(
                position
                for position, row in enumerate(ordered)
                if row["stock_code"] == document["stock_code"]
            )
            if len(ordered) <= 1:
                return 1.0
            return 1.0 - index / (len(ordered) - 1)

        self.calls += 1
        return validate_result_set(
            self.identity,
            [_record(row, percentile_fallback=fallback) for row in documents],
        )


class RecordedProvider:
    """Replay a large model's recorded answers; never calls a model itself."""

    kind = KIND_LLM_MODEL

    def __init__(self, *, source_id: str, model_id: str, prompt: str, recordings):
        self.identity = validate_identity(
            {
                "kind": self.kind,
                "source_id": source_id,
                "prompt_digest": prompt_digest(prompt),
                "model_id": model_id,
            }
        )
        self.recordings = {}
        for recording in recordings or []:
            validated = validate_recording(recording)
            self.recordings[validated["decision_date"]] = validated
        self.external_calls = 0

    def has_recording(self, decision_date: str) -> bool:
        return decision_date[:10] in self.recordings

    def replay(self, decision_date: str, *, parse) -> dict:
        """Build the result set from a recording. Never performs a model call."""
        key = decision_date[:10]
        recording = self.recordings.get(key)
        if recording is None:
            raise ProviderError(
                f"no recording for {key}; an unrecorded large-model result is not "
                "evidence and must not be re-generated silently"
            )
        # The recording must belong to this prompt: a different prompt is a new
        # provider identity, not a cache miss.
        if prompt_digest(recording["prompt"]) != self.identity["prompt_digest"]:
            raise ProviderError(
                "recording was produced by a different prompt; that is a new "
                "provider identity, not a replay"
            )
        return validate_result_set(self.identity, parse(recording["response"]))


def assert_replay_is_free(provider: RecordedProvider) -> None:
    """Guard used by tests: replay must never have issued a model call."""
    if provider.external_calls != 0:
        raise ProviderError("replay issued an external call")
