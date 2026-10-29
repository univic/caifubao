# -*- coding: utf-8 -*-
"""One provider contract across rule, model and large-model sources.

Pinned here: a provider describes stocks and never instructions, the identity is
immutable and content-addressed, a large-model result must be replayed from a
recording rather than re-generated, and provider spend is deducted from the return
before it is reported.
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "shared") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "shared"))

from caifubao_contracts.strategy_provider import (  # noqa: E402
    KIND_LLM_MODEL,
    KIND_ML_MODEL,
    KIND_RULE_SCORE,
    net_return,
    prompt_digest,
    provider_cost,
    record_digest,
    validate_identity,
    validate_record,
    validate_recording,
    validate_result_set,
)
from app.lib.strategy_engine.providers import (  # noqa: E402
    ProviderError,
    RecordedProvider,
    RuleScoreProvider,
    assert_replay_is_free,
)

RULE_IDENTITY = {
    "kind": KIND_RULE_SCORE,
    "source_id": "flip_wide_shadow_v1",
    "artifact_digest": "a" * 64,
}
DOCS = [
    {"date": "2026-08-03", "stock_code": "sh600000", "score": 12.0, "percentile": 0.99},
    {"date": "2026-08-03", "stock_code": "sh600001", "score": 11.0, "percentile": 0.97},
    {"date": "2026-08-03", "stock_code": "sh600002", "score": 10.0, "percentile": 0.90},
]


# --------------------------------------------------------------------------
# record contract
# --------------------------------------------------------------------------


def test_records_share_one_shape_across_kinds():
    record = validate_record(
        {
            "date": "2026-08-03",
            "stock_code": "sh600000",
            "score": 1.0,
            "percentile": 0.5,
        }
    )
    assert record == {
        "date": "2026-08-03",
        "stock_code": "sh600000",
        "score": 1.0,
        "percentile": 0.5,
    }


def test_provider_may_not_emit_an_instruction():
    for field, value in (
        ("side", "BUY"),
        ("quantity", 100),
        ("target_weight", 0.05),
        ("weight", 0.05),
        ("action", "buy"),
    ):
        with pytest.raises(ValueError, match="may not emit instructions"):
            validate_record(
                {
                    "date": "2026-08-03",
                    "stock_code": "sh600000",
                    "score": 1.0,
                    "percentile": 0.5,
                    field: value,
                }
            )


def test_missing_percentile_is_rejected_not_defaulted():
    with pytest.raises(ValueError, match="percentile"):
        validate_record({"date": "2026-08-03", "stock_code": "sh600000", "score": 1.0})


def test_percentile_range_is_enforced():
    with pytest.raises(ValueError, match="in \\[0, 1\\]"):
        validate_record(
            {
                "date": "2026-08-03",
                "stock_code": "sh600000",
                "score": 1.0,
                "percentile": 1.2,
            }
        )


# --------------------------------------------------------------------------
# identity and content addressing
# --------------------------------------------------------------------------


def test_identity_requirements_differ_by_kind():
    assert validate_identity(RULE_IDENTITY)["kind"] == KIND_RULE_SCORE
    with pytest.raises(ValueError, match="artifact_digest"):
        validate_identity({"kind": KIND_RULE_SCORE, "source_id": "x"})
    ml = validate_identity(
        {
            "kind": KIND_ML_MODEL,
            "source_id": "lgbm_v3",
            "artifact_digest": "b" * 64,
            "feature_set_version": "fs_v2",
        }
    )
    assert ml["feature_set_version"] == "fs_v2"
    with pytest.raises(ValueError, match="prompt_digest"):
        validate_identity({"kind": KIND_LLM_MODEL, "source_id": "x", "model_id": "m"})


def test_digest_is_order_independent():
    forward = record_digest(
        [
            {"date": "2026-08-03", "stock_code": "a", "score": 1.0, "percentile": 0.9},
            {"date": "2026-08-03", "stock_code": "b", "score": 2.0, "percentile": 0.8},
        ]
    )
    backward = record_digest(
        [
            {"date": "2026-08-03", "stock_code": "b", "score": 2.0, "percentile": 0.8},
            {"date": "2026-08-03", "stock_code": "a", "score": 1.0, "percentile": 0.9},
        ]
    )
    assert forward == backward


def test_result_set_covers_one_date_and_no_duplicate_code():
    with pytest.raises(ValueError, match="exactly one decision date"):
        validate_result_set(
            RULE_IDENTITY,
            [
                {
                    "date": "2026-08-03",
                    "stock_code": "a",
                    "score": 1,
                    "percentile": 0.9,
                },
                {
                    "date": "2026-08-04",
                    "stock_code": "b",
                    "score": 1,
                    "percentile": 0.9,
                },
            ],
        )
    with pytest.raises(ValueError, match="repeat a stock code"):
        validate_result_set(
            RULE_IDENTITY,
            [
                {
                    "date": "2026-08-03",
                    "stock_code": "a",
                    "score": 1,
                    "percentile": 0.9,
                },
                {
                    "date": "2026-08-03",
                    "stock_code": "a",
                    "score": 2,
                    "percentile": 0.8,
                },
            ],
        )


# --------------------------------------------------------------------------
# adapters
# --------------------------------------------------------------------------


def test_rule_provider_emits_the_uniform_shape():
    provider = RuleScoreProvider(
        source_id="flip_wide_shadow_v1", artifact_digest="a" * 64
    )
    result = provider.result_set(DOCS)
    assert result["decision_date"] == "2026-08-03"
    assert [row["stock_code"] for row in result["records"]] == [
        "sh600000",
        "sh600001",
        "sh600002",
    ]
    assert result["identity"] == RULE_IDENTITY


def test_rule_provider_fills_a_missing_percentile_from_rank():
    provider = RuleScoreProvider(
        source_id="flip_wide_shadow_v1", artifact_digest="a" * 64
    )
    docs = [dict(row) for row in DOCS]
    docs[1] = {key: value for key, value in docs[1].items() if key != "percentile"}
    result = provider.result_set(docs)
    by_code = {row["stock_code"]: row for row in result["records"]}
    assert 0.0 <= by_code["sh600001"]["percentile"] <= 1.0


def test_recorded_provider_replays_without_a_call():
    prompt = "rank the candidates"
    recording = {
        "decision_date": "2026-08-03",
        "prompt": prompt,
        "model_id": "model-x",
        "response": "sh600000,1.0",
        "response_digest": "c" * 64,
    }
    provider = RecordedProvider(
        source_id="llm-screen",
        model_id="model-x",
        prompt=prompt,
        recordings=[recording],
    )
    result = provider.replay(
        "2026-08-03",
        parse=lambda response: [
            {
                "date": "2026-08-03",
                "stock_code": response.split(",")[0],
                "score": 1.0,
                "percentile": 0.95,
            }
        ],
    )
    assert result["records"][0]["stock_code"] == "sh600000"
    assert provider.identity["kind"] == KIND_LLM_MODEL
    assert_replay_is_free(provider)


def test_unrecorded_date_is_refused():
    provider = RecordedProvider(
        source_id="llm-screen", model_id="model-x", prompt="p", recordings=[]
    )
    with pytest.raises(ProviderError, match="no recording"):
        provider.replay("2026-08-03", parse=lambda response: [])


def test_recording_from_another_prompt_is_not_a_replay():
    recording = {
        "decision_date": "2026-08-03",
        "prompt": "a different prompt",
        "model_id": "model-x",
        "response": "x",
        "response_digest": "d" * 64,
    }
    provider = RecordedProvider(
        source_id="llm-screen",
        model_id="model-x",
        prompt="the declared prompt",
        recordings=[recording],
    )
    with pytest.raises(ProviderError, match="different prompt"):
        provider.replay("2026-08-03", parse=lambda response: [])


def test_prompt_digest_is_stable():
    assert prompt_digest("abc") == prompt_digest("abc")
    assert prompt_digest("abc") != prompt_digest("abd")


def test_recording_requires_every_field():
    with pytest.raises(ValueError, match="response_digest"):
        validate_recording(
            {
                "decision_date": "2026-08-03",
                "prompt": "p",
                "model_id": "m",
                "response": "r",
            }
        )


# --------------------------------------------------------------------------
# cost
# --------------------------------------------------------------------------


def test_provider_cost_is_multiplicative_and_guarded():
    assert provider_cost(0.002, 200) == pytest.approx(0.4)
    with pytest.raises(ValueError, match="non-negative"):
        provider_cost(-1.0, 10)
    with pytest.raises(ValueError, match="non-negative integer"):
        provider_cost(0.1, -5)


def test_net_return_deducts_provider_spend():
    gross = 0.12
    # 200 evaluations a session at 0.02 each, 244 sessions, on 100k.
    spend = 200 * 0.02 * 244
    net = net_return(gross, total_cost=spend, capital=100_000)
    assert net == pytest.approx(gross - spend / 100_000)
    assert net < gross
