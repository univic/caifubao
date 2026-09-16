from __future__ import annotations

import json
import hashlib
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.lib.scoring_engine.prediction_integrity import attach_prediction_commitments

from app.lib.strategy_engine.timing_replay import (
    replay_pair,
    validate_model_pin,
    validate_replay_manifest,
)


SHA_A = "a" * 64
SHA_B = "b" * 64
COHORT_FINGERPRINT = hashlib.sha256(b"sh600001\nsh600002").hexdigest()


def _manifest():
    sessions = ["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08"]
    return {
        "schema_version": "timing-replay-p1",
        "cohort": {
            "as_of": "2026-01-04T07:00:00Z",
            "source": "fixture:pit-members-v1",
            "membership_basis": "point_in_time",
            "delisted_completeness": "VERIFIED",
            "provenance": {
                "artifact_uri": "s3://immutable-fixture/members.json",
                "artifact_sha256": SHA_A,
                "member_count": 2,
                "subsequently_delisted_count": 1,
                "suspended_count": 1,
                "includes_subsequently_delisted": True,
                "includes_suspended": True,
            },
            "members": [
                {
                    "stock_code": "sh600002",
                    "listed_on": "2010-01-01",
                    "delisted_on": "2026-06-01",
                    "suspended_at_as_of": False,
                    "evidence_at": "2026-01-04T06:00:00Z",
                },
                {
                    "stock_code": "sh600001",
                    "listed_on": "2012-01-01",
                    "delisted_on": None,
                    "suspended_at_as_of": True,
                    "evidence_at": "2026-01-04T06:00:00Z",
                },
            ],
        },
        "prediction_cohorts": {
            day: {
                "artifact_uri": f"s3://immutable-fixture/ranks/{day}.json",
                "artifact_sha256": SHA_B,
                "prediction_root_sha256": SHA_A,
                "cohort_fingerprint": COHORT_FINGERPRINT,
                "member_codes": ["sh600001", "sh600002"],
                "member_count": 2,
                "data_as_of": f"{day}T07:00:00Z",
            }
            for day in sessions
        },
        "model": {
            "model_version": "ranked-pit-v1",
            "config_hash": "c" * 64,
            "horizon": 20,
        },
        "strategy": {"entry_percentile": 0.9, "exit_percentile": 0.3},
        "window": {"from": sessions[0], "to": sessions[-1]},
        "trading_calendar": sessions,
        "initial_cash": 50_000.0,
        "board_lot": 100,
        "friction": {
            "commission_rate": 0.00025,
            "minimum_commission": 5.0,
            "stamp_duty_rate": 0.001,
            "slippage_rate": 0.001,
        },
    }


def _quotes(*, suspended_second=False, limit_up_second=False):
    rows = []
    for index, day in enumerate(_manifest()["trading_calendar"]):
        rows.append(
            {
                "date": day,
                "open_hfq": 10.0 + index,
                "close_hfq": 10.5 + index,
                "trade_status": 0 if suspended_second and index == 1 else 1,
                "change_rate": 10.0 if limit_up_second and index == 1 else 1.0,
            }
        )
    return rows


def _prediction(day, percentile, **snapshot_overrides):
    return {
        "stock_code": "sh600001",
        "stock_name": "样本一",
        "date": day,
        "horizon": 20,
        "model_version": "ranked-pit-v1",
        "score": 50.0,
        "rank": 1,
        "percentile": percentile,
        "recommendation": "BUY" if percentile >= 0.9 else "NONE",
        "base_price": 10.0,
        "target_date": "2026-02-02",
        "status": "VERIFIED",
        "explanation": {"summary": "fixture"},
        "input_snapshot": {
            "status": "RANKED",
            "scoring_mode": "ranked",
            "freshness": "FRESH",
            "cohort_fingerprint": COHORT_FINGERPRINT,
            "data_as_of": f"{day}T07:00:00Z",
            **snapshot_overrides,
        },
    }


def _bind(manifest, predictions):
    for prediction in predictions:
        dummy = deepcopy(prediction)
        dummy.update(
            stock_code="sh600002",
            stock_name="样本二",
            rank=2,
            percentile=0.5,
            recommendation="NONE",
        )
        root = attach_prediction_commitments([prediction, dummy])
        manifest["prediction_cohorts"][prediction["date"]]["prediction_root_sha256"] = (
            root
        )
    return predictions


def test_manifest_is_point_in_time_source_bound_and_keeps_later_delisting():
    normalized = validate_replay_manifest(_manifest())

    assert normalized["cohort_codes"] == ["sh600001", "sh600002"]
    assert normalized["cohort_source"] == f"fixture:pit-members-v1#sha256={SHA_A}"
    assert normalized["cohort_as_of"] == "2026-01-04T07:00:00Z"
    assert len(normalized["config"]["prediction_cohorts_sha256"]) == 64
    assert len(normalized["config"]["trading_calendar_sha256"]) == 64


def test_documented_manifest_example_is_schema_valid():
    example_path = (
        Path(__file__).resolve().parents[3]
        / "docs"
        / "examples"
        / "timing-replay-p1-manifest.json"
    )

    normalized = validate_replay_manifest(
        json.loads(example_path.read_text(encoding="utf-8"))
    )

    assert normalized["cohort_codes"] == ["sh600001"]


@pytest.mark.parametrize(
    "mutate,match",
    [
        (
            lambda item: item["cohort"].update({"membership_basis": "current_active"}),
            "membership_basis",
        ),
        (
            lambda item: item["cohort"]["provenance"].update(
                {"artifact_sha256": "not-a-hash"}
            ),
            "artifact_sha256",
        ),
        (
            lambda item: item["cohort"]["provenance"].update(
                {"subsequently_delisted_count": 0}
            ),
            "subsequently_delisted_count",
        ),
        (
            lambda item: item["cohort"]["members"][0].update(
                {"evidence_at": "2026-01-05T07:00:00Z"}
            ),
            "evidence_at",
        ),
        (
            lambda item: item["prediction_cohorts"]["2026-01-05"][
                "member_codes"
            ].__setitem__(1, "sh600003"),
            "cohort_fingerprint",
        ),
    ],
)
def test_manifest_rejects_untrustworthy_membership(mutate, match):
    manifest = _manifest()
    mutate(manifest)

    with pytest.raises(ValueError, match=match):
        validate_replay_manifest(manifest)


def test_model_pin_requires_exact_active_ranked_registry_record():
    manifest = validate_replay_manifest(_manifest())
    valid = {
        "model_version": "ranked-pit-v1",
        "config_hash": "c" * 64,
        "scoring_mode": "ranked",
        "status": "ACTIVE",
    }

    assert validate_model_pin(manifest, valid)["model_version"] == "ranked-pit-v1"

    for field, value in (
        ("status", "RETIRED"),
        ("scoring_mode", "raw"),
        ("config_hash", "d" * 64),
    ):
        invalid = {**valid, field: value}
        with pytest.raises(ValueError, match=field):
            validate_model_pin(manifest, invalid)


def test_manifest_rejects_slippage_that_can_make_a_nonpositive_sell_price():
    manifest = _manifest()
    manifest["friction"]["slippage_rate"] = 1.0

    with pytest.raises(ValueError, match="slippage_rate must be < 1"):
        validate_replay_manifest(manifest)


def test_pair_uses_prior_close_signal_and_shared_open_friction_without_liquidation():
    raw_manifest = _manifest()
    predictions = _bind(
        raw_manifest,
        [
            _prediction("2026-01-05", 0.95),
            _prediction("2026-01-06", 0.95),
            _prediction("2026-01-07", 0.5),
        ],
    )
    manifest = validate_replay_manifest(raw_manifest)

    pair = replay_pair("sh600001", _quotes(), predictions, manifest)

    timing_buy = pair["timing"]["trades"][0]
    buy_hold_buy = pair["buy_hold"]["trades"][0]
    assert timing_buy["date"] == "2026-01-06"
    assert timing_buy["exec_price"] == pytest.approx(11.011)
    assert buy_hold_buy["date"] == "2026-01-05"
    assert buy_hold_buy["exec_price"] == pytest.approx(10.01)
    assert timing_buy["status"] == buy_hold_buy["status"] == "FILLED"
    assert timing_buy["quantity"] % 100 == 0
    assert pair["timing"]["trades"][-1]["side"] == "BUY"
    assert pair["timing"]["daily_values"][-1]["equity"] > 0
    assert pair["timing"]["assumptions"] == pair["buy_hold"]["assumptions"]
    assert pair["timing"]["assumptions"]["initial_cash"] == 50_000.0


def test_selected_replay_cohort_accepts_larger_daily_ranked_cohort_proof():
    raw_manifest = _manifest()
    selected = next(
        member
        for member in raw_manifest["cohort"]["members"]
        if member["stock_code"] == "sh600001"
    )
    raw_manifest["cohort"]["members"] = [selected]
    raw_manifest["cohort"]["provenance"].update(
        member_count=1,
        subsequently_delisted_count=0,
        suspended_count=1,
    )
    prediction = _prediction("2026-01-05", 0.95)
    _bind(raw_manifest, [prediction])
    manifest = validate_replay_manifest(raw_manifest)

    pair = replay_pair("sh600001", _quotes(), [prediction], manifest)

    assert manifest["cohort_codes"] == ["sh600001"]
    assert manifest["prediction_cohorts"]["2026-01-05"]["member_count"] == 2
    assert pair["timing"]["trades"][0]["date"] == "2026-01-06"


def test_exit_signal_produces_a_filled_sell_on_the_following_open():
    raw_manifest = _manifest()
    predictions = _bind(
        raw_manifest,
        [
            _prediction("2026-01-05", 0.95),
            _prediction("2026-01-06", 0.2),
        ],
    )
    manifest = validate_replay_manifest(raw_manifest)
    pair = replay_pair(
        "sh600001",
        _quotes(),
        predictions,
        manifest,
    )

    sell = pair["timing"]["trades"][1]
    assert sell["date"] == "2026-01-07"
    assert sell["side"] == "SELL"
    assert sell["status"] == "FILLED"
    assert sell["exec_price"] == pytest.approx(11.988)
    assert sell["stamp_duty"] > 0
    assert pair["timing"]["diagnostics"]["open_position"] is False


@pytest.mark.parametrize("block", ["suspended", "limit_up"])
def test_blocked_next_open_delays_fill(block):
    raw_manifest = _manifest()
    predictions = _bind(raw_manifest, [_prediction("2026-01-05", 0.95)])
    manifest = validate_replay_manifest(raw_manifest)
    quotes = _quotes(
        suspended_second=block == "suspended",
        limit_up_second=block == "limit_up",
    )

    pair = replay_pair("sh600001", quotes, predictions, manifest)

    assert pair["timing"]["trades"][0]["date"] == "2026-01-07"
    assert pair["timing"]["diagnostics"]["blocked_fills"] == 1


@pytest.mark.parametrize(
    "override,reason",
    [
        ({"data_as_of": "2026-01-06T07:00:00Z"}, "future_data_as_of"),
        ({"cohort_fingerprint": "current-active"}, "cohort_fingerprint_mismatch"),
    ],
)
def test_noncausal_or_wrong_cohort_prediction_creates_no_signal(override, reason):
    raw_manifest = _manifest()
    predictions = _bind(raw_manifest, [_prediction("2026-01-05", 0.95, **override)])
    manifest = validate_replay_manifest(raw_manifest)
    pair = replay_pair(
        "sh600001",
        _quotes(),
        predictions,
        manifest,
    )

    assert pair["timing"]["trades"] == []
    assert reason in pair["timing"]["diagnostics"]["rejected_predictions"]


@pytest.mark.parametrize("freshness", [None, "STALE", "CURRENT", " fresh "])
def test_only_exact_fresh_prediction_can_create_a_signal(freshness):
    raw_manifest = _manifest()
    prediction = _prediction("2026-01-05", 0.95)
    _bind(raw_manifest, [prediction])
    manifest = validate_replay_manifest(raw_manifest)
    prediction["input_snapshot"]["freshness"] = freshness

    pair = replay_pair("sh600001", _quotes(), [prediction], manifest)

    assert pair["timing"]["trades"] == []
    assert "stale_prediction" in pair["timing"]["diagnostics"]["rejected_predictions"]


@pytest.mark.parametrize(
    "mutate",
    [
        lambda prediction: prediction.update(score=99.0),
        lambda prediction: prediction.update(rank=2),
        lambda prediction: prediction["input_snapshot"].update(
            {"input_artifact_hash": "0" * 64}
        ),
        lambda prediction: prediction["input_snapshot"]["prediction_commitment"].update(
            {"proof": []}
        ),
    ],
)
def test_prediction_integrity_rejects_committed_field_or_proof_tampering(mutate):
    raw_manifest = _manifest()
    prediction = _prediction("2026-01-05", 0.95)
    _bind(raw_manifest, [prediction])
    manifest = validate_replay_manifest(raw_manifest)

    mutate(prediction)

    pair = replay_pair("sh600001", _quotes(), [prediction], manifest)
    assert pair["timing"]["trades"] == []
    assert (
        "prediction_integrity_mismatch"
        in pair["timing"]["diagnostics"]["rejected_predictions"]
    )


def test_future_verification_fields_are_mutable_outside_prediction_commitment():
    raw_manifest = _manifest()
    prediction = _prediction("2026-01-05", 0.95)
    _bind(raw_manifest, [prediction])
    manifest = validate_replay_manifest(raw_manifest)

    prediction["status"] = "PENDING"
    prediction["verification"] = {"status": "AWAITING_TARGET_DATE"}

    pair = replay_pair("sh600001", _quotes(), [prediction], manifest)
    assert pair["timing"]["trades"][0]["date"] == "2026-01-06"


def test_attribute_style_prediction_document_uses_input_snapshot_provenance():
    raw_manifest = _manifest()
    prediction = _prediction("2026-01-05", 0.95)
    _bind(raw_manifest, [prediction])
    manifest = validate_replay_manifest(raw_manifest)
    prediction = SimpleNamespace(**prediction)

    pair = replay_pair("sh600001", _quotes(), [prediction], manifest)

    assert pair["timing"]["trades"][0]["date"] == "2026-01-06"


def test_missing_authoritative_session_quote_fails_instead_of_shrinking_window():
    manifest = validate_replay_manifest(_manifest())
    quotes = _quotes()
    quotes.pop(2)

    with pytest.raises(ValueError, match="missing quote.*2026-01-07"):
        replay_pair("sh600001", quotes, [], manifest)


def test_pair_is_deterministic_for_reordered_evidence():
    raw_manifest = _manifest()
    predictions = _bind(
        raw_manifest,
        [
            _prediction("2026-01-05", 0.95),
            _prediction("2026-01-06", 0.2),
        ],
    )
    manifest = validate_replay_manifest(raw_manifest)

    first = replay_pair("sh600001", _quotes(), predictions, manifest)
    second = replay_pair(
        "sh600001", list(reversed(_quotes())), list(reversed(predictions)), manifest
    )

    assert first == second
