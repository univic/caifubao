# -*- coding: utf-8 -*-
"""The research pipeline must refuse what the old workflow allowed.

Pinned here: criteria are frozen, trials are counted per hypothesis family so a
re-registration cannot reset the budget, exploration cannot read the confirmation
slice, the causal cutoff must clear the label horizon and embargo, and only a
confirmed stage may conclude while only a forward window may promote.
"""

import sys
from datetime import date
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "shared") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "shared"))

from caifubao_contracts.strategy_research import (  # noqa: E402
    OUTCOME_CONCLUSION,
    OUTCOME_EXPLORATORY,
    OUTCOME_INFEASIBLE,
    OUTCOME_PROMOTION_CANDIDATE,
    SCHEMA_REGISTRATION,
    SCHEMA_VERDICT,
    adjusted_threshold,
    registration_hash,
    validate_registration,
    validate_verdict,
    verdict_hash,
)
from app.lib.strategy_engine.research_pipeline import (  # noqa: E402
    ResearchError,
    ResearchPipeline,
    new_store,
)


def _registration(**overrides):
    base = {
        "schema": SCHEMA_REGISTRATION,
        "registration_id": "reg-flip-width-2026q4",
        "hypothesis": "flip reversal survives at a twenty-name width",
        "family": "flip-width",
        "label_horizon": 20,
        "embargo_sessions": 0,
        "confirmation_start": "2026-09-01",
        "train_cutoff": "2026-08-10",
        "max_trials": 5,
        "max_confirmations": 1,
        "success_criteria": {"min_excess_per_period": 0.0},
        "failure_criteria": {"max_drawdown": 0.3},
    }
    base.update(overrides)
    return base


def _pipeline(**kwargs):
    return ResearchPipeline(store=new_store(), **kwargs)


def _verdict(pipeline, **overrides):
    args = {
        "registration_id": "reg-flip-width-2026q4",
        "stage": "confirmation",
        "outcome": OUTCOME_CONCLUSION,
        "spec_hash": "a" * 64,
        "dataset_id": "snapshot-2026-10",
        "code_version": "b" * 40,
    }
    args.update(overrides)
    return pipeline.verdict(**args)


# --------------------------------------------------------------------------
# registration
# --------------------------------------------------------------------------


def test_registration_normalises_dates_and_defaults_embargo():
    validated = validate_registration(_registration())
    assert validated["confirmation_start"] == "2026-09-01"
    assert validated["embargo_sessions"] == 0


def test_cutoff_must_clear_label_horizon_and_embargo():
    # 2026-09-01 minus (20 + 1 + 0) days = 2026-08-11, which is the latest
    # allowed cutoff; one day later would let a label overlap.
    validate_registration(_registration(train_cutoff="2026-08-11"))
    with pytest.raises(ValueError, match="overlapping labels"):
        validate_registration(_registration(train_cutoff="2026-08-12"))
    with pytest.raises(ValueError, match="overlapping labels"):
        validate_registration(
            _registration(train_cutoff="2026-08-11", embargo_sessions=1)
        )


def test_negative_embargo_is_rejected():
    with pytest.raises(ValueError, match="embargo_sessions"):
        validate_registration(_registration(embargo_sessions=-1))


def test_empty_criteria_are_rejected():
    with pytest.raises(ValueError, match="success_criteria"):
        validate_registration(_registration(success_criteria={}))
    with pytest.raises(ValueError, match="failure_criteria"):
        validate_registration(_registration(failure_criteria={}))


def test_unknown_registration_field_is_rejected():
    with pytest.raises(ValueError, match="unknown registration fields"):
        validate_registration(_registration(lookback=60))


def test_registration_is_frozen_after_the_first_evaluation():
    pipeline = _pipeline()
    pipeline.register(_registration())
    pipeline.record_exploration_trial("reg-flip-width-2026q4", "cfg-1")
    with pytest.raises(ResearchError, match="criteria are frozen"):
        pipeline.register(
            _registration(success_criteria={"min_excess_per_period": 0.01})
        )
    # Re-registering the identical content is idempotent.
    assert pipeline.register(_registration()) == registration_hash(
        validate_registration(_registration())
    )


# --------------------------------------------------------------------------
# trials and multiplicity
# --------------------------------------------------------------------------


def test_trials_accumulate_per_family_across_registrations():
    pipeline = _pipeline()
    pipeline.register(_registration())
    pipeline.record_exploration_trial("reg-flip-width-2026q4", "cfg-1")
    pipeline.record_exploration_trial("reg-flip-width-2026q4", "cfg-2")

    # A new identifier for the same hypothesis must inherit the family count.
    pipeline.register(_registration(registration_id="reg-flip-width-2026q4-b"))
    pipeline.record_exploration_trial("reg-flip-width-2026q4-b", "cfg-3")
    gate = pipeline.confirmation_gate("reg-flip-width-2026q4-b")
    assert gate["family_trials"] == 3
    assert gate["adjusted_alpha"] == pytest.approx(0.05 / 3)


def test_trial_budget_is_enforced():
    pipeline = _pipeline()
    pipeline.register(_registration(max_trials=2))
    pipeline.record_exploration_trial("reg-flip-width-2026q4", "cfg-1")
    pipeline.record_exploration_trial("reg-flip-width-2026q4", "cfg-2")
    with pytest.raises(ResearchError, match="trial budget exhausted"):
        pipeline.record_exploration_trial("reg-flip-width-2026q4", "cfg-3")


def test_bonferroni_adjustment():
    assert adjusted_threshold(0.05, 1) == pytest.approx(0.05)
    assert adjusted_threshold(0.05, 10) == pytest.approx(0.005)
    with pytest.raises(ValueError, match="bonferroni"):
        adjusted_threshold(0.05, 2, method="holm")


# --------------------------------------------------------------------------
# confirmation isolation
# --------------------------------------------------------------------------


def test_exploration_cannot_touch_the_confirmation_slice():
    pipeline = _pipeline()
    pipeline.register(_registration())
    pipeline.assert_exploration_range(
        "reg-flip-width-2026q4", "2026-01-01", "2026-08-31"
    )
    with pytest.raises(ResearchError, match="confirmation slice"):
        pipeline.assert_exploration_range(
            "reg-flip-width-2026q4", "2026-01-01", "2026-09-01"
        )


def test_confirmation_budget_is_per_family():
    pipeline = _pipeline()
    pipeline.register(_registration(max_confirmations=2))
    pipeline.record_confirmation("reg-flip-width-2026q4")
    pipeline.record_confirmation("reg-flip-width-2026q4")
    with pytest.raises(ResearchError, match="confirmation budget exhausted"):
        pipeline.record_confirmation("reg-flip-width-2026q4")


# --------------------------------------------------------------------------
# verdicts
# --------------------------------------------------------------------------


def test_exploration_cannot_conclude():
    pipeline = _pipeline()
    pipeline.register(_registration())
    with pytest.raises(ResearchError, match="cannot report"):
        _verdict(pipeline, stage="exploration")


def test_confirmation_can_conclude_but_not_promote():
    pipeline = _pipeline()
    pipeline.register(_registration())
    concluded = _verdict(pipeline, stage="confirmation")
    assert concluded["outcome"] == OUTCOME_CONCLUSION
    with pytest.raises(ValueError, match="cannot promote"):
        _verdict(
            pipeline,
            stage="confirmation",
            outcome=OUTCOME_PROMOTION_CANDIDATE,
        )


def test_forward_stage_can_promote():
    pipeline = _pipeline()
    pipeline.register(_registration())
    promoted = _verdict(pipeline, stage="forward", outcome=OUTCOME_PROMOTION_CANDIDATE)
    assert promoted["outcome"] == OUTCOME_PROMOTION_CANDIDATE


def test_verdict_records_counts_and_cutoff():
    pipeline = _pipeline()
    pipeline.register(_registration())
    pipeline.record_exploration_trial("reg-flip-width-2026q4", "cfg-1")
    pipeline.record_confirmation("reg-flip-width-2026q4")
    verdict = _verdict(pipeline, stage="confirmation")
    assert verdict["family_trials"] == 1
    assert verdict["confirmations"] == 1
    assert verdict["cutoff"] == {
        "train_cutoff": "2026-08-10",
        "label_horizon": 20,
        "embargo_sessions": 0,
    }


def test_verdict_is_content_addressed_and_immutable():
    pipeline = _pipeline()
    pipeline.register(_registration())
    first = _verdict(pipeline, stage="confirmation")
    digest = verdict_hash(first)
    assert pipeline.store.verdicts[digest] == first
    # Storing the same verdict again is idempotent.
    assert pipeline.store.add_verdict(first) == digest
    # Any edit moves the address, so the stored record cannot be rewritten in
    # place under the old identity.
    tampered = dict(first, code_version="c" * 40)
    assert verdict_hash(tampered) != digest
    assert pipeline.store.verdicts[digest] == first


def test_verdict_requires_provenance():
    pipeline = _pipeline()
    pipeline.register(_registration())
    with pytest.raises(ValueError, match="dataset_id"):
        _verdict(pipeline, stage="confirmation", dataset_id="")


def test_verdict_schema_is_pinned():
    pipeline = _pipeline()
    pipeline.register(_registration())
    verdict = _verdict(pipeline, stage="confirmation")
    assert verdict["schema"] == SCHEMA_VERDICT
    assert verdict["registration_id"] == "reg-flip-width-2026q4"


def test_infeasible_verdict_is_terminal_and_allowed_early():
    pipeline = _pipeline()
    pipeline.register(_registration())
    verdict = _verdict(pipeline, stage="feasibility", outcome=OUTCOME_INFEASIBLE)
    assert verdict["outcome"] == OUTCOME_INFEASIBLE


def test_exploratory_outcome_may_not_claim_a_later_stage():
    pipeline = _pipeline()
    pipeline.register(_registration())
    with pytest.raises(ResearchError, match="may not claim a later stage"):
        _verdict(pipeline, stage="confirmation", outcome=OUTCOME_EXPLORATORY)


def test_unknown_stage_is_rejected_by_the_contract():
    with pytest.raises(ValueError, match="verdict.stage"):
        validate_verdict(
            {
                "schema": SCHEMA_VERDICT,
                "registration_id": "r",
                "family": "f",
                "stage": "discovery",
                "outcome": OUTCOME_CONCLUSION,
                "spec_hash": "a" * 64,
                "dataset_id": "d",
                "code_version": "c",
                "family_trials": 1,
                "registration_trials": 1,
                "confirmations": 1,
                "cutoff": {
                    "train_cutoff": "2026-08-10",
                    "label_horizon": 20,
                    "embargo_sessions": 0,
                },
                "created_at": date(2026, 10, 29).isoformat(),
            }
        )
