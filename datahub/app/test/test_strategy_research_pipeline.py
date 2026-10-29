# -*- coding: utf-8 -*-
"""The research pipeline must refuse what the old workflow allowed.

Pinned here: criteria are frozen, trials are counted per hypothesis family so a
re-registration cannot reset the budget, exploration cannot read the confirmation
slice, the causal cutoff must clear the label horizon and embargo, and only a
confirmed stage may conclude while only a forward window may promote.
"""

import sys
from datetime import date, timedelta
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
        "train_cutoff": "2026-08-03",
        "max_trials": 5,
        "max_confirmations": 1,
        "success_criteria": {"min_excess_per_period": 0.0},
        "failure_criteria": {"max_drawdown": 0.3},
    }
    base.update(overrides)
    return base


SHA = "a" * 64
DATASET = "snapshot-2026-10"


def _sessions(*, end="2026-09-01", count=60, skip=()):
    """Business days before ``end``; enough to place the 21-session cutoff."""
    day = date.fromisoformat(end)
    out = []
    while len(out) < count:
        day -= timedelta(days=1)
        if day.weekday() < 5 and day.isoformat() not in skip:
            out.append(day.isoformat())
    return sorted(out)


def _pipeline(**kwargs):
    return ResearchPipeline(store=new_store(), **kwargs)


def _verdict(pipeline, **overrides):
    args = {
        "registration_id": "reg-flip-width-2026q4",
        "stage": "confirmation",
        "outcome": OUTCOME_CONCLUSION,
        "spec_hash": SHA,
        "dataset_id": DATASET,
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
    # 21 *business* days before 2026-09-01 is 2026-08-03: sessions are not
    # calendar days, so the old 21-calendar-day window leaked a week of labels.
    validate_registration(_registration(train_cutoff="2026-08-03"))
    with pytest.raises(ValueError, match="overlapping labels"):
        validate_registration(_registration(train_cutoff="2026-08-04"))
    with pytest.raises(ValueError, match="overlapping labels"):
        validate_registration(
            _registration(train_cutoff="2026-08-03", embargo_sessions=1)
        )


def test_cutoff_uses_the_real_calendar_when_supplied():
    """Holidays make the business-day approximation optimistic; a calendar fixes it."""
    from datetime import date, timedelta

    sessions = []
    cursor = date(2026, 7, 1)
    while cursor < date(2026, 9, 1):
        if cursor.weekday() < 5:
            sessions.append(cursor.isoformat())
        cursor += timedelta(days=1)
    allowed = sessions[-21]  # 20 label sessions + 1 + embargo 0
    validate_registration(
        dict(_registration(), train_cutoff=allowed), trading_days=sessions
    )
    with pytest.raises(ValueError, match="overlapping labels"):
        validate_registration(
            dict(_registration(), train_cutoff=sessions[-20]), trading_days=sessions
        )
    with pytest.raises(ValueError, match="at least"):
        validate_registration(
            dict(_registration(), train_cutoff=allowed), trading_days=sessions[-10:]
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
    pipeline.register(_registration(), trading_days=_sessions())
    pipeline.record_exploration_trial("reg-flip-width-2026q4", "cfg-1")
    with pytest.raises(ResearchError, match="criteria are frozen"):
        pipeline.register(
            _registration(success_criteria={"min_excess_per_period": 0.01}),
            trading_days=_sessions(),
        )
    # Re-registering the identical content is idempotent.
    assert pipeline.register(
        _registration(), trading_days=_sessions()
    ) == registration_hash(validate_registration(_registration()))


# --------------------------------------------------------------------------
# trials and multiplicity
# --------------------------------------------------------------------------


def test_trials_accumulate_per_family_across_registrations():
    pipeline = _pipeline()
    pipeline.register(_registration(), trading_days=_sessions())
    pipeline.record_exploration_trial("reg-flip-width-2026q4", "cfg-1")
    pipeline.record_exploration_trial("reg-flip-width-2026q4", "cfg-2")

    # A new identifier for the same hypothesis must inherit the family count.
    pipeline.register(
        _registration(registration_id="reg-flip-width-2026q4-b"),
        trading_days=_sessions(),
    )
    pipeline.record_exploration_trial("reg-flip-width-2026q4-b", "cfg-3")
    gate = pipeline.confirmation_gate("reg-flip-width-2026q4-b")
    assert gate["family_trials"] == 3
    assert gate["adjusted_alpha"] == pytest.approx(0.05 / 3)


def test_trial_budget_is_enforced():
    pipeline = _pipeline()
    pipeline.register(_registration(max_trials=2), trading_days=_sessions())
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
    pipeline.register(_registration(), trading_days=_sessions())
    pipeline.assert_exploration_range(
        "reg-flip-width-2026q4", "2026-01-01", "2026-08-31"
    )
    with pytest.raises(ResearchError, match="confirmation slice"):
        pipeline.assert_exploration_range(
            "reg-flip-width-2026q4", "2026-01-01", "2026-09-01"
        )


def test_confirmation_budget_is_per_family():
    pipeline = _pipeline()
    pipeline.register(_registration(max_confirmations=2), trading_days=_sessions())
    pipeline.record_confirmation(
        "reg-flip-width-2026q4", spec_hash=SHA, dataset_id=DATASET, p_value=0.01
    )
    pipeline.record_confirmation(
        "reg-flip-width-2026q4", spec_hash=SHA, dataset_id=DATASET, p_value=0.01
    )
    with pytest.raises(ResearchError, match="confirmation budget exhausted"):
        pipeline.record_confirmation(
            "reg-flip-width-2026q4", spec_hash=SHA, dataset_id=DATASET, p_value=0.01
        )


# --------------------------------------------------------------------------
# verdicts
# --------------------------------------------------------------------------


def test_exploration_cannot_conclude():
    pipeline = _pipeline()
    pipeline.register(_registration(), trading_days=_sessions())
    with pytest.raises(ResearchError, match="cannot report"):
        _verdict(pipeline, stage="exploration")


def test_confirmation_can_conclude_but_not_promote():
    pipeline = _pipeline()
    pipeline.register(_registration(), trading_days=_sessions())
    pipeline.record_confirmation(
        "reg-flip-width-2026q4", spec_hash=SHA, dataset_id=DATASET, p_value=0.01
    )
    concluded = _verdict(pipeline, stage="confirmation")
    assert concluded["outcome"] == OUTCOME_CONCLUSION
    # With both operational gates satisfied the contract still refuses: only a
    # forward window may promote.
    with pytest.raises(ValueError, match="cannot promote"):
        _verdict(
            pipeline,
            stage="confirmation",
            outcome=OUTCOME_PROMOTION_CANDIDATE,
            detail={"structure": {"passed": True}, "capacity": {"passed": True}},
        )


def test_forward_stage_can_promote_only_with_passed_gates():
    pipeline = _pipeline()
    pipeline.register(_registration(), trading_days=_sessions())
    pipeline.record_confirmation(
        "reg-flip-width-2026q4", spec_hash=SHA, dataset_id=DATASET, p_value=0.01
    )
    with pytest.raises(ResearchError, match="structural gate"):
        _verdict(pipeline, stage="forward", outcome=OUTCOME_PROMOTION_CANDIDATE)
    promoted = _verdict(
        pipeline,
        stage="forward",
        outcome=OUTCOME_PROMOTION_CANDIDATE,
        detail={"structure": {"passed": True}, "capacity": {"passed": True}},
    )
    assert promoted["outcome"] == OUTCOME_PROMOTION_CANDIDATE


def test_verdict_records_counts_and_cutoff():
    pipeline = _pipeline()
    pipeline.register(_registration(), trading_days=_sessions())
    pipeline.record_exploration_trial("reg-flip-width-2026q4", "cfg-1")
    pipeline.record_confirmation(
        "reg-flip-width-2026q4", spec_hash=SHA, dataset_id=DATASET, p_value=0.01
    )
    verdict = _verdict(pipeline, stage="confirmation")
    assert verdict["family_trials"] == 1
    assert verdict["confirmations"] == 1
    assert verdict["cutoff"] == {
        "train_cutoff": "2026-08-03",
        "label_horizon": 20,
        "embargo_sessions": 0,
    }


def test_verdict_is_content_addressed_and_immutable():
    pipeline = _pipeline()
    pipeline.register(_registration(), trading_days=_sessions())
    pipeline.record_confirmation(
        "reg-flip-width-2026q4", spec_hash=SHA, dataset_id=DATASET, p_value=0.01
    )
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
    pipeline.register(_registration(), trading_days=_sessions())
    pipeline.record_confirmation(
        "reg-flip-width-2026q4", spec_hash=SHA, dataset_id=DATASET, p_value=0.01
    )
    with pytest.raises(ValueError, match="dataset_id"):
        _verdict(pipeline, stage="confirmation", dataset_id="")


def test_verdict_schema_is_pinned():
    pipeline = _pipeline()
    pipeline.register(_registration(), trading_days=_sessions())
    pipeline.record_confirmation(
        "reg-flip-width-2026q4", spec_hash=SHA, dataset_id=DATASET, p_value=0.01
    )
    verdict = _verdict(pipeline, stage="confirmation")
    assert verdict["schema"] == SCHEMA_VERDICT
    assert verdict["registration_id"] == "reg-flip-width-2026q4"


def test_infeasible_verdict_is_terminal_and_allowed_early():
    pipeline = _pipeline()
    pipeline.register(_registration(), trading_days=_sessions())
    verdict = _verdict(pipeline, stage="feasibility", outcome=OUTCOME_INFEASIBLE)
    assert verdict["outcome"] == OUTCOME_INFEASIBLE


def test_exploratory_outcome_may_not_claim_a_later_stage():
    pipeline = _pipeline()
    pipeline.register(_registration(), trading_days=_sessions())
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
                    "train_cutoff": "2026-08-03",
                    "label_horizon": 20,
                    "embargo_sessions": 0,
                },
                "created_at": date(2026, 10, 29).isoformat(),
            }
        )


# --------------------------------------------------------------------------
# the two bypasses a review found: a missing calendar, and a bare counter
# --------------------------------------------------------------------------


def test_registration_requires_the_exchange_calendar():
    """Business days treat a holiday block as sessions; registering must not."""
    pipeline = _pipeline()
    with pytest.raises(ResearchError, match="needs the exchange calendar"):
        pipeline.register(_registration())
    # An empty calendar is the same omission.
    with pytest.raises(ResearchError, match="needs the exchange calendar"):
        pipeline.register(_registration(), trading_days=[])


def test_a_holiday_block_cannot_be_papered_over():
    """The concrete bypass: the cutoff passes on business days, fails on sessions.

    2026-08-03 is 21 business days before 2026-09-01, so the calendar-less check
    accepts it. With a week-long holiday inside that stretch the real calendar has
    only 16 sessions, so the label horizon would have overlapped the confirmation
    slice.
    """
    holiday_week = {
        "2026-08-10",
        "2026-08-11",
        "2026-08-12",
        "2026-08-13",
        "2026-08-14",
    }
    sessions = _sessions(skip=holiday_week)
    # Without the calendar the embargo looks satisfied.
    validate_registration(_registration(train_cutoff="2026-08-03"))
    with pytest.raises(ValueError, match="overlapping labels"):
        validate_registration(
            _registration(train_cutoff="2026-08-03"), trading_days=sessions
        )
    pipeline = _pipeline()
    with pytest.raises(ValueError, match="overlapping labels"):
        pipeline.register(
            _registration(train_cutoff="2026-08-03"), trading_days=sessions
        )
    # The session-exact cutoff is a week earlier, and it is recorded as proof.
    cutoff = _sessions(skip=holiday_week)[-21]
    pipeline.register(_registration(train_cutoff=cutoff), trading_days=sessions)
    proof = pipeline.store.registration_calendar_proof("reg-flip-width-2026q4")
    assert proof["latest_allowed_cutoff"] == cutoff
    assert proof["required_sessions"] == 21
    assert proof["sessions_before_confirmation_start"] == len(sessions)


def test_a_verdict_needs_the_calendar_attestation():
    """A registration planted without the calendar cannot support a verdict."""
    pipeline = _pipeline()
    pipeline.store.add_registration(validate_registration(_registration()))
    pipeline.record_confirmation(
        "reg-flip-width-2026q4",
        spec_hash=SHA,
        dataset_id=DATASET,
        p_value=0.01,
    )
    with pytest.raises(ResearchError, match="no session-exact embargo check"):
        _verdict(pipeline, stage="confirmation")


def test_a_confirmation_counter_is_not_evidence():
    """Incrementing the counter is not a confirmation of this candidate."""
    pipeline = _pipeline()
    pipeline.register(_registration(max_confirmations=3), trading_days=_sessions())
    with pytest.raises(ResearchError, match="needs a confirmation recorded"):
        _verdict(pipeline, stage="confirmation")

    # A confirmation for a different candidate does not lend this one its result.
    pipeline.record_confirmation(
        "reg-flip-width-2026q4",
        spec_hash="b" * 64,
        dataset_id=DATASET,
        p_value=0.01,
    )
    with pytest.raises(ResearchError, match="needs a confirmation recorded"):
        _verdict(pipeline, stage="confirmation")

    # The same candidate, but failing its threshold: enough to conclude, not to
    # promote. A conclusion may rest on a failed confirmation; a promotion may not.
    pipeline.record_confirmation(
        "reg-flip-width-2026q4",
        spec_hash=SHA,
        dataset_id=DATASET,
        p_value=0.9,
    )
    concluded = _verdict(pipeline, stage="confirmation")
    assert concluded["outcome"] == OUTCOME_CONCLUSION
    with pytest.raises(ResearchError, match="failed"):
        _verdict(
            pipeline,
            stage="forward",
            outcome=OUTCOME_PROMOTION_CANDIDATE,
            detail={"structure": {"passed": True}, "capacity": {"passed": True}},
        )


def test_a_promotion_needs_a_passed_confirmation_for_that_candidate():
    pipeline = _pipeline()
    pipeline.register(_registration(), trading_days=_sessions())
    pipeline.record_confirmation(
        "reg-flip-width-2026q4",
        spec_hash=SHA,
        dataset_id=DATASET,
        # At one trial the adjusted threshold is 0.05, so 0.02 passes.
        p_value=0.02,
    )
    promoted = _verdict(
        pipeline,
        stage="forward",
        outcome=OUTCOME_PROMOTION_CANDIDATE,
        detail={"structure": {"passed": True}, "capacity": {"passed": True}},
    )
    assert promoted["outcome"] == OUTCOME_PROMOTION_CANDIDATE
    assert promoted["confirmations"] == 1


def test_a_confirmation_measured_against_a_looser_threshold_is_refused():
    """More trials after the confirmation tighten the bar; the pass goes stale."""
    pipeline = _pipeline()
    pipeline.register(_registration(), trading_days=_sessions())
    pipeline.record_confirmation(
        "reg-flip-width-2026q4",
        spec_hash=SHA,
        dataset_id=DATASET,
        p_value=0.04,  # passes against 0.05 / 1 trial
    )
    for digest in ("cfg-1", "cfg-2", "cfg-3"):
        pipeline.record_exploration_trial("reg-flip-width-2026q4", digest)
    with pytest.raises(ResearchError, match="predates later trials"):
        _verdict(
            pipeline,
            stage="forward",
            outcome=OUTCOME_PROMOTION_CANDIDATE,
            detail={"structure": {"passed": True}, "capacity": {"passed": True}},
        )
