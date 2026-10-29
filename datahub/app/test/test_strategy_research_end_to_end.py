# -*- coding: utf-8 -*-
"""End-to-end: W0-A's recompute becomes the pipeline's first verdict.

Two things are proven here. First, the structural and capacity gates reject the
configurations that should never have been run. Second, the whole chain works on
real numbers: the churn measurement from ``docs/autoresearch/runs/small-book/``
enters as a registered idea, gets its trials counted, and leaves a content-addressed
verdict.

The verdict is deliberately **exploratory**. The measurement covers turnover and
friction only; thirty-three scored sessions cannot settle a return, so nothing
here may be promoted. That is the pipeline behaving correctly, not failing.
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT / "shared") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "shared"))

from caifubao_contracts.strategy_research import (  # noqa: E402
    OUTCOME_EXPLORATORY,
    OUTCOME_PROMOTION_CANDIDATE,
    SCHEMA_REGISTRATION,
    verdict_hash,
)
from caifubao_contracts.strategy_structure import (  # noqa: E402
    FINDING_CAPACITY,
    FINDING_MIN_HOLDINGS,
    FINDING_STOP_LOSS,
    check_structure,
    evaluate_capacity,
)
from app.lib.strategy_engine.research_pipeline import (  # noqa: E402
    ResearchError,
    ResearchPipeline,
    new_store,
)

# W0-A: adjacent-session churn measured on production flip_wide_shadow_v1 scores.
# N=20 with exit_buffer 2.0 -> 15.5% one-way turnover, 11.41% annual friction.
W0A = {
    "width": 20,
    "exit_buffer": 2.0,
    "one_way_turnover": 0.1548,
    "annual_turnover": 37.78,
    "annual_friction_pct": 11.41,
    "sessions_measured": 33,
}


def _registration():
    return {
        "schema": SCHEMA_REGISTRATION,
        "registration_id": "w0a-churn-flip-width",
        "hypothesis": "narrow flip books are the expensive ones at daily re-sort",
        "family": "flip-width",
        "label_horizon": 20,
        "embargo_sessions": 0,
        "confirmation_start": "2026-09-01",
        "train_cutoff": "2026-08-03",
        "max_trials": 12,
        "max_confirmations": 1,
        "success_criteria": {"absolute_turnover_error": 0.05},
        "failure_criteria": {"max_drawdown": 0.3},
    }


# --------------------------------------------------------------------------
# structural gate
# --------------------------------------------------------------------------


def test_structural_gate_passes_a_reasonable_book():
    result = check_structure(
        holdings=20,
        min_holdings=10,
        annual_turnover=W0A["annual_turnover"],
        max_annual_turnover=60,
        max_drawdown=0.2,
        drawdown_budget=0.3,
        has_stop_loss=True,
    )
    assert result["passed"] is True
    assert result["findings"] == []


def test_structural_gate_rejects_the_five_name_book():
    result = check_structure(
        holdings=5,
        min_holdings=10,
        annual_turnover=20,
        max_annual_turnover=60,
        max_drawdown=0.2,
        drawdown_budget=0.3,
        has_stop_loss=True,
    )
    assert result["passed"] is False
    assert [f["code"] for f in result["findings"]] == [FINDING_MIN_HOLDINGS]


def test_structural_gate_rejects_a_book_without_a_stop():
    result = check_structure(
        holdings=20,
        min_holdings=10,
        annual_turnover=20,
        max_annual_turnover=60,
        max_drawdown=0.2,
        drawdown_budget=0.3,
        has_stop_loss=False,
    )
    assert result["passed"] is False
    assert [f["code"] for f in result["findings"]] == [FINDING_STOP_LOSS]


def test_structural_gate_reports_every_finding():
    result = check_structure(
        holdings=5,
        min_holdings=10,
        annual_turnover=200,
        max_annual_turnover=60,
        max_drawdown=0.5,
        drawdown_budget=0.3,
        has_stop_loss=False,
    )
    assert len(result["findings"]) == 4


# --------------------------------------------------------------------------
# capacity gate
# --------------------------------------------------------------------------


def test_capacity_survives_a_doubling():
    result = evaluate_capacity(
        base_capital=100_000,
        scaled_capital=1_000_000,
        base_metric=0.02,
        scaled_metric=0.018,
        min_retained_ratio=0.5,
    )
    assert result["passed"] is True
    assert result["retained_ratio"] == pytest.approx(0.9)


def test_capacity_collapse_is_a_finding():
    result = evaluate_capacity(
        base_capital=100_000,
        scaled_capital=1_000_000,
        base_metric=0.02,
        scaled_metric=0.002,
        min_retained_ratio=0.5,
    )
    assert result["passed"] is False
    assert result["findings"][0]["code"] == FINDING_CAPACITY


def test_capacity_needs_a_meaningful_base():
    result = evaluate_capacity(
        base_capital=100_000,
        scaled_capital=1_000_000,
        base_metric=0.0,
        scaled_metric=0.0,
        min_retained_ratio=0.5,
    )
    assert result["passed"] is False


def test_capacity_requires_a_larger_capital():
    with pytest.raises(ValueError, match="must exceed"):
        evaluate_capacity(
            base_capital=100_000,
            scaled_capital=100_000,
            base_metric=0.02,
            scaled_metric=0.02,
            min_retained_ratio=0.5,
        )


# --------------------------------------------------------------------------
# end to end
# --------------------------------------------------------------------------


def test_w0a_runs_through_the_pipeline_as_an_exploratory_verdict():
    pipeline = ResearchPipeline(store=new_store())
    pipeline.register(_registration())

    # Twelve widths were swept before the conclusion was read; count them.
    for width in (5, 10, 20, 30, 50, 100, 200, 400, 800):
        pipeline.record_exploration_trial("w0a-churn-flip-width", f"width-{width}")
    for buffer in (1.0, 1.5, 2.0):
        pipeline.record_exploration_trial("w0a-churn-flip-width", f"buffer-{buffer}")

    structural = pipeline.structural_gate(
        "w0a-churn-flip-width",
        holdings=W0A["width"],
        min_holdings=10,
        annual_turnover=W0A["annual_turnover"],
        max_annual_turnover=60,
        max_drawdown=0.0,
        drawdown_budget=0.3,
        has_stop_loss=True,
    )
    assert structural["passed"] is True

    verdict = pipeline.verdict(
        "w0a-churn-flip-width",
        stage="exploration",
        outcome=OUTCOME_EXPLORATORY,
        spec_hash="0" * 64,
        dataset_id="research-scores-2026-08-03..2026-10-28",
        code_version="1" * 40,
        detail={
            "measurement": "turnover and friction only",
            "sessions_measured": W0A["sessions_measured"],
            "annual_friction_pct": W0A["annual_friction_pct"],
            "cannot_conclude": "returns are not measured by this slice",
        },
    )
    assert verdict["family_trials"] == 12
    assert verdict["confirmations"] == 0
    assert verdict_hash(verdict) in pipeline.store.verdicts


def test_an_exploratory_verdict_cannot_be_turned_into_a_promotion():
    pipeline = ResearchPipeline(store=new_store())
    pipeline.register(_registration())
    with pytest.raises(ResearchError, match="cannot report"):
        pipeline.verdict(
            "w0a-churn-flip-width",
            stage="exploration",
            outcome=OUTCOME_PROMOTION_CANDIDATE,
            spec_hash="0" * 64,
            dataset_id="d",
            code_version="1" * 40,
        )


def test_promotion_is_refused_when_capacity_collapsed():
    """The capacity gate is enforced, not advisory."""
    pipeline = ResearchPipeline(store=new_store())
    pipeline.register(_registration())
    pipeline.record_confirmation("w0a-churn-flip-width")
    structural = pipeline.structural_gate(
        "w0a-churn-flip-width",
        holdings=20,
        min_holdings=10,
        annual_turnover=W0A["annual_turnover"],
        max_annual_turnover=60,
        max_drawdown=0.0,
        drawdown_budget=0.3,
        has_stop_loss=True,
    )
    capacity = pipeline.capacity_gate(
        "w0a-churn-flip-width",
        base_capital=100_000,
        scaled_capital=1_000_000,
        base_metric=0.02,
        scaled_metric=0.001,
        min_retained_ratio=0.5,
    )
    assert capacity["passed"] is False
    with pytest.raises(ResearchError, match="capacity gate"):
        pipeline.verdict(
            "w0a-churn-flip-width",
            stage="forward",
            outcome=OUTCOME_PROMOTION_CANDIDATE,
            spec_hash="0" * 64,
            dataset_id="d",
            code_version="1" * 40,
            detail={"structure": structural, "capacity": capacity},
        )


def test_promotion_passes_when_both_gates_pass():
    pipeline = ResearchPipeline(store=new_store())
    pipeline.register(_registration())
    pipeline.record_confirmation("w0a-churn-flip-width")
    structural = pipeline.structural_gate(
        "w0a-churn-flip-width",
        holdings=20,
        min_holdings=10,
        annual_turnover=W0A["annual_turnover"],
        max_annual_turnover=60,
        max_drawdown=0.0,
        drawdown_budget=0.3,
        has_stop_loss=True,
    )
    capacity = pipeline.capacity_gate(
        "w0a-churn-flip-width",
        base_capital=100_000,
        scaled_capital=1_000_000,
        base_metric=0.02,
        scaled_metric=0.019,
        min_retained_ratio=0.5,
    )
    verdict = pipeline.verdict(
        "w0a-churn-flip-width",
        stage="forward",
        outcome=OUTCOME_PROMOTION_CANDIDATE,
        spec_hash="0" * 64,
        dataset_id="d",
        code_version="1" * 40,
        detail={"structure": structural, "capacity": capacity},
    )
    assert verdict["outcome"] == OUTCOME_PROMOTION_CANDIDATE
