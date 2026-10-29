# -*- coding: utf-8 -*-
"""Stage machine and gates for the research pipeline.

The contract lives in ``caifubao_contracts.strategy_research``; this module
enforces the order and counts the trials. Storage is injected so the pipeline is
testable without Mongo and so a later adapter can put it behind an operator
authorisation instead of writing into the research database from here.

Three things this refuses that the previous workflow allowed:

* concluding from the exploration stage;
* re-registering a hypothesis to reset its trial budget (trials are counted per
  hypothesis family, not per registration);
* reading the confirmation window during exploration.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from caifubao_contracts.strategy_structure import check_structure, evaluate_capacity
from caifubao_contracts.strategy_research import (
    OUTCOME_CONCLUSION,
    OUTCOME_EXPLORATORY,
    OUTCOME_INFEASIBLE,
    OUTCOME_PROMOTION_CANDIDATE,
    OUTCOME_REJECTED_STRUCTURE,
    SCHEMA_VERDICT,
    adjusted_threshold,
    registration_hash,
    stage_index,
    validate_registration,
    validate_verdict,
    verdict_hash,
)


class ResearchError(ValueError):
    """A pipeline rule was violated; the caller must not treat it as a warning."""


@dataclass
class InMemoryResearchStore:
    """Test double for the durable store; append-only by construction."""

    registrations: dict[str, dict] = field(default_factory=dict)
    family_trials_by_family: dict[str, int] = field(default_factory=dict)
    registration_trials_by_registration: dict[str, int] = field(default_factory=dict)
    confirmations: dict[str, int] = field(default_factory=dict)
    verdicts: dict[str, dict] = field(default_factory=dict)

    def add_registration(self, registration: dict) -> None:
        self.registrations[registration["registration_id"]] = registration
        self.registration_trials_by_registration.setdefault(
            registration["registration_id"], 0
        )
        self.family_trials_by_family.setdefault(registration["family"], 0)

    def get_registration(self, registration_id: str) -> dict | None:
        return self.registrations.get(registration_id)

    def add_trial(self, registration: dict) -> int:
        family = registration["family"]
        self.family_trials_by_family[family] = (
            self.family_trials_by_family.get(family, 0) + 1
        )
        key = registration["registration_id"]
        self.registration_trials_by_registration[key] = (
            self.registration_trials_by_registration.get(key, 0) + 1
        )
        return self.family_trials_by_family[family]

    def add_confirmation(self, registration: dict) -> int:
        family = registration["family"]
        self.confirmations[family] = self.confirmations.get(family, 0) + 1
        return self.confirmations[family]

    def family_trials(self, family: str) -> int:
        return self.family_trials_by_family.get(family, 0)

    def registration_trials(self, registration_id: str) -> int:
        return self.registration_trials_by_registration.get(registration_id, 0)

    def family_confirmations(self, family: str) -> int:
        return self.confirmations.get(family, 0)

    def add_verdict(self, verdict: dict) -> str:
        digest = verdict_hash(verdict)
        existing = self.verdicts.get(digest)
        if existing is not None and existing != verdict:
            raise ResearchError("verdict identity collision")
        self.verdicts[digest] = verdict
        return digest


@dataclass
class ResearchPipeline:
    """Drive one registration through the stages, enforcing every gate."""

    store: InMemoryResearchStore
    alpha: float = 0.05

    def register(self, registration: dict) -> str:
        validated = validate_registration(registration)
        existing = self.store.get_registration(validated["registration_id"])
        if existing is not None:
            # Compare only the declared fields: a persistent store also keeps
            # bookkeeping (created_at), which validate_registration rejects.
            stored = {key: existing.get(key) for key in validated}
            if registration_hash(stored) != registration_hash(validated):
                raise ResearchError(
                    "registration criteria are frozen; create a new registration id"
                )
            return registration_hash(validated)
        self.store.add_registration(validated)
        return registration_hash(validated)

    def _registration(self, registration_id: str) -> dict:
        registration = self.store.get_registration(registration_id)
        if registration is None:
            raise ResearchError(f"unknown registration {registration_id!r}")
        return registration

    def assert_exploration_range(self, registration_id: str, start, end) -> None:
        """Exploration must not read the confirmation window."""
        registration = self._registration(registration_id)
        confirmation_start = date.fromisoformat(registration["confirmation_start"])
        if date.fromisoformat(str(end)[:10]) >= confirmation_start:
            raise ResearchError(
                "exploration range reaches into the confirmation slice "
                f"(starts {confirmation_start.isoformat()}); the outcome would leak"
            )

    def record_exploration_trial(self, registration_id: str, config_digest: str) -> int:
        registration = self._registration(registration_id)
        if not isinstance(config_digest, str) or not config_digest:
            raise ResearchError("config_digest must be a non-empty string")
        trials = self.store.add_trial(registration)
        if trials > registration["max_trials"]:
            raise ResearchError(
                f"trial budget exhausted for family {registration['family']!r}: "
                f"{trials} > max_trials={registration['max_trials']}"
            )
        return trials

    def confirmation_gate(self, registration_id: str) -> dict:
        """Return the adjusted threshold, or refuse when the budget is spent."""
        registration = self._registration(registration_id)
        family = registration["family"]
        used = self.store.family_confirmations(family)
        if used >= registration["max_confirmations"]:
            raise ResearchError(
                f"confirmation budget exhausted for family {family!r}: {used} "
                f">= max_confirmations={registration['max_confirmations']}"
            )
        family_trials = max(1, self.store.family_trials(family))
        return {
            "family": family,
            "confirmations_used": used,
            "family_trials": family_trials,
            "alpha": self.alpha,
            "adjusted_alpha": adjusted_threshold(self.alpha, family_trials),
        }

    def record_confirmation(self, registration_id: str) -> int:
        gate = self.confirmation_gate(registration_id)
        used = self.store.add_confirmation(self._registration(registration_id))
        if used > gate["confirmations_used"] + 1:
            raise ResearchError("confirmation counter diverged")
        return used

    def structural_gate(
        self,
        registration_id: str,
        *,
        holdings: int,
        min_holdings: int,
        annual_turnover: float,
        max_annual_turnover: float,
        max_drawdown: float,
        drawdown_budget: float,
        has_stop_loss: bool,
    ) -> dict:
        """Run the structural gate; a failure blocks any promotion verdict."""
        self._registration(registration_id)
        return check_structure(
            holdings=holdings,
            min_holdings=min_holdings,
            annual_turnover=annual_turnover,
            max_annual_turnover=max_annual_turnover,
            max_drawdown=max_drawdown,
            drawdown_budget=drawdown_budget,
            has_stop_loss=has_stop_loss,
        )

    def capacity_gate(
        self,
        registration_id: str,
        *,
        base_capital: float,
        scaled_capital: float,
        base_metric: float,
        scaled_metric: float,
        min_retained_ratio: float,
    ) -> dict:
        """Run the capacity gate at a larger capital."""
        self._registration(registration_id)
        return evaluate_capacity(
            base_capital=base_capital,
            scaled_capital=scaled_capital,
            base_metric=base_metric,
            scaled_metric=scaled_metric,
            min_retained_ratio=min_retained_ratio,
        )

    def verdict(
        self,
        registration_id: str,
        *,
        stage: str,
        outcome: str,
        spec_hash: str,
        dataset_id: str,
        code_version: str,
        detail: dict | None = None,
    ) -> dict:
        """Build, validate and store an immutable verdict."""
        registration = self._registration(registration_id)
        if outcome in (OUTCOME_CONCLUSION, OUTCOME_PROMOTION_CANDIDATE):
            if stage_index(stage) < stage_index("confirmation"):
                raise ResearchError(f"stage {stage!r} cannot report {outcome!r}")
            # The stage label alone is not evidence: a caller could claim
            # 'forward' without ever confirming. Require a recorded confirmation.
            if self.store.family_confirmations(registration["family"]) < 1:
                raise ResearchError(
                    f"{outcome!r} needs at least one recorded confirmation for "
                    f"family {registration['family']!r}"
                )
        if outcome in (OUTCOME_EXPLORATORY,) and stage_index(stage) > stage_index(
            "exploration"
        ):
            raise ResearchError("an exploratory verdict may not claim a later stage")
        if outcome == OUTCOME_PROMOTION_CANDIDATE:
            # Advisory gates are not gates. A promotion verdict must carry a
            # passed structural check and a passed capacity check, otherwise the
            # word "promotable" means nothing.
            detail_block = detail or {}
            structure = detail_block.get("structure") or {}
            capacity = detail_block.get("capacity") or {}
            if not structure.get("passed"):
                raise ResearchError(
                    "a promotion candidate needs a passed structural gate"
                )
            if not capacity.get("passed"):
                raise ResearchError(
                    "a promotion candidate needs a passed capacity gate"
                )
        if outcome in (OUTCOME_INFEASIBLE, OUTCOME_REJECTED_STRUCTURE):
            pass  # terminal refusals are allowed from any stage they occur at

        family = registration["family"]
        verdict = {
            "schema": SCHEMA_VERDICT,
            "registration_id": registration_id,
            "family": family,
            "stage": stage,
            "outcome": outcome,
            "spec_hash": spec_hash,
            "dataset_id": dataset_id,
            "code_version": code_version,
            "family_trials": max(1, self.store.family_trials(family)),
            "registration_trials": self.store.registration_trials(registration_id),
            "confirmations": self.store.family_confirmations(family),
            "cutoff": {
                "train_cutoff": registration["train_cutoff"],
                "label_horizon": registration["label_horizon"],
                "embargo_sessions": registration["embargo_sessions"],
            },
            "created_at": datetime.now(timezone.utc).isoformat(),
            "detail": dict(detail or {}),
        }
        validated = validate_verdict(verdict)
        self.store.add_verdict(validated)
        return validated


def new_store() -> InMemoryResearchStore:
    return InMemoryResearchStore()
