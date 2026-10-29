# -*- coding: utf-8 -*-
"""Pre-registration and verdict contracts for the research pipeline.

Why this exists
---------------
The same idea could previously be evaluated from anywhere, with any label
convention, any benchmark and any window, and nothing recorded how many
configurations were tried before the winner was picked. Two consequences showed
up in this repository: a direction mix-up that flipped a result by 30pp, and
reports that had to be withdrawn wholesale.

Three rules make that impossible:

1. **Register first.** Hypothesis, data, procedure, success and failure criteria,
   the confirmation slice and the trial budget are frozen before the first
   evaluation. Criteria cannot move afterwards.
2. **Count trials per hypothesis family.** Re-registering the same hypothesis
   under a new identifier must inherit the family's cumulative trials, otherwise
   the multiplicity adjustment is trivially bypassed.
3. **Isolate the confirmation slice.** The fit/selection cutoff must sit at least
   the label horizon plus one session plus the embargo before the confirmation
   start, so overlapping labels cannot leak the answer.

Pure contract: no datahub, Mongo or broker imports. Storage lives elsewhere.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date, timedelta

SCHEMA_REGISTRATION = "research-registration-v1"
SCHEMA_VERDICT = "research-verdict-v1"

#: Ordered pipeline stages. Later stages may conclude more strongly.
STAGE_ORDER = (
    "registration",
    "feasibility",
    "exploration",
    "confirmation",
    "structural",
    "forward",
    "verdict",
)
#: A research conclusion needs at least a confirmation.
STAGES_MAY_CONCLUDE = frozenset({"confirmation", "structural", "forward", "verdict"})
#: A promotion candidate needs at least the forward window.
STAGES_MAY_PROMOTE = frozenset({"forward", "verdict"})

OUTCOME_INFEASIBLE = "infeasible"
OUTCOME_EXPLORATORY = "exploratory"
OUTCOME_REJECTED_STRUCTURE = "rejected_structure"
OUTCOME_CONCLUSION = "conclusion"
OUTCOME_PROMOTION_CANDIDATE = "promotion_candidate"
OUTCOMES = frozenset(
    {
        OUTCOME_INFEASIBLE,
        OUTCOME_EXPLORATORY,
        OUTCOME_REJECTED_STRUCTURE,
        OUTCOME_CONCLUSION,
        OUTCOME_PROMOTION_CANDIDATE,
    }
)


def canonical_json(value) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def content_hash(value) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def business_days_before(day: date, count: int) -> date:
    """The date ``count`` business days before ``day``, weekends excluded.

    A session-based horizon is not a calendar-day horizon: twenty sessions span
    roughly four weeks, so subtracting twenty-one *calendar* days would leave the
    label overlapping the confirmation slice. Weekends are handled here; exchange
    holidays are not, which makes the result very slightly optimistic, so a caller
    holding the real calendar should pass it and get exact arithmetic.
    """
    if type(count) is not int or count < 0:
        raise ValueError("count must be a non-negative integer")
    cursor = day
    remaining = count
    while remaining:
        cursor -= timedelta(days=1)
        if cursor.weekday() < 5:
            remaining -= 1
    return cursor


def _as_date(value, field: str) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str) and len(value) >= 10:
        try:
            return date.fromisoformat(value[:10])
        except ValueError as exc:
            raise ValueError(f"{field} must be an ISO-8601 date") from exc
    raise ValueError(f"{field} must be an ISO-8601 date")


def _count(value, field: str, *, minimum: int) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{field} must be an integer >= {minimum}")
    return value


def _criteria(value, field: str) -> dict:
    if not isinstance(value, dict) or not value:
        raise ValueError(f"{field} must be a non-empty object")
    for key, threshold in value.items():
        if not isinstance(key, str) or not key:
            raise ValueError(f"{field} keys must be non-empty strings")
        if (
            isinstance(threshold, bool)
            or not isinstance(threshold, (int, float))
            or not math.isfinite(float(threshold))
        ):
            raise ValueError(f"{field}.{key} must be a finite number")
    return dict(value)


def session_cutoff(registration: dict, trading_days) -> dict:
    """Session-exact causal cutoff for a normalized registration, with its proof.

    Counting business days treats a holiday block as if it traded, which makes
    the cutoff optimistic exactly where it matters: around a week-long holiday a
    caller can satisfy the embargo on paper while a session-exact check rejects
    it. This returns the arithmetic *and* the numbers it was derived from, so a
    store can record that the embargo was checked against a real calendar instead
    of against an approximation nobody can audit later.

    Returns ``sessions_before_confirmation_start``, ``required_sessions`` and
    ``latest_allowed_cutoff``; raises when the calendar does not reach far enough
    back to place the cutoff at all.
    """
    if not trading_days:
        raise ValueError("session_cutoff needs the exchange calendar")
    need = registration["label_horizon"] + 1 + registration["embargo_sessions"]
    confirmation_start = _as_date(
        registration["confirmation_start"], "confirmation_start"
    )
    sessions = sorted(_as_date(day, "trading_days") for day in trading_days)
    before = [day for day in sessions if day < confirmation_start]
    if len(before) < need:
        raise ValueError(
            f"trading_days must hold at least {need} sessions before the "
            "confirmation start to place the causal cutoff"
        )
    return {
        "sessions_before_confirmation_start": len(before),
        "required_sessions": need,
        "latest_allowed_cutoff": before[-need].isoformat(),
    }


def validate_registration(registration: dict, *, trading_days=None) -> dict:
    """Validate and normalise a registration; fail closed on anything vague."""
    if not isinstance(registration, dict):
        raise ValueError("registration must be an object")
    allowed = {
        "schema",
        "registration_id",
        "hypothesis",
        "family",
        "label_horizon",
        "embargo_sessions",
        "confirmation_start",
        "train_cutoff",
        "max_trials",
        "max_confirmations",
        "success_criteria",
        "failure_criteria",
    }
    unknown = set(registration) - allowed
    if unknown:
        raise ValueError(f"unknown registration fields: {sorted(unknown)}")
    if registration.get("schema") != SCHEMA_REGISTRATION:
        raise ValueError(f"registration schema must be {SCHEMA_REGISTRATION}")

    result = dict(registration)
    for field in ("registration_id", "hypothesis", "family"):
        value = result.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"registration.{field} must be a non-empty string")

    result["label_horizon"] = _count(
        result.get("label_horizon"), "label_horizon", minimum=1
    )
    result["embargo_sessions"] = _count(
        result.get("embargo_sessions", 0), "embargo_sessions", minimum=0
    )
    result["max_trials"] = _count(result.get("max_trials"), "max_trials", minimum=1)
    result["max_confirmations"] = _count(
        result.get("max_confirmations"), "max_confirmations", minimum=1
    )
    result["success_criteria"] = _criteria(
        result.get("success_criteria"), "success_criteria"
    )
    result["failure_criteria"] = _criteria(
        result.get("failure_criteria"), "failure_criteria"
    )

    confirmation_start = _as_date(
        result.get("confirmation_start"), "confirmation_start"
    )
    train_cutoff = _as_date(result.get("train_cutoff"), "train_cutoff")
    result["confirmation_start"] = confirmation_start.isoformat()
    result["train_cutoff"] = train_cutoff.isoformat()

    # The cutoff must clear the label horizon, one session and the embargo so no
    # label used to fit or select reaches into the confirmation window. Sessions
    # are not calendar days: with the real calendar supplied this is exact.
    #
    # The business-day fallback below is deliberately still allowed here, because
    # this function also canonicalizes a declaration for hashing and for comparing
    # a stored registration with a re-declaration, and neither has a calendar. It
    # is NOT a permission to register: `ResearchPipeline.register` requires the
    # calendar, and the pipeline refuses to certify a verdict for a registration
    # whose embargo was never checked session-exactly.
    need = result["label_horizon"] + 1 + result["embargo_sessions"]
    if trading_days:
        latest_allowed = date.fromisoformat(
            session_cutoff(result, trading_days)["latest_allowed_cutoff"]
        )
    else:
        latest_allowed = business_days_before(confirmation_start, need)
    if train_cutoff > latest_allowed:
        raise ValueError(
            "train_cutoff must be on or before "
            f"{latest_allowed.isoformat()} (confirmation_start - label_horizon - 1 "
            f"- embargo_sessions); overlapping labels would leak the outcome"
        )
    return result


def registration_hash(registration: dict) -> str:
    return content_hash(validate_registration(registration))


def adjusted_threshold(
    alpha: float, trials: int, *, method: str = "bonferroni"
) -> float:
    """Significance threshold for the number of trials actually consumed."""
    if method != "bonferroni":
        raise ValueError("only the bonferroni adjustment is supported")
    if (
        isinstance(alpha, bool)
        or not isinstance(alpha, (int, float))
        or not 0 < float(alpha) < 1
    ):
        raise ValueError("alpha must be a fraction in (0, 1)")
    if type(trials) is not int or trials < 1:
        raise ValueError("trials must be a positive integer")
    return float(alpha) / trials


#: Fields a confirmation record carries besides store bookkeeping. A confirmation
#: is evidence, so it names the candidate it was run against and the threshold it
#: was compared with; a bare counter cannot support a promotion.
CONFIRMATION_FIELDS = frozenset(
    {
        "registration_id",
        "family",
        "spec_hash",
        "dataset_id",
        "p_value",
        "threshold",
        "passed",
        "recorded_at",
    }
)


def validate_confirmation(record: dict) -> dict:
    """Validate one confirmation record and its internal consistency.

    ``passed`` must follow from the record's own numbers: a record cannot claim a
    pass that its p-value and threshold contradict, which is what makes the field
    evidence rather than another assertion.
    """
    if not isinstance(record, dict):
        raise ValueError("confirmation must be an object")
    unknown = set(record) - CONFIRMATION_FIELDS
    if unknown:
        raise ValueError(f"unknown confirmation fields: {sorted(unknown)}")
    result = dict(record)
    for field in ("registration_id", "family", "spec_hash", "dataset_id"):
        value = result.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"confirmation.{field} must be a non-empty string")
    for field in ("p_value", "threshold"):
        value = result.get(field)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or not 0.0 <= float(value) <= 1.0
        ):
            raise ValueError(f"confirmation.{field} must be a finite value in [0, 1]")
        result[field] = float(value)
    threshold = result["threshold"]
    if threshold <= 0.0:
        raise ValueError("confirmation.threshold must be a positive significance level")
    passed = result.get("passed")
    if not isinstance(passed, bool):
        raise ValueError("confirmation.passed must be a boolean")
    if passed != (result["p_value"] <= threshold):
        raise ValueError(
            "confirmation.passed contradicts its own numbers: "
            f"p={result['p_value']} threshold={threshold} passed={passed}"
        )
    if result.get("recorded_at") is not None:
        _as_date(result["recorded_at"], "confirmation.recorded_at")
    return result


def validate_verdict(verdict: dict) -> dict:
    """Validate a verdict; it must name everything needed to reproduce it."""
    if not isinstance(verdict, dict):
        raise ValueError("verdict must be an object")
    allowed = {
        "schema",
        "registration_id",
        "family",
        "stage",
        "outcome",
        "spec_hash",
        "dataset_id",
        "code_version",
        "family_trials",
        "registration_trials",
        "confirmations",
        "cutoff",
        "created_at",
        "detail",
    }
    unknown = set(verdict) - allowed
    if unknown:
        raise ValueError(f"unknown verdict fields: {sorted(unknown)}")
    if verdict.get("schema") != SCHEMA_VERDICT:
        raise ValueError(f"verdict schema must be {SCHEMA_VERDICT}")

    result = dict(verdict)
    for field in (
        "registration_id",
        "family",
        "spec_hash",
        "dataset_id",
        "code_version",
    ):
        value = result.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"verdict.{field} must be a non-empty string")

    stage = result.get("stage")
    if stage not in STAGE_ORDER:
        raise ValueError(f"verdict.stage must be one of {list(STAGE_ORDER)}")
    outcome = result.get("outcome")
    if outcome not in OUTCOMES:
        raise ValueError(f"verdict.outcome must be one of {sorted(OUTCOMES)}")
    if outcome == OUTCOME_CONCLUSION and stage not in STAGES_MAY_CONCLUDE:
        raise ValueError(
            f"stage {stage} cannot conclude; it needs at least a confirmation"
        )
    if outcome == OUTCOME_PROMOTION_CANDIDATE and stage not in STAGES_MAY_PROMOTE:
        raise ValueError(f"stage {stage} cannot promote; it needs the forward window")

    result["family_trials"] = _count(
        result.get("family_trials"), "family_trials", minimum=1
    )
    result["registration_trials"] = _count(
        result.get("registration_trials"), "registration_trials", minimum=0
    )
    result["confirmations"] = _count(
        result.get("confirmations"), "confirmations", minimum=0
    )

    cutoff = result.get("cutoff")
    if not isinstance(cutoff, dict):
        raise ValueError("verdict.cutoff must record the causal boundary")
    for field in ("train_cutoff", "label_horizon", "embargo_sessions"):
        if field not in cutoff:
            raise ValueError(f"verdict.cutoff.{field} is required")
    _as_date(cutoff["train_cutoff"], "verdict.cutoff.train_cutoff")
    _count(cutoff["label_horizon"], "verdict.cutoff.label_horizon", minimum=1)
    _count(cutoff["embargo_sessions"], "verdict.cutoff.embargo_sessions", minimum=0)

    _as_date(result.get("created_at"), "created_at")
    result["created_at"] = str(result["created_at"])
    return result


def verdict_hash(verdict: dict) -> str:
    return content_hash(validate_verdict(verdict))


def stage_index(stage: str) -> int:
    if stage not in STAGE_ORDER:
        raise ValueError(f"unknown stage {stage!r}")
    return STAGE_ORDER.index(stage)
