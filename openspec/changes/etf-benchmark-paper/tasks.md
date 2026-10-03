## 1. Contract

- [x] 1.1 Freeze raw-price, session, fee, action-free and REPLAY-only semantics.
- [x] 1.2 Complete spec-guardian review before implementation.

## 2. Implementation

- [x] 2.1 Add deterministic Decimal benchmark and explicit input validation.
- [x] 2.2 Add local unified CLI, atomic JSON output and halt integration.
- [x] 2.3 Add synthetic 100,000 CNY fixture and reproducible runbook.

## 3. Validation and gates

- [x] 3.1 Hand-calculated opening/holding, fee budget, causal timing, blocked
  fills, stale marks, malformed input, halt and CLI regression tests pass.
- [x] 3.2 Relevant strategy tests, Ruff and OpenSpec strict pass.
- [x] 3.3 Post-implementation spec, contract and QA reviewers complete.
- [x] 3.4 Branch conflict check, draft PR, CI green, then ready for review.

PR #285: spec, contract and QA gates passed; 180 focused/regression tests and
all required CI checks passed before marking ready for review. No merge or
deployment is part of this change.

## 4. Frozen source continuation

- [x] 4.1 Define source/calendar/opening-state contract before coding.
- [x] 4.2 Adapt raw exports without daily-volume look-ahead; retain provenance.
- [x] 4.3 Validate source boundaries and native equivalence; review all gates.
- [ ] 4.4 Update draft PR, check conflicts, pass CI and mark ready.
