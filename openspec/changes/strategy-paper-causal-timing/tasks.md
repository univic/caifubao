# Tasks

- [x] 1. Spec Gate: causal timing/evidence change required; pre-edit guardian reviewed.
- [x] 2. Add failing causal regression tests.
- [x] 3. Version and isolate retrospective decisions; remove future outcome gating.
- [x] 4. Next-session execution and NAV attachment; preserve suspension flags.
- [x] 5. Opening-only sizing and explicit fill evidence.
- [x] 6. Correct active specs/runbook/roadmap evidence claims.
- [x] 7. Focused/full datahub tests, Ruff, strict OpenSpec validation.
- [x] 8. Final spec-guardian, contract-reviewer, qa-reviewer.
- [x] 9. Branch conflict clean; Draft PR #202 created; implementation CI green
  (final ready-event run 34128089619). PR #202 squash merged to develop
  as 87a0ca2 on 2026-09-07, with explicit user approval.

## Task notes
Outcome: P0 causal retrospective paper baseline; forward capture remains next.
Module Impact / Write Scope: datahub strategy only plus relevant public docs/specs.
Spec Gate: required.
Assumptions: no real orders, model promotion or operator database runs.
Validation Plan: failing regression first, focused tests, full datahub suite, Ruff, OpenSpec.
Reviewer Requests: spec-guardian, contract-reviewer, qa-reviewer after validation.
Branch Conflict Check: clean against origin/develop 35a9254.

## Review outcomes

- spec-guardian: GATE_OK; causal REPLAY-only scope and legacy isolation match specs.
- contract-reviewer: GATE_OK; 27 focused tests, no P1/P2 contract blockers.
- qa-reviewer: GATE_OK; 71 strategy tests plus full Ruff and diff checks.
- QA test gaps closed: duplicate execution dates and target equality after outcome-state changes.
- No Mongo round-trip, operator replay, model promotion or forward evidence claimed.

## Closeout

Code complete and merged; deployment and operator replay were not performed by
this task. All outputs remain REPLAY. Next implementation owner: DSH for the
bounded cash-budget slice in production-capability-roadmap/tasks.md. Immutable
forward capture/count remains unstarted and is not part of that handoff.
