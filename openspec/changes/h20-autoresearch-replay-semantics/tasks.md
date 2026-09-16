# H20 Autoresearch Replay Semantics Tasks

## 1. Contract

- [x] 1.1 Freeze validation-only candidate selection and locked final-test use.
- [x] 1.2 Freeze actual-entry-anchored H20 holding and blocked-order roll-forward.
- [x] 1.3 Keep the change research-only and exclude production promotion.

## 2. Bootstrap implementation

- [x] 2.1 Export an immutable, resource-bounded full-universe H20 snapshot.
- [x] 2.2 Record requested/actual entry and exit labels plus blocked-session counts.
- [x] 2.3 Prove component inputs are dated no later than the scoring date.
- [x] 2.4 Run exactly one baseline before entering the experiment loop.

## 3. Validation and review

- [x] 3.1 Run focused exporter/evaluator tests and ruff checks.
- [x] 3.2 Run `openspec validate --all --strict`.
- [x] 3.3 Run spec-guardian and qa-reviewer; resolve every P1 finding.
- [x] 3.4 Complete branch-conflict, draft-PR, and CI gates.
