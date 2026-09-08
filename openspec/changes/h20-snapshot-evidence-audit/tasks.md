# Tasks

- [x] 1. Spec Gate and read-only audit protocol review.
- [x] 2. Offline audit command and synthetic integrity/unit tests.
- [x] 3. Actual snapshot self-joins, merged overlap and bounded dev quote parity.
- [x] 4. Audit report, research interpretation correction, agent-progress update.
- [x] 5. Focused/full datahub pytest, Ruff and strict OpenSpec.
- [ ] 6. spec-guardian + qa-reviewer; branch conflict; Draft PR CI then ready.

Outcome: reproducible evidence for the reported 2026 divergence, not performance certification.
Module Impact / Write Scope: one read-only datahub audit command, tests and public audit docs/specs.
Spec Gate: required (public research interpretation and audit contract).
Assumptions: existing snapshots remain immutable; test-window data is inspected for integrity only.
Validation Plan: synthetic corruption/period-unit checks and observed local/source parity.
Reviewer Requests: spec-guardian before code and after validation; qa-reviewer after validation.
Contract-reviewer: not triggered (no API/auth/production freshness changes).
Branch Conflict Check: clean against origin/develop (2026-09-08).

Validation: 592 datahub tests passed; full Ruff 0.15.15 lint/format passed (CI version); strict OpenSpec 16 passed. Local Ruff 0.16.5 has 658 repository-wide baseline findings; new files pass.

Final reviews: spec-guardian GATE_OK; qa-reviewer PASS, no P1/P2 findings. Contract-reviewer not triggered. PR CI pending.
