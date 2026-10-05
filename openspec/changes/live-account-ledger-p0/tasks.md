# Live Account Ledger P0 Tasks

## 1. Spec and models

- [x] 1.1 Define manual-only execution boundary, idempotency, canonical CSV, and
      reconciliation semantics.
- [x] 1.2 Add OrderIntent, ExecutionFill, and AccountReconciliation MongoEngine models
      with useful indexes and Portfolio scoping.

## 2. Backend APIs

- [x] 2.1 Add create/list order-intent endpoints.
- [x] 2.2 Add JSON fill ingestion and fill listing with Portfolio-scoped idempotency.
- [x] 2.3 Add canonical CSV fill import with per-row applied/duplicate/error reporting.
- [x] 2.4 Update linked intent OPEN/PARTIAL/FILLED status after accepted fills.
- [x] 2.5 Add run/list account-reconciliation endpoints.

## 3. Validation

- [x] 3.1 Add focused tests for intent validation and fill-status derivation.
- [x] 3.2 Add focused tests proving duplicate fills do not double-apply the Portfolio.
- [x] 3.3 Add focused tests for CSV mixed-batch behavior.
- [x] 3.4 Add focused tests for PASS/BREAK reconciliation cases.
- [ ] 3.5 Run backend focused pytest and CI-pinned Ruff.
- [ ] 3.6 Run `openspec validate --all --strict`.

## 4. Review and delivery

- [ ] 4.1 spec-guardian review.
- [ ] 4.2 contract-reviewer review.
- [ ] 4.3 qa-reviewer review.
- [ ] 4.4 Confirm branch conflict-free against current develop.
- [ ] 4.5 Create Draft PR, wait for CI, and only mark ready after required gates pass.
