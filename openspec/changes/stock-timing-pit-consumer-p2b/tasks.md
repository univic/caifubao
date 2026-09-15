# Stock Timing PIT Artifact Consumer P2b Tasks

## 1. Spec Gate

- [x] 1.1 Review immutable binding, artifact-only calculation, `FRESH`
      semantics, collision handling, and P1 compatibility before code changes.
- [x] 1.2 Resolve every P1/P2 spec finding.

## 2. Failing tests

- [x] 2.1 Reject tampered, partial, cross-session, cross-universe, config/model,
      horizon, temporal, and build-provenance mismatches with zero DB access.
- [x] 2.2 Prove artifact-backed component/ranked outputs match production
      semantics, retain missing-quote members as `BLOCKED`, and are deterministic.
- [x] 2.3 Prove every usable prediction carries the exact P1 freshness/cohort/
      input/data-as-of binding, its immutable signal leaf is verified to the
      horizon root, and P1 accepts it only with the emitted handoff record.
- [x] 2.4 Prove dry-run has zero Mongo access; apply rejects registry drift,
      existing natural keys, and output overwrite before any prediction write.

## 3. Implementation

- [x] 3.1 Add P2a artifact-pair validation and an in-memory scoring prefetch
      adapter that performs no market/master database reads.
- [x] 3.2 Add deterministic ranked prediction/result construction with P1
      provenance and insert-only publication after complete preflight.
- [x] 3.3 Add the operator CLI and public runbook/manifest handoff example.
- [x] 3.4 Strengthen P1 manifest/prediction validation with the exact versioned
      Merkle proof and retain mutable future-verification compatibility.

## 4. Validation and gates

- [x] 4.1 Run focused tests and the affected/full datahub suite.
- [x] 4.2 Run Ruff and `openspec validate --all --strict`.
- [x] 4.3 Run spec-guardian, contract-reviewer, and qa-reviewer; resolve all P1
      findings and disposition P2 findings.
- [ ] 4.4 Check target-branch conflicts, create Draft PR, wait for CI, then mark
      ready when every required check is green.
