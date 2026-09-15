# Stock Timing PIT Input Evidence P2a Tasks

## 1. Spec Gate

- [x] 1.1 Review forward-window, full-read-set, shared artifact contract,
      integer encoding, and non-persistence semantics before implementation.
- [x] 1.2 Resolve all P1/P2 findings.

## 2. Failing tests

- [x] 2.1 Canonical artifact hashing/id, UTC instants, fixed-unit numeric encoding,
      source-hash reconciliation, tamper rejection, and deterministic output.
- [x] 2.2 Pre-open universe and post-close input windows reject stale/future/
      calendar-edge capture before source reads.
- [x] 2.3 Complete read-set capture retains no-quote members, sorts source rows,
      rejects future/duplicate rows, and pins ranked model/config/build inputs.
- [x] 2.4 Both CLI commands create new files exclusively, never overwrite, and
      perform no prediction or model mutation.

## 3. Implementation

- [x] 3.1 Add shared-contract canonical encoding/build/validation helpers local
      to the datahub artifact producer.
- [x] 3.2 Add pre-open universe capture and a database reader that captures the
      complete post-close ranked scoring input superset for that frozen cohort.
- [x] 3.3 Add `capture-pit-universe` / `capture-pit-inputs` CLI commands and
      operator documentation.

## 4. Validation and gates

- [x] 4.1 Run focused tests and the affected full datahub suite.
- [x] 4.2 Run Ruff and `openspec validate --all --strict`.
- [x] 4.3 Run spec-guardian, contract-reviewer, and qa-reviewer; resolve P1 and
      acknowledge remaining P2 findings.
- [ ] 4.4 Check branch conflicts, create Draft PR, wait for CI, then mark ready.
