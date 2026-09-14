# Small-account strategy research tasks

## 1. Contract and implementation

- [x] 1.1 Define causal holding-scan, turnover, friction, and replay boundaries.
- [x] 1.2 Add holding-period × buffer scan and factor-lab CLI integration.
- [x] 1.3 Add research-only ETF and small-book simulation helpers.
- [x] 1.4 Add replay-ledger helpers and reproducible research reports.

## 2. Correctness fixes

- [x] 2.1 Prove buffer hysteresis changes holdings/turnover when ranks cross the
      entry band but remain inside the exit band.
- [x] 2.2 Charge friction only on replaced target/control weight.
- [x] 2.3 Reject or safely exclude unresolved held-name labels without rolling
      execution dates.

## 3. Validation and gates

- [x] 3.1 Run focused and full datahub tests plus Ruff.
- [x] 3.2 Run `openspec validate --all --strict`.
- [x] 3.3 Complete spec-guardian and qa-reviewer gates; resolve P1/P2 findings.
- [ ] 3.4 Rebase/conflict check, Draft CI green, then mark ready and merge.
