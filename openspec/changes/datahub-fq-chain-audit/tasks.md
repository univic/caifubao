# FQ Chain Audit Tasks

## 1. Contract

- [x] 1.1 Freeze the read-only, fail-closed verdict, coverage floor and
  source-comparison semantics in the delta spec.
- [x] 1.2 Keep the merged `fq-adj-factor-fix` ledger frozen; this is a separate
  change.

## 2. Implementation

- [x] 2.1 Add the pure helpers (source comparison, HFQ arithmetic, coverage,
  verdict) in the data-integrity handler.
- [x] 2.2 Add the read-only CLI with `--sample-code`/`--sample-size`,
  `--skip-source`, `--jump-scan` and PASS/WARN/FAIL exit codes.
- [x] 2.3 Scope the latest-session anchor to the active stock universe.
- [x] 2.4 Make an all-error / no-compared sample, zero or low coverage, and a
  market-wide jump FAIL instead of PASS.

## 3. Evidence

- [x] 3.1 Run the audit against the research environment on 2026-10-23 and
  record coverage, sample differences, arithmetic and jump findings.
- [x] 3.2 Document the residual risks and the re-run method.

## 4. Validation and review

- [x] 4.1 Unit tests cover tolerance boundaries, source gaps, arithmetic,
  error/no-compare verdicts, coverage floors and jump verdicts.
- [x] 4.2 Ruff (CI rule set), focused pytest and OpenSpec strict pass.
- [x] 4.3 Spec, contract and QA reviewer gates pass on the committed tree
  (PR #313; blockers fixed and re-reviewed).
- [x] 4.4 Branch conflict check against the target base (clean vs develop;
  PR #307 merged before delivery).
