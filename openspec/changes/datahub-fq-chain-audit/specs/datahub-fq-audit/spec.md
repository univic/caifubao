## ADDED Requirements

### Requirement: The FQ/HFQ chain is auditable on demand

The datahub SHALL provide a read-only audit that reports active-universe
coverage, stored HFQ arithmetic, and a sampled comparison against the tushare
`adj_factor` source of truth, so the chain's reliability can be measured again
after any provider or pipeline change.

#### Scenario: Coverage and arithmetic are reported

- GIVEN the latest session's active stock quotes
- WHEN the audit runs
- THEN it reports how many active supported stocks were quoted and how many of
  those quote rows lack `fq_factor`
- AND it verifies `*_hfq == raw × fq_factor` on the stored rows with a small
  rounding tolerance
- AND BSE stocks, which have `daily_quote=false`, are excluded from the active
  denominator and reported separately from a chain defect

#### Scenario: Sample is compared to the source

- GIVEN a bounded sample of active stocks
- WHEN the audit fetches tushare `adj_factor` for each sample
- THEN it compares every stored factor date against the provider value
- AND reports the maximum absolute difference, mismatches beyond the declared
  tolerance, source gaps, and rows that precede the first provider factor
- AND a provider error is reported per code and never treated as a pass

#### Scenario: Verdict is explicit and fails closed

- GIVEN the coverage, arithmetic and sample results
- WHEN the audit finishes
- THEN it returns PASS, WARN, or FAIL with a non-zero process exit for WARN and
  FAIL
- AND it returns FAIL when any active quote lacks a factor, when an active
  coverage floor is not met, when a sampled divergence exceeds tolerance, or
  when the source was requested but no sample could be compared
- AND a sample error or source gap that did not prevent comparison is at least
  a WARN

#### Scenario: Market-wide jump scan is part of the verdict

- GIVEN the operator requests the jump scan over a bounded window
- WHEN the audit runs
- THEN the existing per-date factor-jump scan is included in the report
- AND a target-date jump that moves at least the declared share of the scanned
  universe makes the audit FAIL
- AND the scan remains read-only
