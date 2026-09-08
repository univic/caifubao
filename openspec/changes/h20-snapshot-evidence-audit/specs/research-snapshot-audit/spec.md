## ADDED Requirements

### Requirement: Audit returns have holding-period units
The offline audit SHALL label execution exit/entry returns as holding-period
cohort returns using the configured H20 horizon and rate-cost convention. It
SHALL NOT label them daily returns, compound overlapping cohorts into NAV or
emit strategy promotion/certification decisions. It SHALL report the cost rate and
observed calendar holding durations. Aggregation SHALL average eligible finite-price,
chronologically valid stock returns within each signal date, then average those
cohorts equally. Nominal H20 means 20 per-stock quote observations plus tradability
roll-forward, not necessarily 20 market sessions.

#### Scenario: Overlapping cohorts lose one percent
- WHEN consecutive signal dates each have a nominal H20 holding-period return of minus one percent
- THEN the report identifies a minus one percent mean holding-period cohort return
- AND no daily NAV or compounded total-return field is emitted

### Requirement: Integrity comparisons distinguish missing evidence
The audit SHALL report duplicate signal keys, unusable price values, eligible
rows with invalid signal-entry-exit chronology, execution-label self-join counts,
and missing versus mismatched reference quotes. Missing reference coverage SHALL
NOT be described as a passing comparison. Eligible returns require finite positive
entry/exit prices. Comparisons SHALL normalize date representations.

#### Scenario: Label price disagrees with quoted execution day
- WHEN an execution label is joined to the same stock and actual execution date
- THEN differing finite prices are counted as mismatches
- AND unmatched or nonfinite quote evidence is reported separately

### Requirement: Auditing preserves original artifacts
The command SHALL read existing parquet and optional local reference JSON quotes
without modifying snapshots, candidates, profiles, official run ledgers, or DBs.
An explicit output path SHALL NOT overwrite an input artifact. Audit conclusions
SHALL state their sampled scope and shall not certify untouched dates/symbols.

#### Scenario: Dev sample matches
- WHEN all available sampled dev open/close prices match the snapshot
- THEN the report states sample coverage and match counts
- AND the result does not assert full-universe source correctness
